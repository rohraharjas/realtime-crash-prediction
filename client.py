from confluent_kafka import Producer
from playwright.async_api import async_playwright
from datetime import datetime
import asyncio
import socket
import configparser

def load_kafka_config(file_path="client.properties"):
    """Reads client.properties into a dict usable by confluent_kafka"""
    conf = {}
    parser = configparser.ConfigParser()

    with open(file_path) as f:
        props = "[default]\n" + f.read()

    parser.read_string(props)
    for key, value in parser["default"].items():
        conf[key] = value
    return conf

class ProducerNode:
    def __init__(self, topic):
        kafka_config = load_kafka_config()
        self.producer = Producer(kafka_config)
        self.topic = topic
        hostname = socket.gethostname()
        self.ip_address = socket.gethostbyname(hostname)

class Source(ProducerNode):
    def delivery_report(self, err, msg):
        if err is not None:
            print(f"❌ Delivery failed: {err}")
        else:
            print(f"✅ Frame delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

    async def stream_video(self, video_url):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False)
            context = await browser.new_context()
            page = await context.new_page()

            await page.goto(video_url)
            await page.keyboard.press("f")
            await page.mouse.move(0, 0)
            frame_no=1
            async def push_frame(msg):
                nonlocal frame_no
                data = msg["data"]  # base64 jpeg
                session_id = msg["sessionId"]

                # Acknowledge
                await client.send("Page.screencastFrameAck", {"sessionId": session_id})

                # Push frame to Kafka
                self.producer.produce(
                    self.topic,
                    value=data.encode("utf-8"),
                    key = self.ip_address.encode("utf-8"),
                    headers={
                        "frame_no": str(frame_no)
                    },
                    callback=self.delivery_report
                )
                self.producer.poll(0)
                frame_no += 1
            client = await context.new_cdp_session(page)
            client.on("Page.screencastFrame", push_frame)
            await client.send("Page.startScreencast", {"format": "jpeg", "quality": 80})

            print("🎥 Streaming video frames to Kafka...")

            # Keep the browser alive for video duration (example: 5 mins = 300 sec)
            await page.wait_for_timeout(300_000)  

            # Stop streaming and cleanup
            await client.send("Page.stopScreencast")
            await browser.close()

    def run(self, video_url):
        asyncio.run(self.stream_video(video_url))


if __name__ == "__main__":
    topic = "VideoStream"
    video_url = "https://www.youtube.com/watch?v=E_2lb3j7cN8&list=PLjInUIyuTZrPXXdBC8atIdZBUjdD6cHfZ&index=6"  # Example URL

    source = Source(topic)
    source.run(video_url)