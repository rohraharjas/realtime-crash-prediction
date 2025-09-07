import os
import json
import base64
import socket
import time
from typing import List, Dict
import numpy as np
from PIL import Image
import cv2
from confluent_kafka import Consumer, KafkaException
import torch
from transformers import AutoFeatureExtractor, VideoMAEForVideoClassification
from client import load_kafka_config

# Confluent / SASL settings for Confluent Cloud
CONFIG = load_kafka_config()

INPUT_TOPIC = CONFIG["input.topic"]

# Model & inference settings
HF_MODEL_ID = "zhiyaowang/VideoMaev2-giant-nexar-solution"
WINDOW_SIZE = 16
CRASH_THRESHOLD = float(os.getenv("CRASH_THRESHOLD", "0.5"))  # adjust as needed
INFERENCE_DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
BATCH_INFERENCE = False  # for now single-window at a time

# Socket send settings
ALERT_PAYLOAD_TEMPLATE = {
    "alert": "collision_risk",
    "probability": None,
    "timestamp": None,
    "batch_size": WINDOW_SIZE
}
SOCKET_SEND_TIMEOUT = 5  # seconds

# -----------------------
# Load model + feature extractor
# -----------------------
print(f"Loading model {HF_MODEL_ID} on {INFERENCE_DEVICE} ...")
feature_extractor = AutoFeatureExtractor.from_pretrained(HF_MODEL_ID)
model = VideoMAEForVideoClassification.from_pretrained(HF_MODEL_ID)
model.to(INFERENCE_DEVICE)
model.eval()
print("Model loaded.")

# -----------------------
# Helpers: image decoding + preprocessing
# -----------------------
def decode_base64_to_pil(base64_str: str) -> Image.Image:
    """Decode base64 string to PIL Image (RGB)."""
    img_bytes = base64.b64decode(base64_str)
    # try cv2 then convert to PIL
    nparr = np.frombuffer(img_bytes, np.uint8)
    img_cv = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    if img_cv is None:
        # fallback: open via PIL
        from io import BytesIO
        return Image.open(BytesIO(img_bytes)).convert("RGB")
    # convert BGR->RGB
    img_rgb = cv2.cvtColor(img_cv, cv2.COLOR_BGR2RGB)
    pil = Image.fromarray(img_rgb)
    return pil

def preprocess_frames_for_model(frames: List[Image.Image]):
    """
    Accepts list of PIL images length = WINDOW_SIZE.
    Uses HuggingFace feature_extractor to produce tensor of shape expected by model.
    """
    # feature_extractor for VideoMAE expects list of frames (per example).
    # For batch of 1 video: pass frames as list and wrap in a list
    # Some HF feature extractors accept nested lists: [ [frame1, frame2, ...] ]
    inputs = feature_extractor(frames, return_tensors="pt")  # likely returns pixel_values
    # For video, feature_extractor may return dict with 'pixel_values' shaped (1, num_frames, C, H, W)
    # Move to device
    for k, v in inputs.items():
        if isinstance(v, torch.Tensor):
            inputs[k] = v.to(INFERENCE_DEVICE)
    return inputs

def predict_crash_probability(frames: List[Image.Image]) -> float:
    """Run model inference on a list of frames, return probability of crash (float 0..1)."""
    inputs = preprocess_frames_for_model(frames)
    with torch.no_grad():
        outputs = model(**inputs)
        logits = outputs.logits  # shape (batch, num_labels)
        # Example model was trained with 2 classes: [no-crash, crash], but verify
        # apply softmax to get probability of crash class (assume index 1)
        probs = torch.softmax(logits, dim=1)
        prob_crash = probs[:, 1].cpu().item() if probs.shape[1] > 1 else probs[:, 0].cpu().item()
        # NOTE: model page suggests temperature-scaling: softmax(outputs.logits / 2.0)
        # Use same trick if needed:
        # probs_t = torch.softmax(logits / 2.0, dim=1)
        # prob_crash = probs_t[:,1].cpu().item()
    return prob_crash

# -----------------------
# Alert sender: connect to socket and send JSON
# -----------------------
def send_alert_to_socket(socket_key: str, payload: Dict):
    """
    socket_key is expected as "host:port" or "host,port" or JSON containing host/port.
    We will attempt parsing safely.
    """
    # try parse host:port
    if not socket_key:
        print("Empty socket key; skipping alert send.")
        return False

    host = None
    port = None
    try:
        if ":" in socket_key:
            host, port_s = socket_key.split(":", 1)
            port = int(port_s)
        elif "," in socket_key:
            host, port_s = socket_key.split(",", 1)
            port = int(port_s)
        else:
            # maybe just a host (no port); default port?
            host = socket_key.strip()
            port = 80
    except Exception as ex:
        print(f"Failed parsing socket key `{socket_key}`: {ex}")
        return False

    try:
        with socket.create_connection((host, port), timeout=SOCKET_SEND_TIMEOUT) as s:
            s.settimeout(SOCKET_SEND_TIMEOUT)
            data = json.dumps(payload).encode("utf-8")
            s.sendall(data)
            print(f"Alert sent to {host}:{port}: {payload}")
        return True
    except Exception as ex:
        print(f"Failed to send alert to {host}:{port}: {ex}")
        return False

# -----------------------
# Kafka consumer loop
# -----------------------
def run_consumer():
    consumer = Consumer(CONFIG)
    consumer.subscribe([INPUT_TOPIC])
    print(f"Subscribed to Kafka topic: {INPUT_TOPIC}")

    # per-key buffers
    buffers: Dict[str, List[Image.Image]] = {}

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                # handle errors
                raise KafkaException(msg.error())

            # key is expected to contain the socket address (host:port)
            key_bytes = msg.key()
            key_str = key_bytes.decode("utf-8") if key_bytes else ""
            value_bytes = msg.value()
            if not value_bytes:
                continue

            # value is expected to be base64 string of image bytes (or JSON containing frame)
            try:
                # If the message value is JSON with {'frame': '<base64>'}, handle that
                raw_val = value_bytes.decode("utf-8")
                parsed = None
                try:
                    parsed = json.loads(raw_val)
                except Exception:
                    parsed = raw_val  # treat as raw base64 string

                if isinstance(parsed, dict) and ("frame" in parsed or "frame_base64" in parsed):
                    base64_str = parsed.get("frame") or parsed.get("frame_base64")
                else:
                    base64_str = parsed if isinstance(parsed, str) else None

                if not base64_str:
                    print("No base64 frame found in message; skipping.")
                    continue

                pil_img = decode_base64_to_pil(base64_str)

                # Append to buffer for this key
                buffers.setdefault(key_str, []).append(pil_img)

                # If buffer reached WINDOW_SIZE, run inference and clear buffer
                if len(buffers[key_str]) >= WINDOW_SIZE:
                    window_frames = buffers[key_str][:WINDOW_SIZE]
                    # remove first WINDOW_SIZE frames (non-overlapping tumbling)
                    buffers[key_str] = buffers[key_str][WINDOW_SIZE:]

                    # If model expects RGB 224x224, feature_extractor will handle resizing/cropping.
                    # Ensure number of frames matches expected (some models may require exactly WINDOW_SIZE)
                    if len(window_frames) != WINDOW_SIZE:
                        print(f"Window frame count mismatch: {len(window_frames)}; skipping.")
                        continue

                    # Run inference
                    prob = predict_crash_probability(window_frames)
                    ts = time.time()
                    print(f"[{key_str}] Crash probability: {prob:.4f} (threshold={CRASH_THRESHOLD})")

                    if prob >= CRASH_THRESHOLD:
                        payload = dict(ALERT_PAYLOAD_TEMPLATE)
                        payload["probability"] = prob
                        payload["timestamp"] = ts
                        payload["source_key"] = key_str
                        # send alert to socket
                        send_alert_to_socket(key_str, payload)
            except Exception as ex:
                print(f"Error handling message: {ex}")

    except KeyboardInterrupt:
        print("Shutting down consumer.")
    finally:
        consumer.close()

# -----------------------
# Entrypoint
# -----------------------
if __name__ == "__main__":
    print("Starting Kafka Video Crash Detector...")
    print(f"Using device: {INFERENCE_DEVICE}, window: {WINDOW_SIZE}, threshold: {CRASH_THRESHOLD}")
    run_consumer()
