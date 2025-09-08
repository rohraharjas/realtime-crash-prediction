# 🚦 Video Crash Prediction Pipeline  

This project is a **real-time crash prediction system** that consumes video frames from Kafka, processes them with a Hugging Face model, and issues alerts back to the video source when a potential crash is detected.  

The inspiration for this project comes from the [Nexar Dashcam Crash Prediction Challenge](https://www.kaggle.com/competitions/nexar-collision-prediction) on kaggle. The aim was to implement a system that could predict car crashes in real time.

---

## 🔹 Features  
- 📡 **Kafka and Flink Integration**: Ingests video frame from Kafka and creates windows using Flink on Confluent Cloud.  
- 🤖 **Crash Prediction**: Uses [VideoMAE v2](https://huggingface.co/zhiyaowang/VideoMaev2-giant-nexar-solution) for frame-level crash detection.  
- ⚡ **Real-Time Processing**: Works on frame windows (e.g., 16 frames per source device).  
- 🔔 **Alerting**: Connects to the video source via socket (IP provided in Kafka message) to trigger alerts when a crash is detected.  
- ☁️ **Cloud Ready**: Designed to run with Kafka and Flink on Confluent + Hugging Face models on GCP.  

---

## 🔧 Tech Stack  
- **Python 3.9+**  
- **Confluent Kafka Python client** (`confluent-kafka`)  
- **Hugging Face Transformers / Diffusers**  
- **PyTorch**  
- **Sockets** (for alert communication)  

---

## 🔄 Pipeline

![Pipeline](./public/pipeline.png)

---