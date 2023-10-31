import asyncio
from fastapi import FastAPI, Response
from fastapi.responses import HTMLResponse, StreamingResponse
from ultralytics import YOLO
import cv2
import time
from kafka import KafkaProducer
import base64
from imutils.video import FPS
import subprocess
import json
import uvicorn
app = FastAPI()
model = YOLO("yolov8s.pt")

video_stream = "http://211.132.61.124/mjpg/video.mjpg"
video_capture = cv2.VideoCapture(video_stream)

kafka_brokers = 'localhost:9092'
kafka_topic = 'yolo-kafka'

# Создание объекта KafkaProducer для отправки данных в Kafka
kafka_producer = KafkaProducer(bootstrap_servers=kafka_brokers)

# Команда FFmpeg для обработки видео
ffmpeg_cmd = [
    'ffmpeg', '-y',
    '-f', 'image2pipe',
    '-vcodec', 'mjpeg',
    '-r', '30',
    '-i', '-',
    '-codec:v', 'libx264',
    '-pix_fmt', 'yuv420p',
    '-preset', 'medium',
    '-b:v', '500k',
    '-bufsize', '10000k',
    '-f', 'matroska',
    'pipe:'
]

# Функция для генерации видеокадров
async def gen_frames():
    # Создание подпроцесса FFmpeg
    ffmpeg_process = await asyncio.create_subprocess_exec(
        *ffmpeg_cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE
    )

    try:
        while True:
            fps = FPS().start()
            # Захват кадра из видеопотока
            success, frame = video_capture.read()
            fps.update()
            fps.stop()
            time.sleep(0.3)
            font = cv2.FONT_HERSHEY_SIMPLEX
            cv2.putText(frame, f'FPS {int(fps.fps())}', (10, 50), font, 1, (255, 0, 0), 2, cv2.LINE_AA)
            if not success:
                break
            # Обработка кадра с использованием модели YOLO
            results = model(frame, imgsz=1280)
            annotated_frame = results[0].plot()

            # Кодирование кадра в формат JPEG и получение байтов
            video_bytes = cv2.imencode('.jpg', annotated_frame)[1].tobytes()
            ffmpeg_process.stdin.write(video_bytes)
            
            # Кодирование байтов в base64
            base64_data = base64.b64encode(video_bytes).decode('utf-8')

            # Создание схемы данных и отправка в Kafka
            schema = {"frame": base64_data}
            kafka_producer.send(kafka_topic, value=json.dumps(schema).encode('utf-8'))

            # Отправка кадра клиенту через стрим
            yield (b'--frame_b\r\n'
                    b'Content-Type: image/jpeg\r\n\r\n' + video_bytes + b'\r\n')

    finally:
        # Закрытие stdin FFmpeg и ожидание завершения процесса
        ffmpeg_process.stdin.close()
        await ffmpeg_process.wait()

# Маршрут для главной страницы
@app.get('/', response_class=HTMLResponse)
def index():
    with open('templates/index.html', 'r') as file:
        return Response(content=file.read(), media_type="text/html")

# Маршрут для потоковой передачи видео
@app.get('/video_feed')
def video_feed():
    return StreamingResponse(gen_frames(), media_type='multipart/x-mixed-replace; boundary=frame')

if __name__ == '__main__':
    uvicorn.run(app, host='127.0.0.1', port=8085)
