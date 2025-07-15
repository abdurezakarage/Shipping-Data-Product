import os
import pandas as pd
from ultralytics import YOLO
from pathlib import Path
import json

# Paths
CSV_PATH = 'data/labeled/telegram_data.csv'
PHOTOS_DIR = Path('data/labeled/photos')
OUTPUT_JSON = 'data/labeled/image_detections.json'

# Load CSV
df = pd.read_csv(CSV_PATH)

# Load YOLOv8 model (pre-trained)
model = YOLO('yolov8n.pt')  # You can use yolov8s.pt, yolov8m.pt, etc.

results_list = []

for idx, row in df.iterrows():
    media_path = row.get('Media Path')
    message_id = row.get('ID')
    if pd.isna(media_path) or not media_path:
        continue
    image_path = PHOTOS_DIR / os.path.basename(media_path)
    if not image_path.exists():
        continue

    # Run YOLO detection
    results = model(image_path)
    for result in results:
        for box in result.boxes:
            class_id = int(box.cls[0])
            class_name = model.names[class_id]
            confidence = float(box.conf[0])
            results_list.append({
                'message_id': message_id,
                'image_path': str(image_path),
                'detected_object_class': class_name,
                'confidence_score': confidence
            })

# Save results to JSON
with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
    json.dump(results_list, f, ensure_ascii=False, indent=2)

print(f"Detection results saved to {OUTPUT_JSON}")