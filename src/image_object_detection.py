import os
import pandas as pd
from ultralytics import YOLO
from pathlib import Path
import re

# --- CONFIGURATION ---
IMAGES_DIR = Path("data/labeled/photos")  # <-- Update this to your actual images folder
OUTPUT_CSV = "data/raw/image_detections.csv"  # Where to save detection results

# --- HELPER FUNCTION ---
def extract_message_id(image_path):
    """
    Extracts the message_id from filenames like '@lobelia4cosmetics_18511.jpg'
    or '@tikvahpharma_172099.jpg'.
    """
    name = image_path.stem  # e.g., '@lobelia4cosmetics_18511'
    match = re.search(r'_(\d+)$', name)
    if match:
        return int(match.group(1))
    return None

# --- LOAD YOLOv8 MODEL ---
model = YOLO("yolov8n.pt")  # You can use yolov8s.pt, yolov8m.pt, etc.

results_list = []

# --- SCAN AND DETECT ---
for image_path in IMAGES_DIR.glob("*.jpg"):  # Adjust extension if needed
    message_id = extract_message_id(image_path)
    if message_id is None:
        print(f"Skipping {image_path.name}: could not extract message_id")
        continue

    results = model(image_path)
    for r in results:
        for box in r.boxes:
            results_list.append({
                "message_id": message_id,
                "image_filename": image_path.name,
                "detected_object_class": model.names[int(box.cls)],
                "confidence_score": float(box.conf),
                "bbox": box.xyxy.tolist()  # Optional: bounding box coordinates
            })

# --- SAVE RESULTS ---
if results_list:
    df = pd.DataFrame(results_list)
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"Detection results saved to {OUTPUT_CSV}")
else:
    print("No detections found.")
