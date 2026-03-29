import os
import csv
from collections import defaultdict

root_path = "data"
rows = []

for tag_id in os.listdir(root_path):
    tag_folder = os.path.join(root_path, tag_id)
    if os.path.isdir(tag_folder):
        for sub_folder in os.listdir(tag_folder):
            if sub_folder.startswith("basic_category="):
                category_name = sub_folder.replace("basic_category=", "")
                rows.append({"tag_id": tag_id, "basic_category": category_name})

with open('tag_summary.csv', 'w', newline='', encoding='utf-8-sig') as f:
    writer = csv.DictWriter(f, fieldnames=["tag_id", "basic_category"])
    writer.writeheader()
    writer.writerows(rows)

print(f"CSV saved to tag_summary.csv ({len(rows)} rows)")