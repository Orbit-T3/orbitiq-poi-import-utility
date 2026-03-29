import os
import json
from collections import defaultdict

root_path = "data"  # ระบุ Path หลักของคุณ
tag_summary = defaultdict(list)

# วนลูปดูโฟลเดอร์ทั้งหมด
for tag_id in os.listdir(root_path):
    tag_folder = os.path.join(root_path, tag_id)
    if os.path.isdir(tag_folder):
        # เข้าไปดูข้างใน tagId เพื่อหาโฟลเดอร์ basic_category=...
        for sub_folder in os.listdir(tag_folder):
            if sub_folder.startswith("basic_category="):
                # ดึงชื่อหลังเครื่องหมาย = ออกมา
                category_name = sub_folder
                tag_summary[tag_id].append(category_name)


# Convert defaultdict to regular dict and output as JSON
output_data = dict(tag_summary)
with open('tag_summary.json', 'w', encoding='utf-8') as f:
    json.dump(output_data, f, ensure_ascii=False, indent=2)

print("JSON output saved to tag_summary.json")