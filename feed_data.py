import os
import time

DONE_DIR = os.path.expanduser("~/movie_streaming/streaming-done")
INPUT_DIR = os.path.expanduser("~/movie_streaming/streaming-input")

files = sorted([f for f in os.listdir(DONE_DIR) if f.endswith(".csv")])
print(f"找到 {len(files)} 个批次文件")

for i, fname in enumerate(files):
    src = os.path.join(DONE_DIR, fname)
    dst = os.path.join(INPUT_DIR, fname)
    
    # 读取内容再重新写入，确保时间戳是当前时间
    with open(src, 'r') as f:
        content = f.read()
    with open(dst, 'w') as f:
        f.write(content)
    
    print(f"[批次 {i+1}/{len(files)}] 投递: {fname}")
    time.sleep(15)

print("所有批次投递完成！")