import pandas as pd
import os

# 路径配置
RATINGS_PATH = os.path.expanduser("~/movie_streaming/ratings.csv")
OUTPUT_DIR = os.path.expanduser("~/movie_streaming/streaming-done")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# 读取ratings.csv
df = pd.read_csv(RATINGS_PATH)
print(f"总数据量: {len(df)} 条")

# 切成10个批次
BATCH_NUM = 10
batch_size = len(df) // BATCH_NUM

for i in range(BATCH_NUM):
    start = i * batch_size
    end = start + batch_size if i < BATCH_NUM - 1 else len(df)
    batch = df.iloc[start:end]
    out_path = os.path.join(OUTPUT_DIR, f"ratings_batch_{i+1:02d}.csv")
    batch.to_csv(out_path, index=False)
    print(f"批次 {i+1}: {len(batch)} 条 -> {out_path}")

print("切割完成！")