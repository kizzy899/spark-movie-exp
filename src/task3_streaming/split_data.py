import os

import pandas as pd


DEFAULT_DATA_DIR = "/Users/elemen/Downloads/moviedata-latest"
DEFAULT_STREAMING_DIR = "~/movie_streaming"
BATCH_NUM = int(os.environ.get("STREAMING_BATCH_NUM", "10"))


def env_path(name, default):
    return os.path.expanduser(os.environ.get(name, default))


def get_ratings_path():
    return env_path("MOVIE_STREAMING_RATINGS_PATH", os.path.join(
        env_path("MOVIE_DATA_DIR", DEFAULT_DATA_DIR),
        "Ratings.csv",
    ))


def get_output_dir():
    return env_path("MOVIE_STREAMING_DONE_DIR", os.path.join(
        env_path("MOVIE_STREAMING_DIR", DEFAULT_STREAMING_DIR),
        "streaming-done",
    ))


def main():
    ratings_path = get_ratings_path()
    output_dir = get_output_dir()
    os.makedirs(output_dir, exist_ok=True)

    df = pd.read_csv(ratings_path)
    print(f"总数据量: {len(df)} 条")

    batch_size = len(df) // BATCH_NUM
    for i in range(BATCH_NUM):
        start = i * batch_size
        end = start + batch_size if i < BATCH_NUM - 1 else len(df)
        batch = df.iloc[start:end]
        out_path = os.path.join(output_dir, f"ratings_batch_{i+1:02d}.csv")
        batch.to_csv(out_path, index=False)
        print(f"批次 {i+1}: {len(batch)} 条 -> {out_path}")

    print("切割完成！")


if __name__ == "__main__":
    main()
