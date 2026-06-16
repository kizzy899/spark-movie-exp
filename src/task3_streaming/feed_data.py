import os
import time
from datetime import datetime


DEFAULT_STREAMING_DIR = "~/movie_streaming"
FEED_INTERVAL = int(os.environ.get("STREAMING_FEED_INTERVAL", "15"))


def env_path(name, default):
    return os.path.expanduser(os.environ.get(name, default))


def get_done_dir():
    return env_path("MOVIE_STREAMING_DONE_DIR", os.path.join(
        env_path("MOVIE_STREAMING_DIR", DEFAULT_STREAMING_DIR),
        "streaming-done",
    ))


def get_input_dir():
    return env_path("MOVIE_STREAMING_INPUT_DIR", os.path.join(
        env_path("MOVIE_STREAMING_DIR", DEFAULT_STREAMING_DIR),
        "streaming-input",
    ))


def get_tmp_dir():
    return env_path("MOVIE_STREAMING_TMP_DIR", os.path.join(
        env_path("MOVIE_STREAMING_DIR", DEFAULT_STREAMING_DIR),
        "streaming-tmp",
    ))


def main():
    done_dir = get_done_dir()
    input_dir = get_input_dir()
    tmp_dir = get_tmp_dir()
    os.makedirs(input_dir, exist_ok=True)
    os.makedirs(tmp_dir, exist_ok=True)

    files = sorted([f for f in os.listdir(done_dir) if f.endswith(".csv")])
    print(f"找到 {len(files)} 个批次文件")

    for i, fname in enumerate(files):
        src = os.path.join(done_dir, fname)
        stamped_name = f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{fname}"
        tmp = os.path.join(tmp_dir, stamped_name)
        dst = os.path.join(input_dir, stamped_name)

        with open(src, "r", encoding="utf-8") as f:
            content = f.read()
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(content)
        os.replace(tmp, dst)

        print(f"[批次 {i+1}/{len(files)}] 投递: {stamped_name}")
        time.sleep(FEED_INTERVAL)

    print("所有批次投递完成！")


if __name__ == "__main__":
    main()
