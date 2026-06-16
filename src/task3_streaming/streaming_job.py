import csv
import os
import uuid
from datetime import datetime

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import pymysql

DEFAULT_DATA_DIR = "/Users/elemen/Downloads/moviedata-latest"
DEFAULT_STREAMING_DIR = "~/movie_streaming"
TOP_N = int(os.environ.get("STREAMING_TOP_N", "5"))
BATCH_INTERVAL = int(os.environ.get("STREAMING_BATCH_INTERVAL", "10"))
MIN_RATING_COUNT = int(os.environ.get("STREAMING_MIN_RATING_COUNT", "50"))
RATING_STATE = {}
RUN_ID = os.environ.get("STREAMING_RUN_ID", datetime.now().strftime("%Y%m%d%H%M%S") + "-" + uuid.uuid4().hex[:8])


def env_value(name, default):
    return os.path.expanduser(os.environ.get(name, default))


def get_data_dir():
    return env_value("MOVIE_DATA_DIR", DEFAULT_DATA_DIR)


def get_streaming_dir():
    return env_value("MOVIE_STREAMING_DIR", DEFAULT_STREAMING_DIR)


def get_movies_path():
    return env_value("MOVIE_STREAMING_MOVIES_PATH", os.path.join(get_data_dir(), "Movies.csv"))


def get_stream_dir():
    return env_value("MOVIE_STREAMING_INPUT_DIR", os.path.join(get_streaming_dir(), "streaming-input"))


def to_spark_file_path(path):
    return f"file://{path}"


def parse_csv_line(line):
    return next(csv.reader([line]))


def parse_movie_line(line):
    fields = parse_csv_line(line)
    return int(fields[0]), fields[1]


def parse_rating_line(line):
    fields = parse_csv_line(line)
    return int(fields[1]), float(fields[2])


def get_mysql_config():
    return {
        "host": os.environ.get("MYSQL_HOST", "127.0.0.1"),
        "port": int(os.environ.get("MYSQL_PORT", "3306")),
        "user": os.environ.get("MYSQL_USER", "root"),
        "password": os.environ.get("MYSQL_PASSWORD", "123456"),
        "database": os.environ.get("MYSQL_DB", "movie_analysis"),
        "charset": "utf8mb4",
        "use_unicode": True,
    }

def get_db_conn():
    return pymysql.connect(**get_mysql_config())

def load_movies(spark_context):
    """加载movies.csv，返回 {movieId: title} 字典"""
    movies_path = get_movies_path()
    movies_rdd = spark_context.textFile(to_spark_file_path(movies_path))
    header = movies_rdd.first()
    movies = movies_rdd.filter(lambda line: line != header) \
        .map(parse_movie_line)
    return dict(movies.collect())

def build_top_records(stats, movies_dict):
    records = []
    for movie_id, (avg_rating, count) in stats:
        title = movies_dict.get(movie_id, f"Movie_{movie_id}")
        records.append((movie_id, title, avg_rating, count))
    return records

def update_rating_state(new_values, previous_state):
    total = previous_state[0] if previous_state else 0.0
    count = previous_state[1] if previous_state else 0

    for rating_sum, rating_count in new_values:
        total += rating_sum
        count += rating_count

    return total, count

def state_rows_to_top_stats(rows, min_count=MIN_RATING_COUNT, top_n=TOP_N):
    stats = []
    for movie_id, (rating_sum, count) in rows:
        if count >= min_count:
            stats.append((movie_id, (rating_sum / count, count)))

    return sorted(stats, key=lambda item: item[1][0], reverse=True)[:top_n]

def update_cumulative_state(batch_stats, state):
    for movie_id, (rating_sum, count) in batch_stats:
        old_sum, old_count = state.get(movie_id, (0.0, 0))
        state[movie_id] = (old_sum + rating_sum, old_count + count)

def save_to_mysql(records, batch_time):
    """保存TopN到MySQL"""
    if not records:
        return
    conn = get_db_conn()
    try:
        with conn.cursor() as cursor:
            sql = """INSERT INTO streaming_results 
                     (run_id, batch_time, movie_id, title, avg_rating, rating_count)
                     VALUES (%s, %s, %s, %s, %s, %s)"""
            for movie_id, title, avg_rating, count in records:
                cursor.execute(sql, (RUN_ID, batch_time, movie_id, title, round(avg_rating, 2), count))
        conn.commit()
        print(f"[{batch_time}] 写入 {len(records)} 条到MySQL")
    finally:
        conn.close()

def process_rdd(rdd, movies_dict, state=RATING_STATE):
    """只在有新评分文件的批次更新累计状态并写入MySQL。"""
    if rdd.isEmpty():
        print("本批次无新数据")
        return

    try:
        batch_stats = rdd.filter(lambda line: line and not line.startswith("userId")) \
            .map(parse_rating_line) \
            .mapValues(lambda rating: (rating, 1)) \
            .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
            .collect()

        update_cumulative_state(batch_stats, state)
        stats = state_rows_to_top_stats(state.items())

        batch_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        records = build_top_records(stats, movies_dict)
        save_to_mysql(records, batch_time)

    except Exception as e:
        print(f"处理批次出错: {e}")

def main():
    stream_dir = get_stream_dir()
    os.makedirs(stream_dir, exist_ok=True)

    conf = SparkConf().setAppName("MovieStreaming").setMaster("local[2]")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")

    print("加载电影数据...")
    movies_dict = load_movies(sc)
    print(f"加载了 {len(movies_dict)} 部电影")

    ssc = StreamingContext(sc, BATCH_INTERVAL)

    # 监听本地目录
    stream = ssc.textFileStream(to_spark_file_path(stream_dir))
    stream.foreachRDD(lambda rdd: process_rdd(rdd, movies_dict))

    print(f"开始监听目录: {stream_dir}")
    print(f"当前运行ID: {RUN_ID}")
    print(f"累计统计阈值: 评分人数 >= {MIN_RATING_COUNT}, TopN = {TOP_N}")
    print("请在另一个终端运行 make task3-feed 往目录里放文件")
    print("Ctrl+C 停止")

    ssc.start()
    ssc.awaitTermination()

if __name__ == "__main__":
    main()
