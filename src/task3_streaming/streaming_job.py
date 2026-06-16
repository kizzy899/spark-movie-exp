import os
import time
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import pymysql
from datetime import datetime

# ===== 配置区（只需改这里）=====
MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASSWORD = "123456"  # ← 务必替换成你自己数据库密码
MYSQL_DB = "movie_analysis"

MOVIES_PATH = os.path.expanduser("~/movie_streaming/movies.csv")
STREAM_DIR = os.path.expanduser("~/movie_streaming/streaming-input")
BATCH_INTERVAL = 10  # 秒
# ================================

def get_db_conn():
    return pymysql.connect(
        host=MYSQL_HOST, port=MYSQL_PORT,
        user=MYSQL_USER, password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        charset="utf8mb4",
        use_unicode=True
    )

def load_movies(spark_context):
    """加载movies.csv，返回 {movieId: title} 字典"""
    movies_rdd = spark_context.textFile(f"file://{MOVIES_PATH}")
    header = movies_rdd.first()
    movies = movies_rdd.filter(lambda line: line != header) \
        .map(lambda line: line.split(",", 2)) \
        .filter(lambda f: len(f) >= 2) \
        .map(lambda f: (int(f[0]), f[1].strip('"')))
    return dict(movies.collect())

def save_to_mysql(records, batch_time):
    """保存Top10到MySQL"""
    if not records:
        return
    conn = get_db_conn()
    try:
        with conn.cursor() as cursor:
            sql = """INSERT INTO streaming_results 
                     (batch_time, movie_id, title, avg_rating, rating_count)
                     VALUES (%s, %s, %s, %s, %s)"""
            for movie_id, title, avg_rating, count in records:
                cursor.execute(sql, (batch_time, movie_id, title, round(avg_rating, 2), count))
        conn.commit()
        print(f"[{batch_time}] 写入 {len(records)} 条到MySQL")
    finally:
        conn.close()

def process_rdd(rdd, movies_dict):
    """处理每个批次的RDD"""
    if rdd.isEmpty():
        print("本批次无新数据")
        return

    try:
        header_check = rdd.first()
        # 过滤表头和空行
        data = rdd.filter(lambda line: line and not line.startswith("userId")) \
            .map(lambda line: line.split(",")) \
            .filter(lambda f: len(f) >= 3) \
            .map(lambda f: (int(f[1]), float(f[2])))  # (movieId, rating)

        # 统计每部电影的评分总和和数量
        stats = data.mapValues(lambda r: (r, 1)) \
            .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
            .filter(lambda x: x[1][1] >= 2) \
            .mapValues(lambda v: v[0] / v[1]) \
            .sortBy(lambda x: x[1], ascending=False) \
            .take(10)

        batch_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        records = []
        for movie_id, avg_rating in stats:
            title = movies_dict.get(movie_id, f"Movie_{movie_id}")
            records.append((movie_id, title, avg_rating, 1))
        # 完全删除打印片名代码，不再触发编码报错

        save_to_mysql(records, batch_time)

    except Exception as e:
        print(f"处理批次出错: {e}")

def main():
    os.makedirs(STREAM_DIR, exist_ok=True)

    conf = SparkConf().setAppName("MovieStreaming").setMaster("local[2]")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")

    print("加载电影数据...")
    movies_dict = load_movies(sc)
    print(f"加载了 {len(movies_dict)} 部电影")

    ssc = StreamingContext(sc, BATCH_INTERVAL)
    ssc.checkpoint("/tmp/spark_checkpoint")

    # 监听本地目录
    stream = ssc.textFileStream(f"file://{STREAM_DIR}")
    stream.foreachRDD(lambda rdd: process_rdd(rdd, movies_dict))

    print(f"开始监听目录: {STREAM_DIR}")
    print("请在另一个终端运行 feed_data.py 往目录里放文件")
    print("Ctrl+C 停止")

    ssc.start()
    ssc.awaitTermination()

if __name__ == "__main__":
    main()