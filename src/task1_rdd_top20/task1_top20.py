#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
任务一：基于Spark RDD的电影评分Top20分析
"""

from pyspark import SparkContext, SparkConf
import csv
import json
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DATA_DIR = "/Users/elemen/Downloads/moviedata-latest"
OUTPUT_PATH = os.path.join(SCRIPT_DIR, "top20_output.json")

# 最小评分人数阈值
MIN_RATINGS_COUNT = 10

def get_data_dir():
    return os.path.expanduser(os.environ.get("MOVIE_DATA_DIR", DEFAULT_DATA_DIR))

def to_spark_file_path(path):
    return f"file://{path}"

def parse_csv_line(line):
    return next(csv.reader([line]))

def parse_rating_line(line):
    fields = parse_csv_line(line)
    return int(fields[1]), float(fields[2])

def parse_movie_line(line):
    fields = parse_csv_line(line)
    return int(fields[0]), fields[1]

def main():
    data_dir = get_data_dir()
    ratings_path = to_spark_file_path(os.path.join(data_dir, "Ratings.csv"))
    movies_path = to_spark_file_path(os.path.join(data_dir, "Movies.csv"))

    print("=" * 60)
    print("任务一：Spark RDD 电影Top20分析")
    print("=" * 60)
    print(f"数据目录: {data_dir}")
    print(f"评分文件: {ratings_path}")
    print(f"电影文件: {movies_path}")
    print(f"输出路径: {OUTPUT_PATH}")
    print(f"最小评分人数: {MIN_RATINGS_COUNT}")
    print("=" * 60)

    # 初始化SparkContext
    conf = SparkConf().setAppName("MovieTop20").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")

    try:
        # 1. 读取评分数据
        print("\n[1/4] 读取评分数据...")
        raw_ratings = sc.textFile(ratings_path)
        header_ratings = raw_ratings.first()
        ratings = raw_ratings \
            .filter(lambda line: line != header_ratings) \
            .map(parse_rating_line)
        print(f"        评分数据量: {ratings.count()}")

        # 2. 读取电影数据
        print("\n[2/4] 读取电影数据...")
        raw_movies = sc.textFile(movies_path)
        header_movies = raw_movies.first()
        movies = raw_movies \
            .filter(lambda line: line != header_movies) \
            .map(parse_movie_line)
        print(f"        电影数据量: {movies.count()}")

        # 3. 计算平均分
        print("\n[3/4] 计算平均分...")
        ratings_with_count = ratings.mapValues(lambda r: (r, 1))
        aggregated = ratings_with_count.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
        filtered = aggregated.filter(lambda x: x[1][1] >= MIN_RATINGS_COUNT)
        avg_ratings = filtered.mapValues(lambda x: (x[0] / x[1], x[1]))
        print(f"        有效电影数(≥{MIN_RATINGS_COUNT}人评分): {avg_ratings.count()}")

        # 4. 关联电影名、排序、取Top20
        print("\n[4/4] 排序取Top20...")
        joined = avg_ratings.join(movies)
        mapped = joined.map(lambda x: (x[1][0][0], (x[0], x[1][1], x[1][0][1])))
        sorted_movies = mapped.sortByKey(ascending=False)
        top20_raw = sorted_movies.take(20)

        # 格式化结果
        top20 = []
        for rank, (avg_rating, (movie_id, title, rating_count)) in enumerate(top20_raw, start=1):
            top20.append({
                "rank": rank,
                "movieId": movie_id,
                "title": title,
                "avgRating": round(avg_rating, 2),
                "ratingCount": rating_count
            })

        # 保存结果
        output = {
            "task": "movie_top20",
            "description": "平均评分最高的前20部电影",
            "total_count": len(top20),
            "top20": top20
        }
        with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
            json.dump(output, f, ensure_ascii=False, indent=2)

        # 打印结果
        print("\n" + "=" * 60)
        print("Top20 高分电影")
        print("=" * 60)
        print(f"{'排名':<4} {'电影名称':<50} {'平均分':<6} {'评分人数':<8}")
        print("-" * 60)
        for movie in top20:
            title = movie['title'][:45] + "..." if len(movie['title']) > 48 else movie['title']
            print(f"{movie['rank']:<4} {title:<50} {movie['avgRating']:<6} {movie['ratingCount']:<8}")
        print("=" * 60)
        print(f"\n结果已保存至: {OUTPUT_PATH}")

    except Exception as e:
        print(f"执行出错: {e}")
        import traceback
        traceback.print_exc()
    finally:
        sc.stop()

if __name__ == "__main__":
    main()
