#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Task 2: Spark SQL Gender Tag Preferences

Reads MySQL.users + MySQL.ratings (JDBC) and Movies.csv (local file).
Explodes genres by |, joins, groups by gender+genre, takes top N per gender.
Outputs JSON with union of tags.
"""

import json
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, desc, explode, split, trim

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(SCRIPT_DIR)
PROJECT_DIR = os.path.dirname(BASE_DIR)

OUTPUT_PATH = os.path.join(PROJECT_DIR, "outputs", "task2_gender_tags.json")
DEFAULT_DATA_DIR = "/Users/elemen/Downloads/moviedata-latest"
TOP_N = int(os.environ.get("TASK2_TOP_N", "8"))

MYSQL_CONFIG = {
    "host": os.environ.get("MYSQL_HOST", "127.0.0.1"),
    "port": int(os.environ.get("MYSQL_PORT", "3306")),
    "user": os.environ.get("MYSQL_USER", "root"),
    "password": os.environ.get("MYSQL_PASSWORD", "123456"),
    "database": os.environ.get("MYSQL_DB", "movie_analysis"),
}


def explode_genres(genres_str):
    """Split pipe-delimited genres, filter empty and no-genre entries."""
    if not genres_str:
        return []
    result = [g.strip() for g in genres_str.split("|") if g.strip()]
    return [g for g in result if g != "(No genres listed)"]


def get_top_n_tags(male_data, female_data, top_n=8):
    """Take top N per gender, union with male priority, zero-fill."""
    male_sorted = sorted(male_data, key=lambda x: x[1], reverse=True)[:top_n]
    female_sorted = sorted(female_data, key=lambda x: x[1], reverse=True)[:top_n]
    seen = set()
    ordered_tags = []
    for genre, _ in male_sorted:
        if genre not in seen:
            ordered_tags.append(genre)
            seen.add(genre)
    for genre, _ in female_sorted:
        if genre not in seen:
            ordered_tags.append(genre)
            seen.add(genre)
    male_dict = dict(male_sorted)
    female_dict = dict(female_sorted)
    male_values = [male_dict.get(tag, 0) for tag in ordered_tags]
    female_values = [female_dict.get(tag, 0) for tag in ordered_tags]
    return ordered_tags, male_values, female_values


def build_output_json(tags, male_values, female_values):
    """Build the API response JSON structure."""
    return {
        "code": 200,
        "data": {
            "status": "real",
            "message": "Spark SQL statistic result",
            "tags": tags,
            "male": male_values,
            "female": female_values,
        },
    }


def get_data_dir():
    return os.path.expanduser(os.environ.get("MOVIE_DATA_DIR", DEFAULT_DATA_DIR))


def get_movies_path():
    return os.path.join(get_data_dir(), "Movies.csv")


def to_spark_file_path(path):
    return f"file://{path}"


def get_jdbc_url():
    c = MYSQL_CONFIG
    return f"jdbc:mysql://{c['host']}:{c['port']}/{c['database']}?useSSL=false"


def read_mysql_table(spark, table):
    """Read MySQL table via JDBC as DataFrame."""
    return (
        spark.read.format("jdbc")
        .options(
            url=get_jdbc_url(),
            driver="com.mysql.cj.jdbc.Driver",
            dbtable=table,
            user=MYSQL_CONFIG["user"],
            password=MYSQL_CONFIG["password"],
        )
        .load()
    )


def main():
    print("=" * 60)
    print("Task 2: Spark SQL Gender Tag Preferences")
    print("=" * 60)

    spark = (
        SparkSession.builder.appName("GenderTags")
        .config("spark.jars", "mysql-connector-java-x.x.x.jar")
        .config("spark.driver.extraClassPath", "mysql-connector-java-x.x.x.jar")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        print("\n[1/4] Reading MySQL.users...")
        users_df = read_mysql_table(spark, "users")
        print(f"    Users: {users_df.count()}")

        print("\n[2/4] Reading MySQL.ratings...")
        ratings_df = read_mysql_table(spark, "ratings")
        print(f"    Ratings: {ratings_df.count()}")

        print("\n[3/4] Reading Movies.csv and exploding genres...")
        movies_df = spark.read.option("header", "true").csv(
            to_spark_file_path(get_movies_path())
        )
        genres_df = (
            movies_df.select(
                col("movieId"),
                explode(split(col("genres"), "\\|")).alias("genre"),
            )
            .filter(
                (trim(col("genre")) != "")
                & (col("genre") != "(No genres listed)")
            )
        )
        print(f"    Genre mappings: {genres_df.count()}")

        print("\n[4/4] Computing gender + genre stats...")
        result_df = (
            ratings_df.alias("r")
            .join(users_df.alias("u"), "userId")
            .join(genres_df.alias("g"), "movieId")
            .groupBy("u.gender", "g.genre")
            .agg(countDistinct("r.userId").alias("user_count"))
        )

        male_rows = (
            result_df.filter(col("gender") == "M")
            .orderBy(desc("user_count"))
            .limit(TOP_N)
            .collect()
        )
        female_rows = (
            result_df.filter(col("gender") == "F")
            .orderBy(desc("user_count"))
            .limit(TOP_N)
            .collect()
        )

        male_top = [(row["genre"], row["user_count"]) for row in male_rows]
        female_top = [(row["genre"], row["user_count"]) for row in female_rows]

        tags, male_vals, female_vals = get_top_n_tags(male_top, female_top, TOP_N)
        output = build_output_json(tags, male_vals, female_vals)

        print(f"\n    Male top {TOP_N}: {dict(male_top)}")
        print(f"    Female top {TOP_N}: {dict(female_top)}")
        print(f"    Union tags: {tags}")

        os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
        with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
            json.dump(output, f, ensure_ascii=False, indent=2)

        print(f"\nResults written to: {OUTPUT_PATH}")
        print("Done!")

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
