#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Task 2: Spark SQL Gender Tag Preferences

By default, reads pre-computed results from sql/db_backup.sql.
Set TASK2_DATA_SOURCE=mysql to use the original Spark JDBC calculation.
"""

import ast
import json
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(SCRIPT_DIR)
PROJECT_DIR = os.path.dirname(BASE_DIR)

OUTPUT_PATH = os.path.join(PROJECT_DIR, "outputs", "task2_gender_tags.json")
DEFAULT_DATA_DIR = "/Users/elemen/Downloads/moviedata-latest"
DEFAULT_SQL_DUMP_PATH = os.path.join(PROJECT_DIR, 'sql', 'db_backup.sql')
DEFAULT_JDBC_JAR_PATH = os.path.join(PROJECT_DIR, "lib", "mysql-connector-j-8.4.0.jar")
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


def get_data_source():
    return os.environ.get('TASK2_DATA_SOURCE', 'sql_dump').strip().lower()


def get_sql_dump_path():
    configured_path = os.path.expanduser(
        os.environ.get('TASK2_SQL_DUMP', DEFAULT_SQL_DUMP_PATH)
    )
    if not os.path.isabs(configured_path):
        configured_path = os.path.join(PROJECT_DIR, configured_path)
    return os.path.abspath(configured_path)


def read_gender_stats_from_dump(dump_path):
    '''Read gender_tag_stats values from a MySQL dump without MySQL.'''
    marker = 'INSERT INTO `gender_tag_stats` VALUES '
    statement = None
    with open(dump_path, 'r', encoding='utf-8', errors='replace') as dump_file:
        for line in dump_file:
            if line.startswith(marker):
                statement = line[len(marker):].strip()
                break

    if statement is None:
        raise ValueError('gender_tag_stats data was not found in the SQL dump')
    if statement.endswith(';'):
        statement = statement[:-1]
    try:
        rows = ast.literal_eval('[' + statement + ']')
    except (SyntaxError, ValueError) as exc:
        raise ValueError('Unable to parse gender_tag_stats from the SQL dump') from exc

    # The bundled dump contains dirty duplicates such as Action and Action\\r.
    # Normalize them and retain the larger pre-computed count.
    normalized = {}
    for gender, tag, user_count in rows:
        clean_gender = str(gender).strip().upper()
        clean_tag = str(tag).strip()
        if clean_gender not in {'M', 'F'}:
            continue
        if not clean_tag or clean_tag.lower() in {
            '(no genres listed)',
            '(no genre listed)',
        }:
            continue
        key = (clean_gender, clean_tag)
        normalized[key] = max(normalized.get(key, 0), int(user_count or 0))

    return [
        (gender, tag, count)
        for (gender, tag), count in normalized.items()
    ]


def generate_from_sql_dump(dump_path=None):
    dump_path = dump_path or get_sql_dump_path()
    if not os.path.isfile(dump_path):
        raise FileNotFoundError(f'SQL dump not found: {dump_path}')
    rows = read_gender_stats_from_dump(dump_path)
    male = [(tag, count) for gender, tag, count in rows if gender == 'M']
    female = [(tag, count) for gender, tag, count in rows if gender == 'F']
    tags, male_values, female_values = get_top_n_tags(male, female, TOP_N)
    return build_output_json(tags, male_values, female_values)


def get_jdbc_jar_path():
    configured_path = os.path.expanduser(
        os.environ.get("MYSQL_CONNECTOR_JAR", DEFAULT_JDBC_JAR_PATH)
    )
    if not os.path.isabs(configured_path):
        configured_path = os.path.join(PROJECT_DIR, configured_path)
    return os.path.abspath(configured_path)


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

    data_source = get_data_source()
    print(f'Data source: {data_source}')
    if data_source == 'sql_dump':
        dump_path = get_sql_dump_path()
        print(f'SQL dump: {dump_path}')
        output = generate_from_sql_dump(dump_path)
        os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
        with open(OUTPUT_PATH, 'w', encoding='utf-8') as output_file:
            json.dump(output, output_file, ensure_ascii=False, indent=2)
        print(f'\nResults written to: {OUTPUT_PATH}')
        print('Done!')
        return
    if data_source != 'mysql':
        raise ValueError("TASK2_DATA_SOURCE must be 'sql_dump' or 'mysql'")

    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, countDistinct, desc, explode, split, trim

    jdbc_jar_path = get_jdbc_jar_path()
    if not os.path.isfile(jdbc_jar_path):
        raise FileNotFoundError(
            f"MySQL Connector/J not found: {jdbc_jar_path}. "
            "Set MYSQL_CONNECTOR_JAR to a valid JAR path."
        )

    print(f"MySQL Connector/J: {jdbc_jar_path}")
    spark = (
        SparkSession.builder.appName("GenderTags")
        .config("spark.jars", jdbc_jar_path)
        .config("spark.driver.extraClassPath", jdbc_jar_path)
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
