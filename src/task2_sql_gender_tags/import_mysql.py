#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Task 2: Data import script.
Imports Users.dat (:: delimited) and Ratings.csv into MySQL.
"""

import csv
import os
import time

import pymysql

DEFAULT_DATA_DIR = "/Users/elemen/Downloads/moviedata-latest"


def env_value(name, default):
    return os.path.expanduser(os.environ.get(name, default))


def get_data_dir():
    return env_value("MOVIE_DATA_DIR", DEFAULT_DATA_DIR)


def get_mysql_config():
    return {
        "host": os.environ.get("MYSQL_HOST", "127.0.0.1"),
        "port": int(os.environ.get("MYSQL_PORT", "3306")),
        "user": os.environ.get("MYSQL_USER", "root"),
        "password": os.environ.get("MYSQL_PASSWORD", "123456"),
        "database": os.environ.get("MYSQL_DB", "movie_analysis"),
        "charset": "utf8mb4",
    }


def parse_users_dat_line(line):
    """Parse a :: delimited Users.dat line. Returns dict or None."""
    line = line.strip()
    if not line:
        return None
    fields = line.split("::")
    if len(fields) < 2:
        return None
    return {
        "userId": int(fields[0]),
        "gender": fields[1],
        "age": int(fields[2]) if len(fields) > 2 and fields[2] else None,
        "occupation": int(fields[3]) if len(fields) > 3 and fields[3] else None,
        "zipCode": fields[4].strip() if len(fields) > 4 else None,
    }


def parse_rating_csv_line(line):
    """Parse a CSV line from Ratings.csv (header skipped). Returns dict or None."""
    fields = next(csv.reader([line]))
    if len(fields) < 4:
        return None
    return {
        "userId": int(fields[0]),
        "movieId": int(fields[1]),
        "rating": float(fields[2]),
        "timestamp": int(fields[3]),
    }


def import_users(filepath, conn):
    """Import Users.dat into MySQL.users table."""
    if not os.path.exists(filepath):
        print("[users] File not found:", filepath)
        return 0
    count = 0
    with conn.cursor() as cursor:
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                user = parse_users_dat_line(line)
                if user is None:
                    continue
                cursor.execute(
                    "INSERT IGNORE INTO users (userId, gender, age, occupation, zipCode) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (user["userId"], user["gender"], user["age"],
                     user["occupation"], user["zipCode"]),
                )
                count += 1
                if count % 50000 == 0:
                    conn.commit()
                    print("    Imported", count, "users...")
        conn.commit()
    return count


def import_ratings(filepath, conn, batch_size=50000):
    """Import Ratings.csv into MySQL.ratings table."""
    if not os.path.exists(filepath):
        print("[ratings] File not found:", filepath)
        return 0
    count = 0
    batch = []
    with conn.cursor() as cursor:
        with open(filepath, "r", encoding="utf-8") as f:
            header = f.readline()
            for line in f:
                rating = parse_rating_csv_line(line)
                if rating is None:
                    continue
                batch.append((
                    rating["userId"],
                    rating["movieId"],
                    rating["rating"],
                    rating["timestamp"],
                ))
                count += 1
                if len(batch) >= batch_size:
                    cursor.executemany(
                        "INSERT IGNORE INTO ratings (userId, movieId, rating, timestamp) "
                        "VALUES (%s, %s, %s, %s)",
                        batch,
                    )
                    conn.commit()
                    batch.clear()
                    print("    Imported", count, "ratings...")
        if batch:
            cursor.executemany(
                "INSERT IGNORE INTO ratings (userId, movieId, rating, timestamp) "
                "VALUES (%s, %s, %s, %s)",
                batch,
            )
            conn.commit()
    return count


def main():
    data_dir = get_data_dir()
    users_path = os.path.join(data_dir, "Users.dat")
    ratings_path = os.path.join(data_dir, "Ratings.csv")

    print("=" * 60)
    print("Task 2: Data Import to MySQL")
    print("=" * 60)
    print("Data dir:", data_dir)
    print("Users.dat:", users_path)
    print("Ratings.csv:", ratings_path)
    print("=" * 60)

    if not os.path.exists(data_dir):
        print("ERROR: Data directory not found:", data_dir)
        print("Extract docs/plans/moviedata-latest.rar and set MOVIE_DATA_DIR")
        return

    try:
        conn = pymysql.connect(**get_mysql_config())
        print("MySQL connected")
    except pymysql.err.MySQLError as e:
        print("MySQL connection failed:", e)
        return

    try:
        print("[1/2] Importing users...")
        t0 = time.time()
        user_count = import_users(users_path, conn)
        t1 = time.time()
        print("    Users:", user_count, "({:.1f}s)".format(t1 - t0))

        print("[2/2] Importing ratings...")
        t0 = time.time()
        rating_count = import_ratings(ratings_path, conn)
        t1 = time.time()
        print("    Ratings:", rating_count, "({:.1f}s)".format(t1 - t0))

        print("=" * 60)
        print("Import complete!")
        print("  users table:", user_count)
        print("  ratings table:", rating_count)
        print("=" * 60)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
