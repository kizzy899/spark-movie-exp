import json
import os

from flask import Flask, jsonify, render_template

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TASK1_OUTPUT_PATH = os.path.join(BASE_DIR, "src", "task1_rdd_top20", "top20_output.json")
TASK2_OUTPUT_PATH = os.path.join(BASE_DIR, "outputs", "task2_gender_tags.json")

MYSQL_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "password": "123456",  # 改成你自己的数据库密码
    "database": "movie_analysis",
    "charset": "utf8mb4",
}

TASK2_SAMPLE_DATA = {
    "status": "sample",
    "message": "任务二 Spark SQL 结果尚未接入，当前为页面联调用示例数据。",
    "tags": ["Drama", "Comedy", "Action", "Romance", "Thriller", "Adventure"],
    "male": [1280, 1120, 980, 760, 690, 620],
    "female": [1180, 1040, 720, 890, 560, 510],
}


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/task1")
def task1():
    return render_template("task1.html")


@app.route("/task2")
def task2():
    return render_template("task2.html")


@app.route("/task3")
def task3():
    return render_template("task3.html")


@app.route("/api/top20")
def get_top20():
    """返回任务一已生成的Top20结果。"""
    try:
        with open(TASK1_OUTPUT_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return jsonify({"code": 200, "data": data})
    except FileNotFoundError:
        return jsonify({
            "code": 404,
            "message": "未找到任务一结果文件，请先运行 src/task1_rdd_top20/task1_top20.py",
        }), 404
    except Exception as exc:
        return jsonify({"code": 500, "message": str(exc)}), 500


@app.route("/api/gender-tags")
def get_gender_tags():
    """返回任务二男女标签偏好数据；正式版接入Spark SQL输出。"""
    if os.path.exists(TASK2_OUTPUT_PATH):
        try:
            with open(TASK2_OUTPUT_PATH, "r", encoding="utf-8") as f:
                return jsonify(json.load(f))
        except Exception as exc:
            return jsonify({"code": 500, "message": str(exc)}), 500

    return jsonify({"code": 200, "data": TASK2_SAMPLE_DATA})


@app.route("/api/latest")
def get_latest():
    """返回最近几个批次的Top5数据，供折线图使用。"""
    import pymysql

    conn = pymysql.connect(**MYSQL_CONFIG)
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT DISTINCT batch_time
                FROM streaming_results
                ORDER BY batch_time DESC
                LIMIT 8
            """)
            times = [row[0] for row in cursor.fetchall()]
            times.reverse()

            if not times:
                return jsonify({"times": [], "movies": {}})

            cursor.execute("""
                SELECT movie_id, title
                FROM streaming_results
                WHERE batch_time = %s
                ORDER BY avg_rating DESC
                LIMIT 5
            """, (times[-1],))
            top5 = cursor.fetchall()
            top5_ids = [row[0] for row in top5]
            top5_titles = {row[0]: row[1] for row in top5}

            if not top5_ids:
                return jsonify({"times": [str(batch_time) for batch_time in times], "movies": {}})

            movies_data = {title: [] for title in top5_titles.values()}

            for batch_time in times:
                cursor.execute("""
                    SELECT movie_id, avg_rating
                    FROM streaming_results
                    WHERE batch_time = %s AND movie_id IN %s
                """, (batch_time, tuple(top5_ids)))
                rows = {row[0]: row[1] for row in cursor.fetchall()}
                for movie_id in top5_ids:
                    title = top5_titles[movie_id]
                    avg_rating = rows.get(movie_id, None)
                    movies_data[title].append(float(avg_rating) if avg_rating is not None else None)

            return jsonify({
                "times": [str(batch_time) for batch_time in times],
                "movies": movies_data,
            })
    finally:
        conn.close()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)
