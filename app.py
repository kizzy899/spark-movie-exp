import os
from flask import Flask, jsonify, render_template_string
import pymysql
from datetime import datetime

app = Flask(__name__)

MYSQL_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "password": "123456",  # ← 改成你自己的数据库密码
    "database": "movie_analysis",
    "charset": "utf8mb4"
}

HTML = open(os.path.join(os.path.dirname(__file__), "index.html"), encoding="utf8").read()

@app.route("/")
def index():
    return render_template_string(HTML)

@app.route("/api/latest")
def get_latest():
    """返回最近几个批次的Top5数据，供折线图使用"""
    conn = pymysql.connect(**MYSQL_CONFIG)
    try:
        with conn.cursor() as cursor:
            # 取最近8个批次时间点
            cursor.execute("""
                SELECT DISTINCT batch_time 
                FROM streaming_results 
                ORDER BY batch_time DESC 
                LIMIT 8
            """)
            times = [row[0] for row in cursor.fetchall()]
            times.reverse()  # 时间正序

            if not times:
                return jsonify({"times": [], "movies": {}})

            # 取最新批次里评分最高的Top5电影
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

            # 构造折线图数据
            movies_data = {title: [] for title in top5_titles.values()}

            for t in times:
                cursor.execute("""
                    SELECT movie_id, avg_rating 
                    FROM streaming_results 
                    WHERE batch_time = %s AND movie_id IN %s
                """, (t, tuple(top5_ids)))
                rows = {row[0]: row[1] for row in cursor.fetchall()}
                for mid in top5_ids:
                    title = top5_titles[mid]
                    movies_data[title].append(rows.get(mid, None))

            time_labels = [str(t) for t in times]
            return jsonify({"times": time_labels, "movies": movies_data})
    finally:
        conn.close()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)