# 任务三：Spark Streaming 实时评分折线图

本目录存放任务三相关脚本。

## 文件说明

```text
streaming_job.py     Spark Streaming 监听评分批次，累计统计并写入 MySQL
split_data.py        将 ratings.csv 切分成多个批次文件
feed_data.py         模拟动态投递评分批次文件
legacy_index.html    旧版任务三页面，统一入口已改用 templates/task3.html
```

## 当前推荐入口

统一 Web 页面从根目录启动：

```bash
python3 app.py
```

访问：

```text
http://localhost:5001/task3
```

## 运行流程

1. 复制根目录 `.env.example` 为 `.env`，填写 `MOVIE_DATA_DIR` 和 MySQL 配置。
2. 创建 MySQL 表：

```bash
mysql -u root -p < sql/schema.sql
```

3. 切分评分文件：

```bash
make task3-clean
make task3-split
```

4. 启动 Streaming：

```bash
make task3-stream
```

5. 另开终端投递数据：

```bash
make task3-feed
```

## 配置项

```text
MOVIE_DATA_DIR              MovieLens 数据目录，包含 Ratings.csv 和 Movies.csv
MOVIE_STREAMING_DIR         任务三工作目录，默认 ~/movie_streaming
MOVIE_STREAMING_INPUT_DIR   Spark Streaming 监听目录，可选
MOVIE_STREAMING_DONE_DIR    切分后批次文件目录，可选
MYSQL_HOST                  MySQL 地址
MYSQL_PORT                  MySQL 端口
MYSQL_USER                  MySQL 用户
MYSQL_PASSWORD              MySQL 密码
MYSQL_DB                    MySQL 数据库
STREAMING_TOP_N             每批次写入 TopN，默认 5
STREAMING_MIN_RATING_COUNT  累计评分人数阈值，默认 50
STREAMING_RUN_ID            本次 Streaming 运行ID，可选；默认自动生成
```

`/api/latest` 会按最新 `run_id` 读取数据，避免多个 Streaming 进程同时写表时互相污染页面。
