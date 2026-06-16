# 任务三：Spark Streaming 实时评分折线图

本目录存放任务三相关脚本。

## 文件说明

```text
streaming_job.py     Spark Streaming 监听评分批次并写入 MySQL
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

1. 根据本机 MySQL 修改 `streaming_job.py` 和根目录 `app.py` 中的数据库配置。
2. 准备 `~/movie_streaming/ratings.csv` 和 `~/movie_streaming/movies.csv`。
3. 切分评分文件：

```bash
python3 src/task3_streaming/split_data.py
```

4. 启动 Streaming：

```bash
python3 src/task3_streaming/streaming_job.py
```

5. 另开终端投递数据：

```bash
python3 src/task3_streaming/feed_data.py
```
