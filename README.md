# 基于 Spark Streaming 的电影实时评分分析系统

任务三：Spark Streaming + ECharts 折线图实时展示

---

## 环境要求

| 工具 | 版本 |
|------|------|
| Python | 3.13（用 `/Library/Frameworks/Python.framework/Versions/3.13/bin/python3`） |
| Apache Spark | 4.1.2（Homebrew 安装） |
| MySQL | 8.0+ |
| Flask | 任意版本 |
| pymysql | 1.2.0 |

---

## 项目结构

```
movie_streaming/
├── streaming_job.py       # Spark Streaming 主程序
├── split_data.py          # 把 ratings.csv 切成10个批次
├── feed_data.py           # 模拟实时投递数据
├── app.py                 # Flask 后端接口
├── index.html             # ECharts 折线图前端页面
└── README.md
```

---

## 准备工作

### 1. 准备数据文件

把老师提供的数据文件放到项目目录下：

```
movie_streaming/
├── ratings.csv    ← 放这里
└── movies.csv     ← 放这里
```

### 2. 安装 Python 依赖

```bash
pip3 install flask pymysql pandas
```

### 3. MySQL 建库建表

登录 MySQL：

```bash
mysql -u root -p
```

执行以下 SQL：

```sql
CREATE DATABASE IF NOT EXISTS movie_analysis;
USE movie_analysis;

CREATE TABLE IF NOT EXISTS streaming_results (
    id INT AUTO_INCREMENT PRIMARY KEY,
    batch_time DATETIME NOT NULL,
    movie_id INT NOT NULL,
    title VARCHAR(255) DEFAULT '',
    avg_rating FLOAT NOT NULL,
    rating_count INT NOT NULL
);
```

### 4. 修改配置

打开 `streaming_job.py`，修改顶部配置区的 MySQL 密码：

```python
MYSQL_PASSWORD = "123456"  # ← 改这里
```

打开 `app.py`，同样修改密码：

```python
"password": "你的MySQL密码",  # ← 改这里
```

---

## 运行步骤（需要开 4 个终端）

### 第一步：切割数据

```bash
cd ~/movie_streaming
python3 split_data.py
```

成功后会在 `streaming-done/` 目录生成 10 个批次文件。

---

### 第二步：启动 Spark Streaming（终端1，保持运行）

```bash
cd ~/movie_streaming
PYSPARK_DRIVER_PYTHON=/Library/Frameworks/Python.framework/Versions/3.13/bin/python3 \
PYSPARK_PYTHON=/Library/Frameworks/Python.framework/Versions/3.13/bin/python3 \
spark-submit --master 'local[2]' streaming_job.py
```

等出现以下提示后再继续：

```
开始监听目录: .../streaming-input
请在另一个终端运行 feed_data.py 往目录里放文件
```

---

### 第三步：投递数据（终端2）

```bash
cd ~/movie_streaming
python3 feed_data.py
```

每 15 秒自动投递一个批次文件，共 10 批。  
终端1 会出现类似以下输出说明数据写入成功：

```
[2026-06-16 10:30:00] 写入 10 条到MySQL
```

---

### 第四步：启动 Flask 后端（终端3）

```bash
cd ~/movie_streaming
python3 app.py
```

---

### 第五步：打开浏览器查看折线图

访问：[http://127.0.0.1:5000](http://127.0.0.1:5000)

折线图每 5 秒自动刷新，随着数据批次增加可以看到折线实时延伸。

---

## 注意事项

- **必须先启动 Spark Streaming，再运行 feed_data.py**，顺序不能反。
- 每次重新演示前需要清空 `streaming-input/` 目录和 MySQL 表数据：
  ```bash
  rm -f ~/movie_streaming/streaming-input/*.csv
  mysql -u root -p -e "TRUNCATE TABLE movie_analysis.streaming_results;"
  ```
- Ctrl+C 停止 Spark 时出现的报错是正常退出信息，忽略即可。
- 如果 `streaming-input/` 目录不存在，手动创建：
  ```bash
  mkdir -p ~/movie_streaming/streaming-input
  ```

---

## 迁移到 Linux 主机（交给环境组）

只需改两处：

**streaming_job.py** 中把本地路径改为 HDFS 路径：

```python
STREAM_DIR = "hdfs://localhost:9000/movie/streaming-input"
MOVIES_PATH = "hdfs://localhost:9000/movie/data/movies.csv"
```

**feed_data.py** 中把文件复制改为 HDFS 上传：

```python
os.system(f"hdfs dfs -put {src} /movie/streaming-input/")
```

其他代码不需要改动。
