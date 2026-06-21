# spark-movie-exp

基于 MovieLens 数据集的 Spark 电影数据分析实验项目。项目目标是整合 Linux、Hadoop、Spark、MySQL、ECharts 和 Web 页面，完成三个数据分析展示任务。

## 当前开发方向

本仓库采用轻量 Web 结构：

```text
首页
├── 任务一：Top20 高分电影表格
├── 任务二：男女标签偏好柱状图
└── 任务三：实时评分折线图
```

统一 Web 入口为根目录的 `app.py`，使用 Flask 提供页面路由和 JSON 接口。

## 功能状态

| 模块 | 要求 | 当前状态 |
| --- | --- | --- |
| 任务一 | Spark RDD 计算电影平均分，输出 Top20 并页面展示 | 已有 Spark 脚本、JSON 结果和统一页面 |
| 任务二 | Spark SQL 访问 MySQL，统计男女关注最多的电影标签并柱状图展示 | 页面和接口占位已建，等待 Spark SQL 结果接入 |
| 任务三 | Spark Streaming 动态接收评分文件，折线图实时展示 | 已有 Streaming 脚本、投递脚本和统一页面 |

## 主要目录

```text
.
├── app.py                         # 统一 Flask Web 入口
├── templates/                     # 首页和三个任务页面
├── src/
│   ├── task1_rdd_top20/            # 任务一 RDD Top20 代码和旧版页面
│   ├── task2_sql_gender_tags/      # 任务二 Spark SQL 代码预留目录
│   └── task3_streaming/            # 任务三 Streaming 脚本和旧版页面
├── outputs/                        # Spark 结果输出，供 Web 读取
├── docs/plans/                     # 实验要求、任务拆解和报告模板
└── requirements.txt                # Python 依赖
```

## Web 页面路由

启动统一 Web 服务后访问：

```text
http://localhost:5001/        首页
http://localhost:5001/task1   任务一 Top20 表格
http://localhost:5001/task2   任务二 男女标签柱状图
http://localhost:5001/task3   任务三 实时折线图
```

对应接口：

```text
GET /api/top20         读取任务一 Top20 JSON
GET /api/gender-tags   任务二标签偏好数据，当前为示例数据
GET /api/latest        读取任务三 MySQL 实时统计结果
```

## 快速启动 Web

推荐使用 Java 8 和 Python 3.11。根目录已提供 `Makefile`，第一次运行先安装依赖：

```bash
make setup
```

复制环境变量示例文件，并按本机数据路径修改：

```bash
cp .env.example .env
```

启动统一 Web 服务：

```bash
make web
```

浏览器访问：

```text
http://localhost:5001/
```

常用命令：

```bash
make help          # 查看命令列表
make check         # 编译检查主要 Python 文件
make routes        # 打印 Flask 路由，需要先安装依赖
make task1         # 重新生成任务一 Top20 结果
make task3-split   # 切分任务三评分批次文件
make task3-stream  # 启动任务三 Spark Streaming
make task3-feed    # 投递任务三评分批次文件
```

如果当前环境不支持 `make`，也可以直接运行：

```bash
python3 app.py
```

## 任务一：RDD Top20

相关文件：

```text
src/task1_rdd_top20/task1_top20.py
src/task1_rdd_top20/top20_output.json
src/task1_rdd_top20/index.html
```

统一页面 `/task1` 会读取：

```text
src/task1_rdd_top20/top20_output.json
```

如需重新生成结果，默认读取：

```text
/Users/elemen/Downloads/moviedata-latest
```

推荐复制 `.env.example` 为 `.env` 后修改 `MOVIE_DATA_DIR`：

```bash
cp .env.example .env
```

也可以在命令行临时指定：

```bash
MOVIE_DATA_DIR=/Users/elemen/Downloads/moviedata-latest make task1
```

任务一结果会写入：

```text
src/task1_rdd_top20/top20_output.json
```

统一 Web 页面 `/task1` 会读取同一个文件。

## 任务二：Spark SQL 男女标签偏好

目标流程：

```text
MovieLens 数据导入 MySQL
        ↓
Spark SQL 通过 JDBC 读取 MySQL
        ↓
按 gender + tag 统计关注人数
        ↓
输出男女标签偏好结果
        ↓
页面 /task2 使用 ECharts 柱状图展示
```

当前 `/task2` 已有页面和 `/api/gender-tags` 接口。正式接入时可以输出 JSON 到：

```text
outputs/task2_gender_tags.json
```

建议 JSON 结构：

```json
{
  "code": 200,
  "data": {
    "status": "real",
    "message": "Spark SQL 统计结果",
    "tags": ["Drama", "Comedy", "Action"],
    "male": [1200, 980, 760],
    "female": [1100, 860, 690]
  }
}
```

## 任务三：Spark Streaming 折线图

相关文件：

```text
src/task3_streaming/streaming_job.py
src/task3_streaming/split_data.py
src/task3_streaming/feed_data.py
app.py
templates/task3.html
sql/schema.sql
```

目标流程：

```text
src/task3_streaming/split_data.py 切分 ratings.csv
        ↓
src/task3_streaming/feed_data.py 分批投递评分文件
        ↓
src/task3_streaming/streaming_job.py 监听目录，累计统计并写入 MySQL
        ↓
/api/latest 读取 streaming_results
        ↓
/task3 折线图每 5 秒刷新
```

任务三依赖 MySQL 数据库 `movie_analysis` 和表 `streaming_results`。运行前在 `.env` 中配置 MySQL 账号、数据目录和 Streaming 工作目录，不需要改代码里的账号密码。

现在推荐在 `.env` 中配置 MySQL 和任务三目录，然后执行：

```bash
mysql -u root -p < sql/schema.sql
make task3-clean
make task3-split
make task3-stream
```

另开一个终端投递评分批次：

```bash
make task3-feed
```

每次 `make task3-stream` 启动会生成一个 `run_id` 并写入 MySQL，`/api/latest` 只读取最新一次运行的数据，避免误开旧 Streaming 进程时把旧批次混进当前页面。

页面访问：

```text
http://localhost:5001/task3
```

## 数据说明

实验数据来自 MovieLens，当前仓库的课程资料位于：

```text
docs/plans/
```

本地如已有解压数据，可以先按脚本中的路径配置运行；后续团队可再统一数据目录。

## 协作约定

- 修改仓库前先阅读 `AGENT.md` 和 `docs/specs/web-system-spec.md`。
- 文档类文件放入 `docs/`。
- Spark 计算代码优先放入 `src/` 下对应任务目录。
- Web 统一入口优先使用根目录 `app.py`。
- 三个实验任务都应保留可截图、可答辩演示的页面。
- 未经明确说明，不要重写、删除、移动或大幅重构他人负责的代码。
