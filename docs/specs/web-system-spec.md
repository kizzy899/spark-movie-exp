# 电影数据分析 Web 系统 SPEC

## 1. 背景

本项目是基于 MovieLens 数据集的 Spark 电影数据分析实验。实验要求使用 Linux、Hadoop、Spark、MySQL、ECharts 和 Web 开发技术，将数据存储、分布式计算、数据库访问和页面展示整合成一个可演示的完整系统。

本 SPEC 聚焦 Web 展示系统与 Spark 任务之间的集成方式，目标是让组员明确页面结构、接口约定、数据流和后续开发边界。

## 2. 目标

系统需要提供一个统一 Web 入口，并展示三个实验任务的分析结果：

1. 任务一：基于 Spark RDD 计算电影平均评分，输出前 20 个高分影片，并在页面展示表格。
2. 任务二：将数据导入 MySQL，通过 Spark SQL 统计男、女关注人数最多的电影标签，并在页面展示柱状图。
3. 任务三：基于 Spark Streaming 动态增加评分文件，统计最新评分结果，并在页面展示实时变化折线图。

Web 系统应满足课程答辩和截图演示需要，优先保证结构清晰、页面可访问、数据链路可说明。

## 3. 非目标

本阶段不做复杂前后端分离，不引入 Vue、React 或 Next.js。

本阶段不强制统一所有数据路径，也不把大数据文件搬入仓库。

本阶段不实现用户登录、权限管理、后台管理系统、在线上传数据等功能。

本阶段不允许为了风格统一而重写他人已经完成的模块。已有任务代码应以保留、接入、修复为主，避免无必要的整体替换。

## 4. 技术选型

| 模块 | 技术 |
| --- | --- |
| Web 后端 | Flask |
| 页面 | HTML、CSS、JavaScript |
| 图表 | ECharts |
| 批处理计算 | Spark RDD |
| SQL 分析 | Spark SQL |
| 实时计算 | Spark Streaming |
| 数据库存储 | MySQL |
| 分布式存储 | HDFS 或本地文件存储 |
| 运行环境 | Linux 优先 |

选择 Flask 的原因是项目页面和接口规模较小，Flask 上手快、代码量少，适合课程实验集成。ECharts 用于柱状图和折线图展示，能满足可视化截图和答辩演示需求。

## 5. 页面结构

统一 Web 入口为根目录 `app.py`。页面由 Flask 渲染 `templates/` 下的 HTML 文件。

```text
/        首页
/task1   任务一：Top20 高分电影表格
/task2   任务二：男女标签偏好柱状图
/task3   任务三：实时评分折线图
```

页面文件：

```text
templates/index.html   首页和三个任务入口
templates/task1.html   Top20 表格页面
templates/task2.html   男女标签柱状图页面
templates/task3.html   实时评分折线图页面
```

首页只负责展示项目概览和入口，不承担数据分析逻辑。

## 6. 接口约定

### 6.1 任务一接口

```text
GET /api/top20
```

数据来源：

```text
src/task1_rdd_top20/top20_output.json
```

响应结构：

```json
{
  "code": 200,
  "data": {
    "task": "movie_top20",
    "description": "平均评分最高的前20部电影",
    "total_count": 20,
    "top20": [
      {
        "rank": 1,
        "movieId": 632,
        "title": "Land and Freedom (Tierra y libertad) (1995)",
        "avgRating": 4.64,
        "ratingCount": 14
      }
    ]
  }
}
```

页面行为：

- `/task1` 页面请求 `/api/top20`。
- 成功后渲染排名、电影名称、平均评分、评分人数。
- 如果结果文件不存在，页面展示错误提示。

### 6.2 任务二接口

```text
GET /api/gender-tags
```

正式数据来源：

```text
outputs/task2_gender_tags.json
```

如果该文件不存在，接口返回示例数据，用于页面联调。

正式响应结构：

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

页面行为：

- `/task2` 页面请求 `/api/gender-tags`。
- 使用 ECharts 渲染双柱状图。
- X 轴为电影标签。
- Y 轴为关注人数。
- 两组柱分别代表男性和女性。

### 6.3 任务三接口

```text
GET /api/latest
```

数据来源：

```text
MySQL movie_analysis.streaming_results
```

响应结构：

```json
{
  "times": ["2026-06-16 20:00:00", "2026-06-16 20:00:10"],
  "movies": {
    "Movie A": [4.2, 4.3],
    "Movie B": [3.9, 4.1]
  }
}
```

页面行为：

- `/task3` 页面每 5 秒请求 `/api/latest`。
- 如果暂无数据，显示等待提示。
- 如果 MySQL 或数据表不可用，显示错误提示。
- 有数据后使用 ECharts 渲染多折线图。

## 7. 数据处理链路

### 7.1 任务一链路

```text
Movies.csv / Ratings.csv
        ↓
Spark RDD 读取和解析
        ↓
按 movieId 聚合评分总和和评分人数
        ↓
计算平均评分并过滤低评分人数电影
        ↓
按平均评分排序取 Top20
        ↓
输出 top20_output.json
        ↓
Web 页面表格展示
```

当前实现目录：

```text
src/task1_rdd_top20/
```

### 7.2 任务二链路

```text
MovieLens 数据导入 MySQL
        ↓
Spark SQL 通过 JDBC 读取 MySQL
        ↓
关联用户、评分、标签或电影类型数据
        ↓
按 gender + tag 分组统计关注人数
        ↓
输出 task2_gender_tags.json
        ↓
Web 页面柱状图展示
```

当前预留目录：

```text
src/task2_sql_gender_tags/
```

### 7.3 任务三链路

```text
ratings.csv
        ↓
split_data.py 切分评分批次
        ↓
feed_data.py 模拟动态投递文件
        ↓
streaming_job.py 使用 Spark Streaming 监听目录
        ↓
统计当前批次 Top 电影评分
        ↓
写入 MySQL streaming_results
        ↓
/task3 页面定时刷新折线图
```

当前实现目录：

```text
src/task3_streaming/
```

## 8. 仓库结构约定

```text
.
├── app.py
├── templates/
├── src/
│   ├── task1_rdd_top20/
│   ├── task2_sql_gender_tags/
│   └── task3_streaming/
├── outputs/
├── docs/
│   ├── plans/
│   └── specs/
├── requirements.txt
└── README.md
```

约定：

- 根目录只放项目级入口和说明文件。
- 三个任务的 Spark 脚本分别放入 `src/` 下对应目录。
- Web 页面统一放入 `templates/`。
- Spark 生成、供 Web 读取的结果文件放入 `outputs/`。
- 文档放入 `docs/`。

## 9. 协作和变更边界

### 9.1 Agent 必读要求

任何 Agent、AI 助手或组员在修改本仓库前，必须先阅读以下文件：

```text
docs/specs/web-system-spec.md
README.md
AGENT.md
```

阅读目的：

- 明确当前系统采用 Flask + templates 的轻量 Web 方案。
- 明确页面、接口和目录结构的约定。
- 避免重复造轮子或误删他人代码。
- 确认当前任务二仍是待接入状态，任务一和任务三已有基础实现。

### 9.2 不得随意修改他人代码

默认规则：

- 不得删除、重写、移动他人负责的代码文件。
- 不得为了个人代码风格改动与当前任务无关的模块。
- 不得在未说明原因的情况下修改已有接口返回结构。
- 不得破坏已存在的页面路由和 API 路由。
- 不得把实验数据、临时文件、缓存文件混入代码目录。

允许修改的情况：

- 当前任务明确要求修改该文件。
- 该文件存在会阻塞当前功能的 bug。
- 修改是为了接入本 SPEC 已约定的接口或目录结构。
- 修改前已经说明影响范围，修改后完成了基本验证。

### 9.3 修改前检查清单

修改代码前，先确认：

1. 当前要改的是哪个实验任务。
2. 该文件是否属于自己负责的范围。
3. 是否存在更小的增量改法。
4. 是否会影响 `/`、`/task1`、`/task2`、`/task3` 页面。
5. 是否会影响 `/api/top20`、`/api/gender-tags`、`/api/latest` 接口。
6. 修改后准备用什么命令验证。

### 9.4 修改后验证清单

每次改动后至少进行以下检查：

```bash
python3 -m py_compile app.py
```

如果改动 Spark 或辅助脚本，还应编译对应脚本，例如：

```bash
python3 -m py_compile src/task1_rdd_top20/task1_top20.py
python3 -m py_compile src/task3_streaming/streaming_job.py
```

如果本机已安装依赖，应启动 Web 服务检查页面：

```bash
python3 app.py
```

并访问：

```text
http://localhost:5001/
http://localhost:5001/task1
http://localhost:5001/task2
http://localhost:5001/task3
```

## 10. 运行方式

安装依赖：

```bash
python3 -m venv .venv
source .venv/bin/activate
pip3 install -r requirements.txt
```

启动 Web：

```bash
python3 app.py
```

访问：

```text
http://localhost:5001/
```

## 11. 后续开发计划

优先级从高到低：

1. 完成任务二 Spark SQL 程序，生成 `outputs/task2_gender_tags.json`。
2. 补充 MySQL 建表脚本，至少包含任务三 `streaming_results` 表。
3. 修复任务一 CSV 解析问题，避免带逗号的电影名被截断。
4. 统一配置项，将 MySQL 密码、数据路径从代码中抽到配置文件或环境变量。
5. 使用真实环境截图补充课程报告。

## 12. 风险和注意事项

- 任务二当前仍是示例数据，不能作为最终实验结果。
- 任务三依赖 MySQL 和 Spark Streaming，单独启动 Web 不会自动产生实时数据。
- 任务一当前已有 JSON 结果，但重新生成时需要确认本机数据路径。
- 大型 MovieLens 数据文件是否提交到 Git 需要团队统一决定。
- 答辩截图应来自实际运行页面，不能只使用文档或静态示意图。
