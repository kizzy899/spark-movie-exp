# spark-movie-exp

基于 MovieLens 开放数据集构建的电影数据离线分析系统。集成 Hadoop 分布式存储、Spark 分布式计算（RDD / SQL / Streaming）、MySQL 数据库与 ECharts 可视化，实现电影数据的存储、处理、分析和结果展示全流程。

---

## 项目结构

`	ext
spark-movie-exp/
├── plans/                    # 项目规划文档
│   ├── 0-项目概述与目标.md          # 顶层规划：目标、技术栈、分工、交付物
│   ├── 1-任务一：基于RDD的电影评价Top20分析.md    # 任务一详细设计
│   ├── 2-任务二：Spark SQL男女标签偏好柱状图.md   # 任务二详细设计
│   ├── 3-任务三：Spark Streaming实时折线图.md     # 任务三详细设计
│   └── Task_breakdown_旧版.md      # 原始任务分解文档（归档）
├── docs/
│   └── plans/
│       ├── moviedata-latest.rar      # MovieLens 原始数据集
│       ├── Spark考核目标.docx         # 课程考核说明
│       ├── 附件1-基于Spark的电影数据离线分析系统.docx  # 功能要求附件
│       └── 总结报告模板.docx           # 课程报告模板
├── tmp-extract/              # 临时解压目录
├── .gitignore
└── README.md
`

---

## 三大功能任务

| 任务       | 技术栈                      | 核心功能                                                     |
| ---------- | --------------------------- | ------------------------------------------------------------ |
| **任务一** | Spark RDD + HDFS            | 读取评分数据 -> RDD过滤 -> 按平均评分排序 -> Top20 影片 -> Web表格展示 |
| **任务二** | Spark SQL + MySQL + ECharts | 数据导入MySQL -> Spark SQL按标签统计男女关注 -> 双柱并列柱状图展示 |
| **任务三** | Spark Streaming + ECharts   | 监控HDFS目录 -> Spark Streaming增量更新 -> 实时Top5 -> 折线图动态更新 |

### 任务一 — 基于 RDD 的电影评价 Top20 分析

- 使用 sc.textFile 读取 HDFS 中的 ratings.csv
- educeByKey 按 movieId 聚合，计算平均评分
- 过滤评分人数 >= 10 的电影
- join movies.csv 获取电影名称，sortBy 降序，	ake(20)
- Web 后端提供 JSON API，前端表格展示（排名、电影名、平均分、评分人数）
- 详细设计见 [plans/1-任务一：基于RDD的电影评价Top20分析.md](plans/1-任务一：基于RDD的电影评价Top20分析.md)

### 任务二 — Spark SQL 男女标签偏好柱状图

- 将 ratings、users、tags 数据导入 MySQL
- Spark SQL 通过 JDBC 读取 MySQL，按 gender + tag 分组统计
- 分别输出男/女关注人数最多的 Top10 标签
- ECharts 双柱并列柱状图展示（男蓝女红）
- 详细设计见 [plans/2-任务二：Spark SQL男女标签偏好柱状图.md](plans/2-任务二：Spark SQL男女标签偏好柱状图.md)

### 任务三 — Spark Streaming 实时折线图

- Spark Streaming 	extFileStream 监控 HDFS 目录（batch = 10s）
- updateStateByKey 增量更新各电影评分状态
- 实时计算 Top5 写入 MySQL
- 模拟脚本分批投放评分文件
- ECharts 折线图实时显示 Top5 评分随时间变化
- 详细设计见 [plans/3-任务三：Spark Streaming实时折线图.md](plans/3-任务三：Spark Streaming实时折线图.md)

---

## 技术栈

| 层次       | 技术                                          |
| ---------- | --------------------------------------------- |
| 操作系统   | Linux（CentOS 7 / Ubuntu 18.04+）             |
| 分布式存储 | Hadoop HDFS                                   |
| 分布式计算 | Spark (RDD / SQL / Streaming)                 |
| 关系数据库 | MySQL 5.7+ / 8.0                              |
| 前端可视化 | ECharts                                       |
| Web 后端   | JavaEE / Spring Boot / Flask / Node.js 等不限 |

---

## 小组分工（6人）

| 角色             | 职责                                                       |
| ---------------- | ---------------------------------------------------------- |
| **组长**         | 项目统筹、架构设计、文档整合、答辩协调、整体测试联调       |
| **任务一负责人** | RDD数据处理程序、后端接口、Top20 Web页面展示               |
| **任务二负责人** | Spark SQL分析程序、MySQL表设计与数据导入、ECharts柱状图    |
| **任务三负责人** | Spark Streaming程序、模拟数据脚本、实时折线图              |
| **Web全栈**      | Web后端框架搭建、前端整体架构、ECharts可视化集成、接口联调 |
| **数据库与文档** | MySQL数据库设计（E-R图）、数据导入脚本、课程报告撰写与排版 |

---

## 快速开始（环境搭建概览）

1. **Linux 环境**：安装操作系统，配置 IP、SSH、JDK 1.8+
2. **Hadoop 集群**：配置 HDFS，启动 NameNode / DataNode，上传数据
3. **Spark 部署**：配置 spark-env.sh，启动 Spark 集群
4. **MySQL**：安装建库，导入 ratings、users、tags 数据
5. **Web 框架**：选择后端语言/框架，搭建项目目录，集成 ECharts CDN

各环境的详细搭建步骤和验证方法见 [0-项目概述与目标.md](plans/0-项目概述与目标.md) 第 2 章。

---

## 交付物

- **课程总报告**（30页以上）：技术选型 + 架构框图 + 数据库设计 + 各任务实现 + 问题及解决 + 总结
- **答辩PPT**：每位组员 5 分钟展示
- **源代码**：所有 Spark 程序（RDD/SQL/Streaming）、Web 后端、前端页面
- **数据与截图**：处理结果 + 各任务运行截图
- **截止时间**：2026年6月25日

---

## 注意事项

- 截图不可使用教师 PPT 中的图片，必须为实际运行截图
- users.dat 分隔符为 ::，导入 MySQL 前需转换为 CSV
- Streaming 需文件原子移动到监控目录（put / mv），不支持原地追加
- 提前 1 周完成开发，留足时间撰写课程报告
- 详情参见各子文档中的「常见问题」章节
