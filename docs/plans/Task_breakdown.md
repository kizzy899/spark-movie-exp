 # 基于Spark的电影数据离线分析系统 —— 任务分解文档
 
 > 数据来源：[MovieLens Latest Dataset](https://grouplens.org/datasets/movielens/latest/)
 > 数据集文件：`docs/plans/moviedata-latest.rar`
 >
 > | 文件 | 内容 | 记录数 |
 > |------|------|--------|
 > | movies.csv | 电影信息(movieId, title, genres) | 45,843部电影 |
 > | ratings.csv | 评分数据(userId, movieId, rating, timestamp) | ~2600万条评分 |
 > | tags.csv | 标签数据(userId, movieId, tag, timestamp) | ~75万条标签 |
 > | users.dat | 用户信息(UserID::Gender::Age::Occupation::Zip-code) | 270,896个用户 |
 >
 > **评分标准**：5星制，半星增量（0.5-5.0）；**性别**：M=男, F=女
 > **工具栈**：Linux系统 + Hadoop(HDFS) + Spark(RDD/SQL/Streaming) + MySQL + ECharts + Web后端(语言不限)
 
 ---
 
 ## 一、目标分析
 
 ### 1.1 项目总体目标
 基于MovieLens开放电影数据集，构建一个完整的电影数据离线分析系统。系统集成了Hadoop分布式存储、Spark分布式计算、MySQL关系数据库和Web可视化技术，实现电影数据的存储、处理、分析和结果展示全流程。
 
 ### 1.2 考核要求梳理
 | 维度 | 要求 |
 |------|------|
 | 组织形式 | 小组制（4-6人），设小组长，分配任务 |
 | 提交时间 | 2026年6月25日前 |
 | 提交内容 | 课程总报告（30页以上）+ 答辩PPT（每人5分钟） |
 | 报告要求 | 技术选型 + 架构分析 + 数据库设计 + 处理过程 + 问题解决 + 总结 |
 | 技术栈 | Linux + Hadoop + Spark(RDD/SQL/Streaming) + MySQL + ECharts + Web |
 | 答辩方式 | 按学号顺序，每人5分钟PPT展示 |
 
 ### 1.3 三大功能任务概述
 | 任务 | 技术点 | 核心功能 |
 |------|--------|----------|
 | 任务一 | Spark RDD + HDFS | 电影数据RDD过滤、按平均评分排序、输出Top20高分影片、Web展示 |
 | 任务二 | Spark SQL + MySQL + ECharts | 数据导入MySQL、Spark SQL按标签统计男女关注人数、柱状图展示 |
 | 任务三 | Spark Streaming + ECharts | 动态新增评分文件、实时统计最新数据、折线图实时变化展示 |
 
 ### 1.4 小组成员分工建议
 | 角色 | 职责 |
 |------|------|
 | 组长（1人） | 项目统筹、架构设计、文档整合、答辩协调 |
 | 环境搭建（1-2人） | Linux + Hadoop + Spark + MySQL 环境部署，协助各组调试 |
 | 任务一（1人） | RDD数据处理 + 后端接口 + Web页面 |
 | 任务二（1-2人） | Spark SQL + MySQL + ECharts柱状图前后端 |
 | 任务三（1人） | Spark Streaming + 实时折线图前后端 |
 | 文档报告（全员） | 各自撰写分工部分，组长汇总整合 |
 
 ---
 
 ## 二、搭建框架
 
 ### 2.1 Linux 环境准备
 - [ ] 安装 Linux 操作系统（推荐 CentOS 7 / Ubuntu 18.04+）
 - [ ] 配置静态 IP 和主机名
 - [ ] 关闭防火墙（systemctl stop firewalld / ufw disable）
 - [ ] 配置 SSH 免密登录（若为集群则需主节点到各节点免密）
 - [ ] 安装 JDK 1.8+
 - [ ] 验证：`java -version` 正常输出
 
 ### 2.2 Hadoop 分布式环境搭建
 - [ ] 下载并解压 Hadoop（推荐 2.7.x 或 3.x）
 - [ ] 配置 `core-site.xml`（fs.defaultFS 指向 HDFS）
 - [ ] 配置 `hdfs-site.xml`（副本数、NameNode/DataNode 路径）
 - [ ] 配置 `mapred-site.xml`（YARN 模式）
 - [ ] 配置 `yarn-site.xml`
 - [ ] 配置 `hadoop-env.sh`（JAVA_HOME）
 - [ ] 格式化 HDFS：`hdfs namenode -format`
 - [ ] 启动 HDFS：`start-dfs.sh`
 - [ ] 启动 YARN：`start-yarn.sh`
 - [ ] 验证：浏览器访问 `http://<namenode>:50070` 看到 Web UI
 - [ ] 验证：`hdfs dfsadmin -report` 显示 DataNode 正常
 
 ### 2.3 Spark 环境搭建
 - [ ] 下载并解压 Spark（推荐 2.4.x 或 3.x，预编译 Hadoop 版本）
 - [ ] 配置 `spark-env.sh`（JAVA_HOME、HADOOP_CONF_DIR、SPARK_MASTER_HOST）
 - [ ] 配置 `slaves` 文件（若为集群）
 - [ ] 启动 Spark：`start-all.sh`
 - [ ] 验证：浏览器访问 `http://<master>:8080` 看到 Spark Web UI
 - [ ] 验证：运行 `spark-shell`，执行 `sc.parallelize(1 to 10).sum` 正常
 
 ### 2.4 MySQL 数据库安装与配置
 - [ ] 安装 MySQL Server（推荐 5.7+ 或 8.0）
 - [ ] 启动 MySQL 服务：`systemctl start mysqld`
 - [ ] 配置 root 密码并允许远程连接
 - [ ] 创建项目数据库：`CREATE DATABASE movie_analysis;`
 - [ ] 验证：`mysql -u root -p -e "SHOW DATABASES;"` 显示 `movie_analysis`
 
 ### 2.5 数据上传与准备
 - [ ] 从 `docs/plans/moviedata-latest.rar` 解压数据文件
 - [ ] 上传数据到 HDFS（`hdfs dfs -put` 各文件至 `/movie/data/`）
 - [ ] 导入 ratings.csv 和 users.dat 到 MySQL（创建表 + LOAD DATA）
 
 ### 2.6 Web 项目框架搭建
 - [ ] 选择 Web 后端框架（推荐 Spring Boot / Flask / Node.js Express / PHP）
 - [ ] 搭建项目目录结构，配置 ECharts CDN
 - [ ] 配置与 Spark 的调用接口
 
 ---
 
 ## 三、任务一：基于 RDD 的电影评分 Top20 分析
 
 ### 3.1 数据流
 ```
 HDFS Ratings.csv → sc.textFile → 解析(movieId, rating) → mapValues+reduceByKey
 → 计算平均分 → filter(评分人数>=10) → join movies.csv → sortBy 降序 → take(20)
 → Web 表格展示
 ```
 
 ### 3.2 实现步骤
 - [ ] **Step 1**: 用 `sc.textFile` 读取 HDFS 中 Ratings.csv，跳过表头，解析为 RDD[(movieId, rating)]
 - [ ] **Step 2**: `mapValues` + `reduceByKey` 按 movieId 聚合 (总评分, 计数)
 - [ ] **Step 3**: `mapValues` 计算平均评分，`filter` 过滤评分人数 >= 10 的影片
 - [ ] **Step 4**: `join` movies.csv 获取电影名，`sortBy` 降序排列，`take(20)` 取前20
 - [ ] **Step 5**: 结果保存到 HDFS 或 MySQL
 - [ ] **Step 6**: 后端接口读取结果返回 JSON
 - [ ] **Step 7**: 前端页面用表格展示 Top20（排名、电影名、平均评分、评分人数）
 - [ ] **Step 8**: 测试验证 + 截图
 
 ### 3.3 成果物清单
 - [ ] Spark RDD 数据处理程序
 - [ ] Web 后端接口（Top20 JSON）
 - [ ] Web 前端展示页面（表格）
 
 ---
 
 ## 四、任务二：Spark SQL 男女标签偏好柱状图
 
 ### 4.1 数据流
 ```
 MySQL(ratings+users) ← 预先导入 → Spark SQL JDBC 读取
 → JOIN ON userId → 按 gender+tag 分组统计评分人数
 → 各性别 Top10 标签 → ECharts 双柱并列柱状图
 ```
 
 ### 4.2 实现步骤
 - [ ] **Step 1**: MySQL 建表（ratings、users），导入数据
 - [ ] **Step 2**: 处理 tags 数据（如 tags.csv 稀疏则改用 movies.csv 的 genres）
 - [ ] **Step 3**: SparkSession JDBC 读取 MySQL，编写 SQL 按 gender+tag 分组
 - [ ] **Step 4**: 结果写回 MySQL 或生成 JSON
 - [ ] **Step 5**: ECharts 双柱图（男蓝女红），X 轴标签名，Y 轴关注人数
 - [ ] **Step 6**: 测试验证 + 截图
 
 ### 4.3 核心 SQL
 ```sql
 SELECT u.gender, t.tag, COUNT(DISTINCT r.userId) as user_count
 FROM ratings r JOIN users u ON r.userId = u.userId
 JOIN tags t ON r.movieId = t.movieId
 GROUP BY u.gender, t.tag
 ORDER BY u.gender, user_count DESC
 ```
 
 ### 4.4 成果物清单
 - [ ] MySQL 建表脚本 + 导入脚本
 - [ ] Spark SQL 程序
 - [ ] ECharts 柱状图页面 + 后端接口
 
 ---
 
 ## 五、任务三：Spark Streaming 实时折线图
 
 ### 5.1 数据流
 ```
 HDFS /movie/streaming-input/ 监控目录 → Spark Streaming batch=10s
 → 解析评分 → updateStateByKey 增量更新 → Top5 → MySQL/Redis
 → WebSocket/轮询 → ECharts 折线图实时更新
 ```
 
 ### 5.2 实现步骤
 - [ ] **Step 1**: 创建 HDFS 监控目录 `hdfs dfs -mkdir -p /movie/streaming-input/`
 - [ ] **Step 2**: Spark Streaming 编程（StreamingContext, textFileStream, updateStateByKey）
 - [ ] **Step 3**: 每批次计算当前 Top5 写入 MySQL
 - [ ] **Step 4**: 编写模拟数据脚本，分批放入 HDFS 监控目录
 - [ ] **Step 5**: WebSocket 或 HTTP 轮询推送数据
 - [ ] **Step 6**: ECharts 折线图（X 轴时间、Y 轴评分、多线代表 Top5 电影）
 - [ ] **Step 7**: 联调测试，逐步放入文件观察折线图变化，截图记录
 
 ### 5.3 成果物清单
 - [ ] Spark Streaming 程序
 - [ ] 模拟数据生成脚本
 - [ ] WebSocket/轮询后端 + ECharts 折线图页面
 
 ---
 
 ## 六、文档报告（基于总结报告模板）
 
 | 章节 | 内容 | 参考页数 |
 |------|------|----------|
 | 封面 | 学院、专业、班级、姓名、学号 | 1 |
 | 1. 项目原理 | Spark 分布式基本原理、核心数据抽象、设计原理 | 3-4 |
 | 2. 平台搭建 | 软硬件清单、环境搭建截图（15-20张，不可用教师PPT）+ 描述 | 5-6 |
 | 3. 总体设计 | 体系架构框图、功能分层结构图 | 3-4 |
 | 4. 数据库设计 | E-R 图、各表字段设计 | 2-3 |
 | 5. 数据处理设计 | 数据流图逐层细化（0层→1层→2层） | 4-5 |
 | 6. 程序实现 | 核心代码 + 注释 + 类图 | 6-8 |
 | 7. 问题及解决 | 3-5个典型问题（环境/RDD/Streaming/ECharts） | 2-3 |
 | 8. 结论及展示 | 全部功能截图 + 文字说明 | 3-4 |
 | 9. 总结 | 每人一段心得 + 团队总结 | 1-2 |
 
 **格式**：5号字、1.5倍行距、段首空2字符、正文>=30页
 
 ---
 
 ## 七、常见问题
 
 - Hadoop 启动失败：检查 JAVA_HOME、SSH 免密、防火墙、core-site.xml
 - Spark 连接 HDFS 失败：HADOOP_CONF_DIR 配置、版本匹配
 - RDD 数据倾斜：repartition 或 key 加盐
 - Streaming 不触发：textFileStream 需文件原子移动到监控目录
 - users.dat 分隔符 `::`，导入前需转 CSV
 - tags 稀疏时改用 movies.csv 的 genres
 - 提前 1 周完成开发，留足时间写文档；截图不可使用教师 PPT
