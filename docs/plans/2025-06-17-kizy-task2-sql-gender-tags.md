 # 任务二：Spark SQL 性别标签偏好 — 实施计划
 
 > 目标：实现 src/task2_sql_gender_tags 功能，通过 Spark SQL 访问 MySQL，按电影类型（genres）统计男/女关注人数，取 Top8 标签在 ECharts 双柱状图上展示。
 
 ## 一、数据流
 
 ```text
 Users.dat (::分隔)       Ratings.csv                Movies.csv
     ↓                       ↓                           ↓
 import_to_mysql.py      import_to_mysql.py          (Spark SQL 脚本直接 CSV)
     ↓                       ↓                           ↓
 MySQL.users 表          MySQL.ratings 表             Spark DataFrame
     ↓                       ↓                           ↓
     └── Spark SQL JDBC 读取 ──→ JOIN on userId/movieId
                                          ↓
                                  explode(genres) by "|"
                                          ↓
                              GROUP BY gender, genre, COUNT DISTINCT userId
                                          ↓
                              取男 Top8 + 女 Top8 → 并集
                                          ↓
                         JSON → outputs/task2_gender_tags.json
                                          ↓
                             GET /api/gender-tags 读取
                                          ↓
                             /task2 ECharts 双柱状图展示
 ```
 
 ## 二、数据库设计
 
 **users 表**（来源：Users.dat，`::` 分隔）
 
 | 字段 | 类型 | 说明 |
 |------|------|------|
 | userId | INT PRIMARY KEY | 用户 ID |
 | gender | CHAR(1) NOT NULL | 性别：M=男，F=女 |
 | age | INT | 年龄编号 |
 | occupation | INT | 职业编号 |
 | zipCode | VARCHAR(10) | 邮编 |
 
 **ratings 表**（来源：Ratings.csv）
 
 | 字段 | 类型 | 说明 |
 |------|------|------|
 | userId | INT NOT NULL | 用户 ID |
 | movieId | INT NOT NULL | 电影 ID |
 | rating | DECIMAL(2,1) NOT NULL | 评分 0.5-5.0 |
 | timestamp | INT NOT NULL | Unix 时间戳 |
 
 ## 三、文件清单
 
 **新建：** `src/task2_sql_gender_tags/task2_gender_tags.py`、`src/task2_sql_gender_tags/import_mysql.py`、`tests/test_task2_gender_tags.py`、`tests/run_all.py`、`tests/__init__.py`
 
 **修改：** `sql/schema.sql`、`templates/task2.html`、`Makefile`、`.env.example`、`src/task2_sql_gender_tags/README.md`
 
 ## 四、测试用例
 
 | 编号 | 用例 | 说明 |
 |------|------|------|
 | TC1 | test_genre_explode | genres 拆分为多个单标签 |
 | TC2 | test_top8_selection | Top8 截取 |
 | TC3 | test_union_format_output | 并集 + 补 0 |
 | TC4 | test_less_than_8_tags | 少于 8 个时不填充 |
 | TC5 | test_empty_data_returns_empty | 空数据兜底 |
 | TC6 | test_movie_with_no_genre_filtered | 脏数据过滤 |
 | TC7 | test_output_json_structure | JSON 结构校验 |
 | TC8 | test_import_users_dat_parsing | Users.dat 解析 |
