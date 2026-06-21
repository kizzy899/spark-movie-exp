# 任务二：Spark SQL 性别标签偏好（已实现）

本目录存放任务二的 SQL 备份读取脚本、Spark SQL 分析代码和 MySQL 导入脚本。

## 文件说明

```text
task2_gender_tags.py    默认读取 SQL 备份，也可读取 MySQL + Movies.csv
import_mysql.py         数据导入脚本，将 Users.dat + Ratings.csv 导入 MySQL
README.md               本文件（说明文档）
```

## Web 接入方式

从项目根目录启动统一 Web 服务：

```text
http://localhost:5001/
http://localhost:5001/task2
```

API：

```text
GET /api/gender-tags   → 返回性别标签偏好 JSON
```

## 默认运行（无需 MySQL）

```bash
make task2
# 或从项目根目录执行
python src/task2_sql_gender_tags/task2_gender_tags.py
```

默认读取 `sql/db_backup.sql` 中的 `gender_tag_stats`，结果写入 `outputs/task2_gender_tags.json`。

## MySQL + Spark JDBC 模式（可选）

1. 解压 `docs/plans/moviedata-latest.rar` 到某目录
2. 在项目根目录复制 `.env.example` 为 `.env`，填入 `MOVIE_DATA_DIR` 和 MySQL 配置
3. 执行 `mysql -u root -p < sql/schema.sql` 创建表
4. 执行 `make task2-import` 或 `python import_mysql.py`

数据导入完成后，将 `TASK2_DATA_SOURCE` 设为 `mysql`，再运行：

```bash
$env:TASK2_DATA_SOURCE = "mysql"  # PowerShell
python src/task2_sql_gender_tags/task2_gender_tags.py
```

默认使用项目内的 `lib/mysql-connector-j-8.4.0.jar`；如需更换版本，可通过 `MYSQL_CONNECTOR_JAR` 指定其他路径。

数据库路线的完整配置和排错步骤见 `docs/task2-mysql-guide.md`。Web 页面 `/task2` 会自动读取生成的结果。

## 测试

```bash
make test2
# 或
pytest ../../tests/test_task2_gender_tags.py -v
```
