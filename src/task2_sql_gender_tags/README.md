# 任务二：Spark SQL 性别标签偏好（已实现）

本目录存放任务二的 Spark SQL 分析代码和 MySQL 导入脚本。

## 文件说明

```text
task2_gender_tags.py    Spark SQL 核心脚本，读取 MySQL + Movies.csv，生成 JSON
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

## 数据导入

1. 解压 `docs/plans/moviedata-latest.rar` 到某目录
2. 在项目根目录复制 `.env.example` 为 `.env`，填入 `MOVIE_DATA_DIR` 和 MySQL 配置
3. 执行 `mysql -u root -p < sql/schema.sql` 创建表
4. 执行 `make task2-import` 或 `python import_mysql.py`

## 运行 Spark SQL

```bash
make task2
# 或
python task2_gender_tags.py
```

需要 PySpark 环境和 `mysql-connector-java-x.x.x.jar`（放在项目根目录）。

结果写入 `outputs/task2_gender_tags.json`，Web 页面 `/task2` 自动读取。

## 测试

```bash
make test2
# 或
pytest ../../tests/test_task2_gender_tags.py -v
```
