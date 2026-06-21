# 任务二：免数据库运行与本机 MySQL 操作

## 默认方式：直接读取 SQL 备份

项目默认读取 `sql/db_backup.sql` 中已经计算好的 `gender_tag_stats`，不会连接数据库，也不需要 MySQL Connector/J：

```powershell
python src/task2_sql_gender_tags/task2_gender_tags.py
```

结果写入 `outputs/task2_gender_tags.json`。默认配置如下：

```dotenv
TASK2_DATA_SOURCE=sql_dump
TASK2_SQL_DUMP=sql/db_backup.sql
TASK2_TOP_N=8
```

`TASK2_SQL_DUMP` 可以使用相对项目根目录的路径或绝对路径。仓库中的实际文件名是 `db_backup.sql`。

## 可选方式：连接本机 MySQL 后重新计算

数据库配置应填写运行脚本的电脑能够访问的 MySQL。若 MySQL 就安装在你的电脑上，`MYSQL_HOST` 使用 `127.0.0.1`；若 MySQL 在虚拟机、Docker 或另一台服务器上，则填写对应地址。

### 1. 配置 `.env` 或环境变量

```dotenv
TASK2_DATA_SOURCE=mysql
MOVIE_DATA_DIR=D:/data/moviedata-latest
MYSQL_HOST=127.0.0.1
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=你的密码
MYSQL_DB=movie_analysis
MYSQL_CONNECTOR_JAR=lib/mysql-connector-j-8.4.0.jar
TASK2_TOP_N=8
```

注意：Python 脚本不会自动加载 `.env`。使用 Makefile 时会读取根目录 `.env`；直接运行 Python 时，应先在当前终端设置这些环境变量。

### 2. 导入数据库结构和原始数据

在项目根目录执行：

```powershell
cmd /c "mysql -u root -p < sql\schema.sql"
python src/task2_sql_gender_tags/import_mysql.py
```

`import_mysql.py` 会从 `MOVIE_DATA_DIR` 读取 `Users.dat` 和 `Ratings.csv`，写入本机 MySQL 的 `users`、`ratings` 表。

如果希望把整个备份（含已有统计结果）恢复到本机，可改为：

```powershell
cmd /c "mysql -u root -p < sql\db_backup.sql"
```

### 3. 运行 Spark JDBC 计算

```powershell
$env:TASK2_DATA_SOURCE = "mysql"
python src/task2_sql_gender_tags/task2_gender_tags.py
```

这条路线要求 Java、PySpark、MySQL 服务以及 Connector/J 均可用。脚本会通过 JDBC 读取 `users` 和 `ratings`，结合 `MOVIE_DATA_DIR/Movies.csv` 重新统计，并覆盖 `outputs/task2_gender_tags.json`。

### 4. 常见检查

```powershell
mysql -h 127.0.0.1 -P 3306 -u root -p -e "USE movie_analysis; SELECT COUNT(*) FROM users; SELECT COUNT(*) FROM ratings;"
```

- `Access denied`：检查用户名、密码及该用户权限。
- `Connection refused`：确认 MySQL 服务已启动，端口确实为 3306。
- `Communications link failure`：检查地址、端口、防火墙和 JDBC URL。
- `ClassNotFoundException: com.mysql.cj.jdbc.Driver`：检查 `MYSQL_CONNECTOR_JAR` 路径。
