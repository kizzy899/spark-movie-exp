VENV_PYTHON ?= python3.11
PYTHON ?= $(shell if [ -x .venv/bin/python ]; then echo .venv/bin/python; else echo python3; fi)
PIP ?= $(shell if [ -x .venv/bin/pip ]; then echo .venv/bin/pip; else echo pip3; fi)
PYSPARK_PYTHON ?= $(PYTHON)
PYSPARK_DRIVER_PYTHON ?= $(PYTHON)
export PYSPARK_PYTHON
export PYSPARK_DRIVER_PYTHON

ifneq (,$(wildcard .env))
include .env
export
endif

.PHONY: help setup web check routes task1 task2-import task2 task2-clean test2 test-all task3-clean task3-split task3-stream task3-feed

help:
	@printf "常用命令：\n"
	@printf "  make setup         创建虚拟环境并安装依赖\n"
	@printf "  cp .env.example .env 后填写本机路径\n"
	@printf "  make web           启动统一 Web 服务 http://localhost:5001/\n"
	@printf "  make check         编译检查主要 Python 文件\n"
	@printf "  make routes        打印 Flask 路由，需要先安装依赖\n"
	@printf "  make task1         重新生成任务一 Top20 结果，可用 MOVIE_DATA_DIR 指定数据目录\n"
	@printf "  make task2-import  导入 Users.dat 和 Ratings.csv 到 MySQL\n"
	@printf "  make task2         从 SQL 备份生成性别标签 JSON（默认无需 MySQL）\n"
	@printf "  make task2-clean   清空 MySQL users 和 ratings 表\n"
	@printf "  make test2         仅运行任务二单元测试\n"
	@printf "  make test-all      运行所有单元测试\n"
	@printf "  make task3-clean   清理任务三本地运行目录\n"
	@printf "  make task3-split   切分任务三评分批次文件\n"
	@printf "  make task3-stream  启动任务三 Spark Streaming\n"
	@printf "  make task3-feed    投递任务三评分批次文件\n"

setup:
	$(VENV_PYTHON) -m venv .venv
	.venv/bin/pip install -r requirements.txt

web:
	$(PYTHON) app.py

check:
	$(PYTHON) -m py_compile app.py \
		src/task1_rdd_top20/task1_top20.py \
		src/task2_sql_gender_tags/import_mysql.py \
		src/task2_sql_gender_tags/task2_gender_tags.py \
		src/task3_streaming/split_data.py \
		src/task3_streaming/feed_data.py \
		src/task3_streaming/streaming_job.py

routes:
	$(PYTHON) -c 'from app import app; print("\n".join(sorted(str(rule) for rule in app.url_map.iter_rules())))'

task1:
	$(PYTHON) src/task1_rdd_top20/task1_top20.py

task2-import:
	$(PYTHON) src/task2_sql_gender_tags/import_mysql.py

task2:
	$(PYTHON) src/task2_sql_gender_tags/task2_gender_tags.py

task2-clean:
	$(PYTHON) -c "import pymysql; c=pymysql.connect(host='$(MYSQL_HOST)',port=$(MYSQL_PORT),user='$(MYSQL_USER)',password='$(MYSQL_PASSWORD)',database='$(MYSQL_DB)'); cur=c.cursor(); cur.execute('TRUNCATE TABLE users'); cur.execute('TRUNCATE TABLE ratings'); c.commit(); c.close(); print('已清空 users 和 ratings 表')"

test2:
	$(PYTHON) -m unittest tests.test_task2_gender_tags -v

test-all:
	$(PYTHON) -m unittest discover -s tests -v

task3-clean:
	test -n "$(MOVIE_STREAMING_DIR)"
	rm -rf "$(MOVIE_STREAMING_DIR)"

task3-split:
	$(PYTHON) src/task3_streaming/split_data.py

task3-stream:
	$(PYTHON) src/task3_streaming/streaming_job.py

task3-feed:
	$(PYTHON) src/task3_streaming/feed_data.py
