# 任务一：RDD Top20 高分电影

本目录存放任务一的 Spark RDD 分析代码和历史 Web 页面。

## 文件说明

```text
task1_top20.py       Spark RDD 分析脚本，生成 Top20 结果
top20_output.json    当前 Web 统一入口读取的结果文件
index.html           旧版任务一独立页面，保留作参考
```

## 当前推荐入口

统一 Web 页面从根目录启动：

```bash
python3 app.py
```

访问：

```text
http://localhost:5001/task1
```

## 重新生成结果

默认读取数据目录：

```text
/Users/elemen/Downloads/moviedata-latest
```

推荐在项目根目录复制环境变量文件：

```bash
cp .env.example .env
```

然后修改 `.env` 中的 `MOVIE_DATA_DIR`，再运行：

```bash
make task1
```

结果会写入本目录的 `top20_output.json`，统一 Web 页面 `/task1` 会读取该文件。
