# 任务一：RDD Top20 高分电影

本目录存放任务一的 Spark RDD 分析代码和历史 Web 页面。

## 文件说明

```text
task1_top20.py       Spark RDD 分析脚本，生成 Top20 结果
top20_output.json    当前 Web 统一入口读取的结果文件
web_app.py           旧版任务一独立 Flask 服务，保留作参考
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

运行前先检查 `task1_top20.py` 中的 `BASE_DIR` 是否指向本机数据路径。

```bash
python3 src/task1_rdd_top20/task1_top20.py
```
