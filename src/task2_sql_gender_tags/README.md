# 任务二：Spark SQL 男女标签偏好

本目录预留给任务二实现代码。

## 实验目标

```text
MovieLens 数据导入 MySQL
        ↓
Spark SQL 通过 JDBC 访问 MySQL
        ↓
按电影标签统计男、女关注人数
        ↓
输出男女关注人数最多的电影标签
        ↓
Web 页面使用 ECharts 柱状图展示
```

## Web 接入方式

统一 Web 页面已经提供：

```text
http://localhost:5001/task2
GET /api/gender-tags
```

任务二 Spark SQL 程序完成后，建议输出：

```text
outputs/task2_gender_tags.json
```

JSON 结构：

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
