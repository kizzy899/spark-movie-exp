# outputs

这里存放 Spark 任务生成、供 Web 页面读取的结果文件。

当前约定：

```text
task2_gender_tags.json
```

任务二接入 Spark SQL 后，可以将男女标签偏好统计结果写入该文件，统一 Web 服务的 `/api/gender-tags` 会优先读取它。

示例结构：

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
