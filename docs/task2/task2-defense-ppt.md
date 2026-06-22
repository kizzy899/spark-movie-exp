# spark大数据 movie_task2 答辩演讲稿（3 分钟）

> 依据：实验二任务说明。此答辩聚焦 Spark SQL 的基础编程、DataFrame 操作与多数据源管理；不额外虚构 RDD 转换内容。
>
> 统计口径：PPT 中的“标签”指 `Movies.csv` 中的 `genres`（电影类型），不是任意自由文本标签。

## 时间分配

| 页码 | 内容 | 时间 |
|---|---|---:|
| 1 | 封面与任务介绍 | 15 秒 |
| 2 | 任务目标与统计口径 | 18 秒 |
| 3 | 数据流与实现顺序 | 22 秒 |
| 4 | 核心 SQL / DataFrame 逻辑 | 20 秒 |
| 5 | Top 8 筛选策略 | 16 秒 |
| 6 | 页面图表展示方式 | 16 秒 |
| 7 | 测试与功能验证 | 22 秒 |
| 8 | 结果展示（柱状图） | 22 秒 |
| 9 | 环境与排错 | 14 秒 |
| 10 | 结语 | 15 秒 |
| **合计** |  | **180 秒** |

## 第 1 页：封面

PPT 关键词：Spark SQL、MySQL JDBC、CSV、DataFrame、柱状图

演讲稿：

老师好，我汇报实验二的 movie_task2。本任务通过 Spark SQL 读取 MySQL 与 CSV 数据，对男女用户的电影类型覆盖人数进行统计，并将结果展示在页面柱状图中。

代码参考：[src/task2_sql_gender_tags/task2_gender_tags.py](src/task2_sql_gender_tags/task2_gender_tags.py#L182-L294)

---

## 第 2 页：任务目标与统计口径

PPT 关键词：性别、电影标签、去重用户数、Top 8

演讲稿：

我们要解决的核心问题是：不同性别用户更常关注哪些电影类型。这里的“标签”来自 `Movies.csv` 中的 `genres` 字段，表示电影类型。为了避免同一用户重复计数，我们按 `userId` 做去重，再分别统计男性和女性各自的类型覆盖人数。

代码参考：[src/task2_sql_gender_tags/task2_gender_tags.py](src/task2_sql_gender_tags/task2_gender_tags.py#L33-L38)、[src/task2_sql_gender_tags/task2_gender_tags.py](src/task2_sql_gender_tags/task2_gender_tags.py#L93-L142)

---

## 第 3 页：数据流与实现顺序

PPT 流程：MySQL `users` / `ratings` → CSV `Movies.csv` → Spark DataFrame → 聚合结果 → JSON → 页面图表

演讲稿：

整体流程可以分成几个步骤。首先通过 JDBC 读取 MySQL 中的用户表和评分表；接着读取本地 CSV 中的电影类型数据；然后用 Spark SQL 的 DataFrame 进行关联、拆分和聚合；最后把结果写入 JSON，供前端页面展示。

代码参考：[src/task2_sql_gender_tags/task2_gender_tags.py](src/task2_sql_gender_tags/task2_gender_tags.py#L145-L179)、[src/task2_sql_gender_tags/task2_gender_tags.py](src/task2_sql_gender_tags/task2_gender_tags.py#L202-L244)

---

## 第 4 页：核心 SQL / DataFrame 逻辑

PPT 关键词：`join`、`explode`、`countDistinct`、`groupBy`

演讲稿：

核心逻辑主要有三步。第一步是把 `ratings` 表和 `users` 表按 `userId` 关联；第二步把 `movieId` 与 `Movies.csv` 对应，再用 `explode` 拆分 `genres`，得到每个电影对应的类型；第三步按 `gender` 和 `genre` 分组，并用 `countDistinct(userId)` 计算每个性别在每个类型中的去重用户数。

代码参考：[src/task2_sql_gender_tags/task2_gender_tags.py](src/task2_sql_gender_tags/task2_gender_tags.py#L230-L253)

---

## 第 5 页：Top 8 筛选策略

PPT 关键词：`orderBy`、`limit(8)`、并集对齐、补 0

演讲稿：

为了让图表更清晰，我们把每个性别的结果按人数从高到低排序，再各取前 8 个标签。这里的“并集”指的是把男性前 8 个标签和女性前 8 个标签合并到一起，去掉重复的标签后得到一个统一的标签列表。比如男性有 Drama、Action，女性有 Drama、Comedy，那么最终统一标签就会是 Drama、Action、Comedy。这样两组数据就能按同一个标签顺序对齐，缺失的标签位置用 0 补上，柱状图就不会出现错位。

代码参考：[src/task2_sql_gender_tags/task2_gender_tags.py](src/task2_sql_gender_tags/task2_gender_tags.py#L41-L59)、[src/task2_sql_gender_tags/task2_gender_tags.py](src/task2_sql_gender_tags/task2_gender_tags.py#L255-L276)

---

## 第 6 页：页面图表展示方式

PPT 映射：`tags` → x 轴、`male` / `female` → 两组柱状图

演讲稿：

页面端直接读取 JSON。JSON 里的 `tags`、`male`、`female` 数组按位置对应，前端就可以把它们分别映射到柱状图的类别轴和两组数值数据。这样页面只需要负责展示，不需要重新做复杂统计。

代码参考：[src/task2_sql_gender_tags/task2_gender_tags.py](src/task2_sql_gender_tags/task2_gender_tags.py#L62-L73)、[src/task2_sql_gender_tags/task2_gender_tags.py](src/task2_sql_gender_tags/task2_gender_tags.py#L278-L280)

---

## 第 7 页：测试与功能验证

PPT 流程：数据导入 → 统计脚本 → JSON 输出 → API / 页面 → 图表展示

演讲稿：

从功能验证来看，流程是先把原始数据导入到数据库，再运行统计脚本；脚本输出 JSON 后，接口和页面能够正常读取；最后图表能够正确显示。项目中也包含了针对拆分逻辑、Top 8 选择、JSON 结构等的测试，用来验证关键功能是否稳定。

代码参考：[src/task2_sql_gender_tags/task2_gender_tags.py](src/task2_sql_gender_tags/task2_gender_tags.py#L182-L294)

---

## 第 8 页：结果展示（柱状图）

PPT 结果：男女各自关注人数最多的类型中，Drama 都处于前列；男性为 4139，女性为 1631。

演讲稿：

从最终结果看，男女组内关注人数最多的电影类型都以 Drama 为主，男性对应 4139，女性对应 1631。这里的数值表示的是某性别用户对该类型的覆盖人数，反映的是覆盖范围，而不是直接等同于偏好强度。

代码参考：[src/task2_sql_gender_tags/task2_gender_tags.py](src/task2_sql_gender_tags/task2_gender_tags.py#L255-L276)

---

## 第 9 页：环境与排错

PPT 顺序：MySQL 连接 → 驱动配置 → 数据路径 → JSON 输出 → 页面展示

演讲稿：

实验过程中，排错一般按数据库连接、驱动配置、数据文件路径、JSON 输出和页面展示的顺序来检查。这样可以更快定位问题是发生在数据读取、统计计算，还是前端展示环节。

代码参考：[src/task2_sql_gender_tags/task2_gender_tags.py](src/task2_sql_gender_tags/task2_gender_tags.py#L145-L179)、[src/task2_sql_gender_tags/task2_gender_tags.py](src/task2_sql_gender_tags/task2_gender_tags.py#L205-L218)

---

## 第 10 页：结语

PPT 要点：完成任务、掌握方法、说明边界

演讲稿：

以上是 movie_task2 的整体过程与结果。我通过 Spark SQL 完成了对 MySQL 和 CSV 的读取、关联、聚合和可视化展示；同时也明确了结果的统计边界与解释方式。汇报完毕，请老师指正。

代码参考：[src/task2_sql_gender_tags/task2_gender_tags.py](src/task2_sql_gender_tags/task2_gender_tags.py#L182-L294)

---

## 答辩用词边界

- 默认运行模式可表述为“读取 SQL 备份结果”；只有在实际设置了数据源为 MySQL 的情况下，才说明“通过 Spark JDBC 重算”。
- 这里的标签统计口径是 `genres`，不是额外新建的标签表。
- 男女样本规模不同，因此覆盖人数不能直接等同于偏好强度；若要比较偏好强弱，需进一步结合占比分析。