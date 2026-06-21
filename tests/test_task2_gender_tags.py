#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
任务二：Spark SQL 性别标签偏好 — 单元测试

mock pyspark 模块，无需真实 Spark 环境即可测试纯 Python 逻辑。
"""

import importlib.util
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch

# ---------------------------------------------------------------------------
# Mock pyspark 模块（需在导入 task2_gender_tags 前完成）
# ---------------------------------------------------------------------------

for mod_name in [
    "pyspark",
    "pyspark.sql",
    "pyspark.sql.functions",
    "pyspark.sql.types",
    "pyspark.streaming",
]:
    if mod_name not in sys.modules:
        sys.modules[mod_name] = types.ModuleType(mod_name)

from unittest.mock import MagicMock

sys.modules["pyspark"].SparkConf = MagicMock
sys.modules["pyspark"].SparkContext = MagicMock
sys.modules["pyspark.sql"].SparkSession = MagicMock
sys.modules["pyspark.sql"].Column = MagicMock
sys.modules["pyspark.sql.functions"].col = MagicMock
sys.modules["pyspark.sql.functions"].countDistinct = MagicMock
sys.modules["pyspark.sql.functions"].desc = MagicMock
sys.modules["pyspark.sql.functions"].explode = MagicMock
sys.modules["pyspark.sql.functions"].split = MagicMock
sys.modules["pyspark.sql.functions"].trim = MagicMock
sys.modules["pyspark.streaming"].StreamingContext = MagicMock

# 如果 pymysql 未安装，mock 它
if "pymysql" not in sys.modules:
    sys.modules["pymysql"] = types.ModuleType("pymysql")
    sys.modules["pymysql"].connect = MagicMock

# ---------------------------------------------------------------------------
# 导入待测模块
# ---------------------------------------------------------------------------

TASK2_MODULE_PATH = (
    Path(__file__).resolve().parents[0]
    / ".."
    / "src"
    / "task2_sql_gender_tags"
    / "task2_gender_tags.py"
).resolve()

spec = importlib.util.spec_from_file_location("task2_gender_tags", TASK2_MODULE_PATH)
task2 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(task2)

# import_mysql.py 也 mock pymysql，不依赖外部数据库
IMPORT_MODULE_PATH = (
    Path(__file__).resolve().parents[0]
    / ".."
    / "src"
    / "task2_sql_gender_tags"
    / "import_mysql.py"
).resolve()

spec2 = importlib.util.spec_from_file_location("import_mysql", IMPORT_MODULE_PATH)
import_mysql = importlib.util.module_from_spec(spec2)
spec2.loader.exec_module(import_mysql)

# ---------------------------------------------------------------------------
# 测试用例
# ---------------------------------------------------------------------------


class TestGenreExplode(unittest.TestCase):
    """TC1: 验证 genres 按 | 拆分为正确的标签列表"""

    def test_normal_genres_split(self):
        self.assertEqual(
            task2.explode_genres("Crime|Drama|Thriller"),
            ["Crime", "Drama", "Thriller"],
        )

    def test_single_genre(self):
        self.assertEqual(task2.explode_genres("Comedy"), ["Comedy"])

    def test_empty_string(self):
        self.assertEqual(task2.explode_genres(""), [])

    def test_no_genres_listed(self):
        self.assertEqual(task2.explode_genres("(No genres listed)"), [])

    def test_whitespace_stripped(self):
        self.assertEqual(task2.explode_genres("  Action  | Drama "), ["Action", "Drama"])


class TestTopNSelection(unittest.TestCase):
    """TC2: 验证 Top8 截取逻辑"""

    def test_select_top_8_from_20(self):
        """从 20 个中取 Top 8"""
        data = [(f"Tag{i}", i) for i in range(20, 0, -1)]
        result = task2.get_top_n_tags(data, data, top_n=8)
        tags, male_vals, female_vals = result
        self.assertEqual(len(tags), 8)
        self.assertEqual(tags[0], "Tag20")
        self.assertEqual(tags[-1], "Tag13")

    def test_select_all_when_less_than_8(self):
        """TC4: 少于 8 个时不填充"""
        data = [("Action", 100), ("Drama", 80), ("Comedy", 60)]
        tags, male_vals, female_vals = task2.get_top_n_tags(data, data, top_n=8)
        self.assertEqual(len(tags), 3)
        self.assertEqual(tags, ["Action", "Drama", "Comedy"])


class TestUnionFormatOutput(unittest.TestCase):
    """TC3: 验证并集 + 补 0 对齐"""

    def test_union_and_zero_fill(self):
        male = [("Drama", 100), ("Action", 90), ("Horror", 30)]
        female = [("Drama", 80), ("Comedy", 70), ("Romance", 50)]
        tags, male_vals, female_vals = task2.get_top_n_tags(male, female, top_n=8)
        self.assertEqual(tags, ["Drama", "Action", "Horror", "Comedy", "Romance"])
        self.assertEqual(male_vals, [100, 90, 30, 0, 0])
        self.assertEqual(female_vals, [80, 0, 0, 70, 50])

    def test_both_empty(self):
        """TC5: 空数据"""
        tags, male_vals, female_vals = task2.get_top_n_tags([], [], top_n=8)
        self.assertEqual(tags, [])
        self.assertEqual(male_vals, [])
        self.assertEqual(female_vals, [])


class TestNoGenreFiltered(unittest.TestCase):
    """TC6: 脏数据过滤"""

    def test_no_genre_listed_not_in_explode(self):
        self.assertEqual(task2.explode_genres("(No genres listed)"), [])

    def test_partial_no_genre_in_string(self):
        result = task2.explode_genres("Action|(No genres listed)|Drama")
        self.assertEqual(result, ["Action", "Drama"])


class TestOutputJsonStructure(unittest.TestCase):
    """TC7: 验证 JSON 输出结构完整性"""

    def test_output_contains_required_fields(self):
        output = task2.build_output_json(
            tags=["Action", "Drama"],
            male_values=[100, 80],
            female_values=[60, 90],
        )
        self.assertIn("code", output)
        self.assertIn("data", output)
        self.assertIn("status", output["data"])
        self.assertIn("message", output["data"])
        self.assertIn("tags", output["data"])
        self.assertIn("male", output["data"])
        self.assertIn("female", output["data"])
        self.assertEqual(output["code"], 200)
        self.assertEqual(output["data"]["status"], "real")
        self.assertEqual(output["data"]["message"], "Spark SQL statistic result")


class TestSqlDumpInput(unittest.TestCase):
    '''验证无需数据库即可读取 MySQL dump 中的任务二结果。'''

    def test_parse_normalize_and_merge_duplicate_tags(self):
        import tempfile

        sql = (
            'INSERT INTO `gender_tag_stats` VALUES '
            "('F','Action\\r',12),('F','Action',20),"
            "('M','Drama',30),('M','(no genres listed)\\r',99);\n"
        )
        with tempfile.NamedTemporaryFile(
            'w', suffix='.sql', delete=False, encoding='utf-8'
        ) as handle:
            handle.write(sql)
            dump_path = handle.name
        try:
            rows = task2.read_gender_stats_from_dump(dump_path)
        finally:
            Path(dump_path).unlink()

        self.assertIn(('F', 'Action', 20), rows)
        self.assertIn(('M', 'Drama', 30), rows)
        self.assertEqual(len(rows), 2)


class TestImportUsersDatParsing(unittest.TestCase):
    """TC8: 验证 Users.dat (:: 分隔) 解析"""

    def test_parse_user_with_all_fields(self):
        line = "1::F::1::10::48067"
        user = import_mysql.parse_users_dat_line(line)
        self.assertEqual(user["userId"], 1)
        self.assertEqual(user["gender"], "F")
        self.assertEqual(user["age"], 1)
        self.assertEqual(user["occupation"], 10)
        self.assertEqual(user["zipCode"], "48067")

    def test_parse_user_male(self):
        line = "2::M::25::15::55123"
        user = import_mysql.parse_users_dat_line(line)
        self.assertEqual(user["userId"], 2)
        self.assertEqual(user["gender"], "M")
        self.assertEqual(user["age"], 25)

    def test_parse_empty_line(self):
        self.assertIsNone(import_mysql.parse_users_dat_line(""))
        self.assertIsNone(import_mysql.parse_users_dat_line("  "))

    def test_parse_invalid_line(self):
        self.assertIsNone(import_mysql.parse_users_dat_line("not_valid"))

    def test_parse_rating_csv_line(self):
        line = "1,110,4.5,987654321"
        rating = import_mysql.parse_rating_csv_line(line)
        self.assertEqual(rating["userId"], 1)
        self.assertEqual(rating["movieId"], 110)
        self.assertEqual(rating["rating"], 4.5)
        self.assertEqual(rating["timestamp"], 987654321)


if __name__ == "__main__":
    unittest.main()
