import importlib.util
import sys
import types
import unittest
from pathlib import Path


fake_pyspark = types.ModuleType("pyspark")
fake_pyspark.SparkContext = object
fake_pyspark.SparkConf = object
sys.modules.setdefault("pyspark", fake_pyspark)

fake_streaming = types.ModuleType("pyspark.streaming")
fake_streaming.StreamingContext = object
sys.modules.setdefault("pyspark.streaming", fake_streaming)

fake_pymysql = types.ModuleType("pymysql")
sys.modules.setdefault("pymysql", fake_pymysql)

MODULE_PATH = Path(__file__).resolve().parents[1] / "src" / "task3_streaming" / "streaming_job.py"
spec = importlib.util.spec_from_file_location("streaming_job", MODULE_PATH)
streaming_job = importlib.util.module_from_spec(spec)
spec.loader.exec_module(streaming_job)


class Task3StreamingTest(unittest.TestCase):
    def test_parse_movie_line_keeps_title_with_comma(self):
        line = '318,"Shawshank Redemption, The (1994)",Crime|Drama'

        movie_id, title = streaming_job.parse_movie_line(line)

        self.assertEqual(movie_id, 318)
        self.assertEqual(title, "Shawshank Redemption, The (1994)")

    def test_build_top_records_preserves_rating_count(self):
        stats = [(318, (4.5, 12))]
        movies = {318: "Shawshank Redemption, The (1994)"}

        records = streaming_job.build_top_records(stats, movies)

        self.assertEqual(records, [(318, "Shawshank Redemption, The (1994)", 4.5, 12)])

    def test_update_rating_state_accumulates_sum_and_count(self):
        new_values = [(4.0, 1), (5.0, 1)]
        previous_state = (9.0, 2)

        state = streaming_job.update_rating_state(new_values, previous_state)

        self.assertEqual(state, (18.0, 4))

    def test_state_to_top_stats_uses_cumulative_average_and_min_count(self):
        rows = [
            (1, (90.0, 20)),
            (2, (30.0, 6)),
            (3, (45.0, 9)),
        ]

        stats = streaming_job.state_rows_to_top_stats(rows, min_count=10, top_n=2)

        self.assertEqual(stats, [(1, (4.5, 20))])

    def test_update_cumulative_state_merges_batch_stats(self):
        state = {1: (10.0, 2)}
        batch_stats = [(1, (5.0, 1)), (2, (8.0, 2))]

        streaming_job.update_cumulative_state(batch_stats, state)

        self.assertEqual(state, {1: (15.0, 3), 2: (8.0, 2)})


if __name__ == "__main__":
    unittest.main()
