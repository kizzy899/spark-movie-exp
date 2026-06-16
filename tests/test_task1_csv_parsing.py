import importlib.util
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch


fake_pyspark = types.ModuleType("pyspark")
fake_pyspark.SparkContext = object
fake_pyspark.SparkConf = object
sys.modules.setdefault("pyspark", fake_pyspark)

MODULE_PATH = Path(__file__).resolve().parents[1] / "src" / "task1_rdd_top20" / "task1_top20.py"
spec = importlib.util.spec_from_file_location("task1_top20", MODULE_PATH)
task1_top20 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(task1_top20)


class Task1CsvParsingTest(unittest.TestCase):
    def test_parse_movie_line_keeps_title_with_comma(self):
        line = '318,"Shawshank Redemption, The (1994)",Crime|Drama'

        movie_id, title = task1_top20.parse_movie_line(line)

        self.assertEqual(movie_id, 318)
        self.assertEqual(title, "Shawshank Redemption, The (1994)")

    def test_movie_data_dir_can_be_configured_with_env_var(self):
        with patch.dict("os.environ", {"MOVIE_DATA_DIR": "/tmp/movie-data"}):
            self.assertEqual(task1_top20.get_data_dir(), "/tmp/movie-data")

    def test_output_path_matches_web_api_source_file(self):
        self.assertEqual(
            task1_top20.OUTPUT_PATH,
            str(MODULE_PATH.parent / "top20_output.json"),
        )


if __name__ == "__main__":
    unittest.main()
