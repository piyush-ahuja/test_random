import os
import logging
import unittest
from typing import List
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

class ParquetProcessor:

    @staticmethod
    def count_zero_indexes(file_path: str) -> int:
        try:
            logging.info(f"Processing file: {file_path}")
            df = pd.read_parquet(file_path)
            count = df[df['index'] == "00000000000"].shape[0]
            logging.info(f"Count of zero indexes in file {file_path}: {count}")
            return count
        except Exception as e:
            logging.error(f"Error processing file {file_path}: {e}")
            return -1

    @staticmethod
    def process_files(files: List[str]) -> List[int]:
        counts = []
        for file_path in files:
            count = ParquetProcessor.count_zero_indexes(file_path)
            if count != -1:
                counts.append(count)
        return counts

class TestParquetProcessor(unittest.TestCase):

    def test_count_zero_indexes(self):
        test_file = "test_parquet_file.parquet"
        df = pd.DataFrame({"index": ["00000000000", "1", "2", "00000000000", "4"]})
        df.to_parquet(test_file)

        processor = ParquetProcessor()
        count = processor.count_zero_indexes(test_file)
        os.remove(test_file)
        self.assertEqual(count, 2)

    def test_process_files(self):
        test_file1 = "test_parquet_file1.parquet"
        test_file2 = "test_parquet_file2.parquet"
        df1 = pd.DataFrame({"index": ["00000000000", "1", "2", "00000000000", "4"]})
        df2 = pd.DataFrame({"index": ["00000000000", "1", "2", "3", "4"]})
        df1.to_parquet(test_file1)
        df2.to_parquet(test_file2)

        processor = ParquetProcessor()
        counts = processor.process_files([test_file1, test_file2])
        os.remove(test_file1)
        os.remove(test_file2)
        self.assertEqual(counts, [2, 1])

if __name__ == "__main__":
    unittest.main(argv=['first-arg-is-ignored'], exit=False)
