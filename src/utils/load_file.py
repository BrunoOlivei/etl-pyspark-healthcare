# %%
import os
from typing import Optional

import chardet
import pandas as pd
from pyspark.sql import DataFrame, SparkSession

spark = SparkSession.builder.appName("HealthcareApp").getOrCreate()


# %%
class ReadFile:
    def __init__(
        self,
        path: str,
        file_name: str,
        table_name: Optional[str] = None,
        sep: Optional[str] = None,
    ) -> None:
        self.path = path
        self.file_name = file_name
        self.table_name = table_name
        self.sep = sep

    def _format_path(self) -> str:
        if self.path.endswith("/"):
            return self.path
        else:
            return f"{self.path}/"

    def _validate_directory(self, path: str) -> bool:
        if not os.path.isdir(path):
            raise FileNotFoundError(f"Directory not found: {path}")
        else:
            return True

    def _format_file_name(self) -> Optional[str]:
        try:
            if "." in self.file_name and not self.file_name.endswith("."):
                return self.file_name.split(".")[0]
        except Exception as e:
            raise Exception(f"Error on format file name: {self.file_name}: {e}")

    def _find_file(self, path: str, file_name: str) -> bool:
        try:
            files = os.listdir(path)
            for f in files:
                if f.split(".")[0] == file_name:
                    return True
            return False
        except Exception as e:
            raise Exception(f"Error listing files on {path}: {e}")

    def _get_file_extension(self, path: str) -> str:
        if "." in self.file_name and not self.file_name.endswith("."):
            return os.path.splitext(self.file_name)[1].lstrip(".")

        base = self.file_name
        with os.scandir(path) as it:
            for entry in it:
                if not entry.is_file():
                    continue
                name = entry.name
                root, ext = os.path.splitext(name)
                if root == base and ext:
                    return ext.lstrip(".")

        raise FileNotFoundError(
            f"File with base name '{self.file_name}' not found in {path}"
        )

    def _detect_encoding(self, full_path: str, sample_size: int = 10000) -> str:
        try:
            with open(f"{full_path}", "rb") as f:
                raw_data = f.read(sample_size)
            result = chardet.detect(raw_data)
            if result["encoding"] == "UTF-8-SIG":
                return "utf-8"
            else:
                return result["encoding"] or "utf-8"
        except Exception as e:
            print(f"Error detecting encoding. Assuming UTF-8 as default: {e}")
            return "utf-8"

    def _load_excel(self, full_path: str) -> DataFrame:
        try:
            df = pd.read_excel(  # type: ignore
                full_path, dtype=str, sheet_name=self.table_name, engine="openpyxl"
            )
            return spark.createDataFrame(df)  # type: ignore
        except Exception as e:
            raise Exception(
                f"Error on load excel table {self.table_name} from file {self.file_name} on directory {self.path}: {e}"
            )
        
    def _load_csv(self, full_path: str, encoding: str) -> DataFrame:
        try:
            df = (
                spark.read.format("csv")
                .option("header", "true")
                .option("inferSchema", "false")
                .option("sep", self.sep)
                .option("encoding", encoding)
                .load(full_path)
            )
            return df
        except Exception as e:
            raise Exception(f"Error loading data from file {self.file_name} on directory {self.path}: {e}")

    # TODO implement a method to read a parquet file using pyspark

    def _get_full_path(self) -> str:
        try:
            path = self._format_path()
            self._validate_directory(path)
            extension = self._get_file_extension(path)
            file_name = self._format_file_name()
            return f"{path}{file_name}.{extension}"
        except Exception as e:
            raise Exception(
                f"Error on get the full path of {self.path} and {self.file_name}: {e}"
            )

    def load_data(self) -> Optional[DataFrame]:
        path = self._format_path()
        self._validate_directory(path)
        extension = self._get_file_extension(path)
        file_name = self._format_file_name()
        full_path = f"{path}{file_name}.{extension}"
        
        if extension == ".xlsx":
            df = self._load_excel(full_path)
            print(f"File {self.file_name} sucessfully loaded")
            return df
        elif extension in [".csv", ".txt"]:
            encoding = self._detect_encoding(full_path)
            df = self._load_csv(full_path, encoding)
            print(f"File {self.file_name} sucessfully loaded")
        else:
            raise ValueError(
                f"Error, extension {extension} not supported"
            )



