# TODO: split data if loading more than 100k of data
from metabase import Mb_Client
from dotenv import dotenv_values
import pandas as pd
import datetime
import math
import re


class SqlWorker():
    def __init__(self) -> None:
        secrets: dict = dotenv_values(".env")

        self._mb_client: Mb_Client = Mb_Client(
            url=f"{secrets['mb_url']}",
            username=secrets["username"],
            password=secrets["password"]
        )

    def get_query(self, query_name: str, params: dict = {}) -> str:
        sql_req: str = open(f"queries/{query_name}.sql").read()
        return sql_req.format(**params) if bool(params) else sql_req

    def get_payload(self, query: str) -> dict:
        payload: dict = {
            "database": 2,
            "type": "native",
            "format_rows": False,
            "pretty": False,
            "native": {
                "query": query
            }
        }
        return payload

    def convert_string_int2int(self, value: str) -> int:
        return int(value.replace(',', ''))

    def get_data_h(self) -> pd.DataFrame:
        query = self.get_query("get_data_h")
        payload = self.get_payload(query)
        query_result = self._mb_client.post("dataset/json", payload)
        df = pd.json_normalize(query_result)
        df.hour = df.hour.apply(self.convert_string_int2int)
        df.dau = df.dau.apply(self.convert_string_int2int)
        df.unified_cnt = df.unified_cnt.apply(self.convert_string_int2int)
        return df

    def get_data_d(self) -> pd.DataFrame:
        query = self.get_query("get_data_d")
        payload = self.get_payload(query)
        query_result = self._mb_client.post("dataset/json", payload)
        df = pd.json_normalize(query_result)
        df.date = df.date.apply(self.convert_string_int2int)
        df.dau = df.dau.apply(self.convert_string_int2int)
        df.unified_cnt = df.unified_cnt.apply(self.convert_string_int2int)
        return df
