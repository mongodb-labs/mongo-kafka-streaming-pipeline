import requests
import datetime
import argparse
from typing import List

import pandas as pd


class PrometheusQuery:
    def __init__(self, suffix: str, query: str):
        """
        :param suffix: suffix used for column name
        :param query: Promthesus query that is used for the API request 
        """
        self.query: str = query
        self.suffix: str = suffix
        self.result: dict = {}

    def execute(self, start: float, end: float) -> dict:
        """
        Execute the given query.
        :param start: Unix timestamp of the start time
        :param end: Unix timestamp of the end time
        :return: dictionary of response
        """

        r: requests.Response = requests.get(f"http://localhost:9090/api/v1/query_range?query={self.query}&start={start}&end={end}&step=5")

        self.result = r.json()

        return self.result



class PrometheusAPIWrapper:
    def __init__(self):
        cpu_query: str = "sum(irate(container_cpu_usage_seconds_total[5m]) * 100) by (name) / sum(container_spec_cpu_quota / container_spec_cpu_period) by (name)"
        memory_query: str = "sum without (dc,from,id) (container_memory_usage_bytes - container_memory_cache)"

        self.valid_names: List[str] = ['kafka-connect', 'kafka-broker', 'mongo']
        self.end: float = datetime.datetime.now().timestamp()
        self.start: float = (datetime.datetime.now() - datetime.timedelta(minutes=3)).timestamp()

        self.queries: List[PrometheusQuery] = [
            PrometheusQuery('cpu', cpu_query), 
            PrometheusQuery('memory', memory_query), 
        ]
        
        self.df_dict: dict = {}
        self.df: pd.DataFrame = None

    def get_stats(self):
        """
        For each of the queries defined the PrometheusAPIWrapper class, we execute and combine into a dictionary.
        """
        try:
            for query in self.queries:
                res: dict = query.execute(self.start, self.end)
                for service in res["data"]["result"]:
                    if service['metric']['name'] in self.valid_names:
                        column_name: str = f"{service['metric']['name']}-{query.suffix}"
                        
                        self.df_dict[column_name] = [value[1] for value in service["values"]]
                        self.df_dict['timestamp'] = [value[0] for value in service["values"]]
        except Exception as e:
            pass

    def to_df(self) -> pd.DataFrame:
        """
        Convert dictionary to DataFrame.
        :return: DataFrame of all Prometheus data
        """
        self.df = pd.DataFrame.from_dict(self.df_dict)

        return self.df

    def export_csv(self, file_name: str):
        """
        Save DataFrame into CSV
        :param filename: filename to save CSV
        """
        self.df.to_csv(file_name, index=False)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process some integers.')
    
    parser.add_argument('--throughput')
    parser.add_argument('--size')

    args = parser.parse_args()

    print(args)

    prom = PrometheusAPIWrapper()
    prom.get_stats()
    print(prom.to_df().head())

    prom.export_csv(f"test-data/throughput-{args.throughput}-size-{args.size}.csv")