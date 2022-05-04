from prefect import Task
import requests
from datetime import datetime

from exceptions.network import PageLoadingException


class MOEXApiTask(Task):
    def __init__(
        self,
        api_url="http://iss.moex.com",
        engine="currency",
        market="selt",
        board="CETS",
        security="USD000UTSTOM",
        **kwargs,
    ):
        self.url = f"{api_url}/iss/history/engines/{engine}/markets/{market}/boards/{board}/securities/{security}"
        super().__init__(**kwargs)

    def _get_page(self, from_date):
        r = requests.get(
            self.url + ".json", params={"from": from_date.strftime("%Y-%m-%d")}
        )
        if not r.status_code == 200:
            raise PageLoadingException(f"Url {self.url} returned {r.status_code}.")
        return r.json()

    def _parse_page(self, json):
        indexes = {
            "date": json["history"]["columns"].index("TRADEDATE"),
            "shortname": json["history"]["columns"].index("SHORTNAME"),
            "open": json["history"]["columns"].index("OPEN"),
            "low": json["history"]["columns"].index("LOW"),
            "high": json["history"]["columns"].index("HIGH"),
            "close": json["history"]["columns"].index("CLOSE"),
        }
        rate = {
            key: json["history"]["data"][0][value] for key, value in indexes.items()
        }
        rate["date"] = datetime.strptime(rate["date"], "%Y-%m-%d")
        return rate

    def _get_last_date(self):
        r = requests.get(self.url + "/dates.json")
        json = r.json()
        desired_index = json["dates"]["columns"].index("till")
        return datetime.strptime(json["dates"]["data"][0][desired_index], "%Y-%m-%d")

    def run(self):
        last_date = self._get_last_date()
        page_json = self._get_page(last_date)
        return self._parse_page(page_json)
