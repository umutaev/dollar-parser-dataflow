from prefect import Task
from bs4 import BeautifulSoup
import requests
from datetime import datetime

from exceptions.network import PageLoadingException


class BestChangeParseTask(Task):
    def __init__(
        self, url="https://www.bestchange.ru/qiwi-to-tether-bep20.html", **kwargs
    ):
        self.url = url
        super().__init__(**kwargs)

    def _get_page(self):
        r = requests.get(self.url)
        if not r.status_code == 200:
            raise PageLoadingException(f"Url {self.url} returned {r.status_code}.")
        return r.text

    def _parse_html(self, html):
        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table", {"id": "content_table"}).tbody.findChildren("tr")
        rates = []
        for row in table:
            try:
                currency_raw = row.findChildren("td", {"class": "bi"})
                rates.append(
                    {
                        "exchange": (
                            row.findChild("td", {"class": "bj"})
                            .find("div", {"class": "ca"})
                            .text
                        ),
                        "currency": {
                            "from": {
                                "label": currency_raw[0].find("small").text,
                                "value": float(
                                    currency_raw[0]
                                    .find("small")
                                    .previous_sibling.rstrip()
                                ),
                            },
                            "to": {
                                "label": currency_raw[1].find("small").text,
                                "value": float(
                                    currency_raw[1]
                                    .find("small")
                                    .previous_sibling.rstrip()
                                ),
                            },
                        },
                    }
                )
            except AttributeError:
                pass
        return {
            "date": datetime.utcnow(),
            "rates": rates,
        }

    def run(self):
        page_html = self._get_page()
        return self._parse_html(page_html)
