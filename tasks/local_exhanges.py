from prefect import Task
import requests
from bs4 import BeautifulSoup
from datetime import datetime

from exceptions.network import PageLoadingException


class LocalExchangesTask(Task):
    def __init__(self, url="https://ru.myfin.by/currency/ekaterinburg", **kwargs):
        self.url = url
        super().__init__(**kwargs)

    def _get_page(self, page):
        r = requests.get(self.url, params={"page": page})
        if r.status_code not in [200, 301]:
            raise PageLoadingException(f"Url {self.url} returned {r.status_code}.")
        if r.status_code == 301:
            return None
        return r.text

    def _parse_html(self, html):
        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table", {"id": "content_table"}).tbody.findChildren("tr")
        rates = []
        for row in table:
            try:
                currency_raw = row.findChildren("td", {"class": " curr_hid USD"})
                rates.append(
                    {
                        "exchange": (
                            row.findChild("td", {"class": "bank_name"})
                            .find("a", {"class": "bank_link"})
                            .text
                        ),
                        "currency": {
                            "sell": float(currency_raw[0].text),
                            "buy": float(currency_raw[0].text),
                        },
                        "date": datetime.strptime(
                            row.findChild("time").text, "%d.%m.%Y %H:%M"
                        ),
                    }
                )
            except AttributeError:
                pass
        return rates

    def _iterate_pages(self):
        page_number = 1
        page = self._get_page(page_number)
        while page is not None:
            yield page
            page_number += 1
            page = self._get_page(page_number)

    def run(self):
        currency_rates = []
        for page in self._iterate_pages():
            currency_rates.extend(self._parse_html(page))
        return currency_rates
