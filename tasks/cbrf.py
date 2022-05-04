from prefect import Task
import requests
from datetime import datetime

from exceptions.network import PageLoadingException


class CBRFApiTask(Task):
    def __init__(
        self,
        url="http://www.cbr.ru/scripts/XML_daily.asp",
        **kwargs,
    ):
        self.url = url
        super().__init__(**kwargs)

    def _get_page(self):
        r = requests.get(self.url)
        if not r.status_code == 200:
            raise PageLoadingException(f"Url {self.url} returned {r.status_code}.")
        return r.text

    def _parse_page(self, xml):
        import xml.etree.ElementTree as ET

        root = ET.fromstring(xml)
        currencies = {}
        for child in root:
            if child.tag == "Valute":  # and child.attrib.get("ID") == self.currency_id:
                try:
                    currencies[child.attrib["ID"]] = {
                        "from": {
                            "label": "RUB",
                            "value": float(
                                child.find("Nominal").text.replace(",", ".")
                            ),
                        },
                        "to": {
                            "label": child.find("CharCode").text,
                            "value": float(child.find("Value").text.replace(",", ".")),
                        },
                    }
                except (AttributeError, KeyError) as e:
                    pass  # TODO: log e and child.find("CharCode")
        return {
            "date": datetime.strptime(root.attrib["Date"], "%d.%m.%Y"),
            "rates": currencies,
        }

    def run(self):
        page_xml = self._get_page()
        return self._parse_page(page_xml)
