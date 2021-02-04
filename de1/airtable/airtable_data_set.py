import time
from typing import Dict, Any
import urllib.parse

import pandas
import requests
from kedro.io import AbstractDataSet, DataSetError


class AirtableException(DataSetError):
    pass


class AirtableDataSet(AbstractDataSet):
    BASE_API = 'https://api.airtable.com/v0'

    def __init__(
            self,
            table_name: str,
            view: str = '',
            credentials: Dict[str, str] = None,
    ):
        """

        :param table_name:
        :param credentials:
            base_id:
            api_key:
        """
        self._table_name = table_name
        self._view = view
        if credentials is None or 'api_key' not in credentials or 'base_id' not in credentials:
            raise DataSetError('Credentials must be passed with "api_key" and "base_id" keys')
        self._api_key = credentials['api_key']
        self._base_id = credentials['base_id']

    def _page_url(self, offset=''):
        pagination_params = urllib.parse.urlencode({
            'pageSize': 100,
            'offset': offset,
        })
        api_url = f'{AirtableDataSet.BASE_API}/' \
                  f'{self._base_id}/' \
                  f'{urllib.parse.quote(self._table_name)}?' \
                  f'{pagination_params}'
        return api_url

    @property
    def _headers(self):
        return {
            'Authorization': f'Bearer {self._api_key}'
        }

    def _call_api(self, offset=''):
        url = self._page_url(offset=offset)
        while True:
            resp = requests.get(url, headers=self._headers)
            if resp.status_code == 429:
                time.sleep(30)
                continue
            elif resp.status_code >= 300:
                j = resp.json()
                if 'error' in j:
                    raise AirtableException(j['error']['type'])
                else:
                    raise AirtableException(j)
            return resp.json()

    def _retrieve_records(self):
        j = self._call_api()
        if 'error' in j:
            raise AirtableException(j['error']['type'])

        all_records = j['records']

        while 'offset' in j:
            j = self._call_api(offset=j['offset'])
            all_records += j['records']

        return all_records

    @staticmethod
    def _clean_records(raw_records):
        return [
            {
                **record['fields'],
                '__airtable_id': record['id'],
                '__airtable_created_time': record['createdTime'],
             }
            for record in raw_records
        ]

    def _load(self) -> Any:
        raw_records = self._retrieve_records()
        clean_records = self._clean_records(raw_records)
        return pandas.DataFrame(clean_records)

    def _save(self, data: Any) -> None:
        raise DataSetError('Save Unsupported')

    def _describe(self) -> Dict[str, Any]:
        return dict(
            table_name=self._table_name,
            view=self._view,
            base_id=self._base_id,
        )

