import os
import json
import asyncio
from pathlib import Path
from itertools import repeat
from urllib.parse import urljoin

import singer
import requests
import pendulum
from singer.bookmarks import write_bookmark, get_bookmark
from pendulum import datetime, period


class FastlyAuthentication(requests.auth.AuthBase):
    def __init__(self, api_token: str):
        self.api_token = api_token

    def __call__(self, req):
        req.headers.update({"Fastly-Key": self.api_token})

        return req


class FastlyClient:
    def __init__(self, auth: FastlyAuthentication, url="https://api.fastly.com"):
        self._base_url = url
        self._auth = auth
        self._session = None

    @property
    def session(self):
        if not self._session:
            self._session = requests.Session()
            self._session.auth = self._auth
            self._session.headers.update({"Accept": "application/json"})

        return self._session

    def _get(self, path, params=None):
        url = urljoin(self._base_url, path)
        response = self.session.get(url, params=params)
        response.raise_for_status()

        return response.json()

    def bill(self, at: datetime):
        try:
            return self._get(f"billing/v2/year/{at.year}/month/{at.month}")
        except:
            return None

    def stats(self, at, params=None):
        try:
            if at is not None:
                return self._get(f"stats"/{at})
            else:
                return self._get(f"stats")
        except:
            return None

    def service(self, service_id):
        try:
            return self._get(f"service/{service_id}")
        except:
            return None

class FastlySync:
    def __init__(self, client: FastlyClient, state={}):
        self._client = client
        self._state = state

    @property
    def client(self):
        return self._client

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, value):
        singer.write_state(value)
        self._state = value

    def sync(self, stream, schema):
        func = getattr(self, f"sync_{stream}")
        return func(schema)

    async def sync_bills(self, schema, period: pendulum.period = None):
        """Output the `bills` in the period."""
        stream = "bills"
        loop = asyncio.get_event_loop()

        if not period:
            # build a default period from the last bookmark
            start = pendulum.parse(get_bookmark(self.state, stream, "start_time"))
            end = pendulum.now()
            period = pendulum.period(start, end)

        singer.write_schema(stream, schema.to_dict(), ["invoice_id"])

        for at in period.range("months"):
            result = await loop.run_in_executor(None, self.client.bill, at)
            if result:
                response = result.copy()

                if response['line_items']:
                    result['line_items'] = json.dumps(response['line_items'])
                if response['regions']:
                    result['regions'] = json.dumps(response['regions'])
                if response['total']['extras']:
                    result['total_extras'] = json.dumps(response['total']['extras'])
                singer.write_record(stream, result)
                self.state = write_bookmark(self.state, stream, "start_time", result["end_time"])

    async def sync_stats(self, schema, period:pendulum.period = None):
        """Output the stats in the period."""
        stream = "stats"
        loop = asyncio.get_event_loop()

        singer.write_schema(stream, schema.to_dict(), ["service_id", "start_time"])
        start = get_bookmark(self.state, stream, "from")
        result = await loop.run_in_executor(None, self.client.stats, start)
        if result:
            for n in result['data']:
                service_result = await loop.run_in_executor(None, self.client.service, n)
                for i in result['data'][n]:
                    i['service_name'] = service_result['name']
                    i['service_versions'] = json.dumps(service_result['versions'])
                    i['service_customer_id'] = service_result['customer_id']
                    i['service_publish_key'] = service_result['publish_key']
                    i['service_comment'] = service_result['comment']
                    i['service_deleted_at'] = service_result['deleted_at']
                    i['service_updated_at'] = service_result['updated_at']
                    i['service_created_at'] = service_result['created_at']
                    singer.write_record(stream, i)
                    self.state = write_bookmark(self.state, stream, "from", result['meta']["to"])