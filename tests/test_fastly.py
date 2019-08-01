import asyncio
import os

import mock
import pendulum
import requests_mock
import simplejson
import unittest

from singer import Schema

from tap_fastly import FastlyClient, FastlyAuthentication, FastlySync


def load_file_current(filename, path):
    myDir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(myDir, path, filename)
    with open(path) as file:
        return simplejson.load(file)

def load_file(filename, path):
    sibB = os.path.join(os.path.dirname(__file__), '..', path)
    with open(os.path.join(sibB, filename)) as f:
        return simplejson.load(f)

# Our test case class
class MyGreatClassTestCase(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(MyGreatClassTestCase, self).__init__(*args, **kwargs)
        self.client = FastlyClient(FastlyAuthentication("111"))

    @requests_mock.mock()
    def test_projects(self, m):
        record_value = load_file_current('stats_output.json', 'data_test')
        m.get('https://api.fastly.com/stats?from=15603730166&to=1564150277', json=[record_value])
        self.assertEqual(self.client.stats("15603730166", "1564150277"), [record_value])


    @requests_mock.mock()
    def test_sync_stats(self, m):
        loop = asyncio.get_event_loop()
        record_value = load_file_current('stats_output.json', 'data_test')
        with mock.patch('tap_fastly.FastlyClient.stats', return_value=record_value):
            record_service = load_file_current('service_output.json', 'data_test')
            with mock.patch('tap_fastly.FastlyClient.service', return_value=record_service):
                config = {"start_date":"2017-07-01T00:00:00Z"}
                dataSync = FastlySync(self.client,{}, config=config)
                schema = load_file('stats.json', 'tap_fastly/schemas')
                resp = dataSync.sync_stats(Schema(schema))
                with mock.patch('singer.write_record') as patching:
                    task = asyncio.gather(resp)
                    loop.run_until_complete(task)
                    patching.assert_called_with('stats', record_value['data']['id'][0])

    @requests_mock.mock()
    def test_events(self, m):
        record_value = load_file_current('bills_output.json', 'data_test')
        m.get('https://api.fastly.com/billing/v2/year/2019/month/7', json=[record_value])
        at = pendulum.parse("2019-07-31T00:00:00+00:00")
        self.assertEqual(self.client.bill(at), [record_value])


    @requests_mock.mock()
    def test_sync_events(self, m):
        loop = asyncio.get_event_loop()
        record_value = load_file_current('bills_output.json', 'data_test')
        with mock.patch('tap_fastly.FastlyClient.bill', return_value=record_value):
            dataSync = FastlySync(self.client)
            schema = load_file('bills.json', 'tap_fastly/schemas')
            start = pendulum.parse("2019-05-31T00:00:00+00:00")
            end = pendulum.now()
            period = pendulum.period(start, end)
            resp = dataSync.sync_bills(Schema(schema), period=period)
            with mock.patch('singer.write_record') as patching:
                task = asyncio.gather(resp)
                loop.run_until_complete(task)
                patching.assert_called_with('bills', record_value)


if __name__ == '__main__':
    unittest.main()