try:
    from urlparse import urljoin
except ImportError:
    from urllib import urljoin

import requests
from requests.adapters import HTTPAdapter

from .settings import get_config


class Client(object):
    def __init__(self, master_url=None, token=None):
        self.config = get_config()
        self.master_url = master_url or self.config.get('master_url')
        self.token = token or self.config.get('token')
        self.session = requests.Session()
        self.session.mount('https://', HTTPAdapter(max_retries=1000))
        self.session.mount('http://', HTTPAdapter(max_retries=1000))

    def post(self, url, data):
        headers = {'Authorization': 'Token ' + self.token}
        response = self.session.post(url, data, headers=headers, verify=False)
        # TODO: Define exceptions form response
        if response.status_code != 201:
            raise Exception(response.content)
        return response

    def put(self, url, data):
        headers = {'Authorization': 'Token ' + self.token}
        response = self.session.put(url, data, headers=headers, verify=False)
        # TODO: Define exceptions form response
        if response.status_code != 200:
            raise Exception(response.content)
        return response

    def patch(self, url, data):
        headers = {'Authorization': 'Token ' + self.token}
        response = self.session.patch(url, data, headers=headers, verify=False)
        # TODO: Define exceptions form response
        if response.status_code != 200:
            raise Exception(response.content)
        return response

    def post_result(self, bench_name, data, metadata):
        result_data = {}
        result_data.update(data)
        result_data.update(metadata)
        url = urljoin(self.master_url, bench_name) + '/'
        # self.logger.info('POST log: %s %s' % (response.status_code, url))
        response = self.post(url, result_data)
        return response

    def create_scenario(self, name, data):
        url = urljoin(self.master_url, 'scenario/')
        url = urljoin(url, name + '/')
        response = self.post(url, data)
        return response.json()

    def create_transaction(self, scenario_name, transaction_name, data):
        url = urljoin(self.master_url, 'scenario/')
        url = urljoin(url, scenario_name + '/')
        url = urljoin(url, transaction_name + '/')
        response = self.post(url, data)
        return response.json()

    def patch_resource(self, resource_name, data, pk):
        url = urljoin(self.master_url, resource_name) + '/' + str(pk) + '/'
        response = self.patch(url, data)
        return response
