import json
import os
from bs4 import BeautifulSoup
import requests


class HttpRequester():

    def request(self, urls):
        responses = self.__request_url(urls)

        sites = []
        for reponse in responses:
            site = BeautifulSoup(reponse.text, 'html.parser')
            sites.append(site)

        self.__save(sites)

        

    def __request_url(self, urls):
        headers = {'Accept-Language': 'pt-BR'}
        responses = []

        for url in urls:
            url += '/about'
            try:
                response = requests.get(url, headers=headers)
            except:
                print(url)
            responses.append(response)

        return responses
    

    def __save(self, sites):
        sites_serializable = [str(site) for site in sites]

        dir_path = os.path.abspath(os.path.join(__file__, '../../../export/extract'))

        os.makedirs(dir_path, exist_ok=True)

        file_path = os.path.join(dir_path, 'html.json')

        with open(file_path, 'w', encoding='utf-8') as file:
            json.dump(sites_serializable, file, ensure_ascii=False, indent=4)
