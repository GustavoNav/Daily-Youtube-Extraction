import requests
import re
from bs4 import BeautifulSoup

class HttpRequester():
    def __init__(self) -> None:
        self.__url = 'https://www.youtube.com/feed/trending' 


    def request(self):
        request = self.__request_url(self.__url)

        site = BeautifulSoup(request.text, 'html.parser')

        html = self.__collect_essential(str(site))
        links = self.__collect_link(html)

        # Remove Duplicates
        links_unique = list(set(links))
        
        return links_unique

    def __request_url(self,url):
        headers = {'Accept-Language': 'pt-BR'}
        request = requests.get(url, headers=headers)

        return request
    
    def __collect_essential(self, site):
        match = None
        pattern = r'FEtrending.*FEtrending'
        match = re.search(pattern, site)

        return match.group()
    
    def __collect_link(self, html):
        pattern = r'\"url\":\"/watch\?v=([^\"]*)\"'
        
        match = None
        match = re.findall(pattern, html)
        
        links = []
        for code in match:
            links.append('https://www.youtube.com/watch?v=' + code)
        
        return links

    



    


    


