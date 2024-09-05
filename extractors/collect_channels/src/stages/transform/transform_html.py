import json
import os
import re
from datetime import datetime

class TransformHtml:

    def transform(self):
        file_path = os.path.abspath(os.path.join(__file__, '../../../../export/extract/html.json'))
        with open(file_path, 'r', encoding='utf-8') as file:
            htmls = json.load(file)

        informations = []
        for html in htmls:
            transformed_html = {
                'channel_id': self.__collect_channel_id(html),
                'channel_name': self.__collect_name(html),
                'icon': self.__collect_icon(html),
                'banner': self.__collect_banner(html),
                'subscriptions': self.__collect_subscriptions(html),
                'videos': self.__collect_videos(html),
                'views': self.__collect_views(html),
                'date_creation': self.__collect_date_creation(html),
                'channel_location': self.__collect_location(html),
                'extraction_date': (datetime.today()).strftime('%Y-%m-%d')
            }
        
            informations.append(transformed_html)
        
        return informations

    
    def __collect_channel_id(self, html):
        match = None
        pattern = r'\"iosAppArguments\":\"https://www.youtube.com/channel/(.{1,30}?)"'
        match = re.search(pattern, html)

        if match:
            channel_id = match.group(1)
            return channel_id

        return None

    def __collect_name(self, html):
        match = None
        pattern = r'<title>(.{1,40}?) - YouTube'
        match = re.search(pattern, html)

        if not match:
            pattern = r'<meta content=\"Canal (.{1,40})"'
            match = re.search(pattern, html)

        if match:
            name = match.group(1)
            return name
        
        return None
    
    def __collect_icon(self, html):
        match = None
        pattern = r'"width":88,"height":88},\{"url":"(https://.+?)",'
        match = re.search(pattern, html)

        if match:
            icon = match.group(1)
            return icon
            
        else:
            pattern = r'property="og:title"/><link href="(https://.+?)"'
            match = re.search(pattern, html)
            if match:
                icon = match.group(1)
                return icon
        
        return None

    def __collect_banner(self, html):
        match = None
        pattern = r'"banner":\{"thumbnails":\[\{"url":"(https://.+?)","'
        
        match = re.search(pattern, html)

        if match:
            banner = match.group(1)
            return banner
        else:
            pattern = r'\"banner\":{\"imageBannerViewModel\":{\"image\":{\"sources\":\[{\"url\":\"(https://[^"]+?)\"'
            match = re.search(pattern, html)
            if match:
                banner = match.group(1)
                return banner
        
        return None
    
    def __collect_subscriptions(self, html):
        match = None
        pattern = r'"subscriberCountText\":\"(.{1,20}?) inscritos'
        match = re.search(pattern, html)

        if not match:
            pattern = r'\"simpleText\":\"(.{1,20}?) inscritos'
            match = re.search(pattern, html)

        if match:
            subscriptions = match.group(1)
            subscriptions= subscriptions.replace('\xa0', ' ')
    
            values = subscriptions.split(' ')
            values.append('')
            

            number_str = values[0].replace(',', '.')
            multiplier = 1

            if values[1] == 'mi':
                multiplier = 1000000
            elif values[1] == 'mil':
                multiplier = 1000

            number = int(float(number_str) * multiplier)

            return number
        
        return None
    
    def __collect_videos(self, html):
        match = None

        pattern = r'"videosCountText":\{"runs":\[\{"text":"([\d,\. ]{1,20})"'
        match = re.search(pattern, html)

        if not match:
            pattern = r'"videoCountText":"([\d,\.]+) vídeos"'
            match = re.search(pattern, html)

        if not match:
            pattern = r'"text":\{"content":"([\d,\.]+) vídeos"'
            match = re.search(pattern, html)

        if match:
            videos = match.group(1)
            videos = videos.replace('\xa0', ' ')
            videos = videos.replace('.', '')
            return videos

        return None

    def __collect_views(self, html):
        match = None
        pattern = r'\"viewCountText\":\"(.{1,30}?) visualizações'
        
        match = re.search(pattern, html)

        if match:
            views = match.group(1)
            views = views.replace('.', '')
            return views
        
        return None
    
    def __collect_date_creation(self, html):
        match = None
        pattern = r'"joinedDateText":\{"content":"Inscreveu-se em (.{1,30}?)"'
        match = re.search(pattern, html)

        if match:
            date = match.group(1)
            
            meses = {
                'jan.': '01', 'fev.': '02', 'mar.': '03', 'abr.': '04',
                'mai.': '05', 'jun.': '06', 'jul.': '07', 'ago.': '08',
                'set.': '09', 'out.': '10', 'nov.': '11', 'dez.': '12'
            }
            
            regex_data = r'(\d{1,2}) de (\w{3}\.) de (\d{4})'
            match_data = re.match(regex_data, date)
            
            if match_data:
                dia = match_data.group(1)
                mes_abr = match_data.group(2)
                ano = match_data.group(3)
                
                mes_num = meses.get(mes_abr.lower())
                
                if mes_num:
                    data_formatada = f"{ano}-{mes_num}-{dia}"
                    return data_formatada
            
        return None


    def __collect_location(self, html):
        match = None
        pattern = r'"country":"(.{1,30}?)"'
        match = re.search(pattern, html)

        if match:
            location = match.group(1)
            return location
        
        return None