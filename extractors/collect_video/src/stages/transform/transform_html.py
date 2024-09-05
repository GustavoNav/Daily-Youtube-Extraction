import re
import os
import json
from datetime import datetime, timezone


class TransformHtml():
    def transform(self) -> list:
        file_path = os.path.abspath(os.path.join(__file__, '../../../../export/extract/html.json'))
        with open(file_path, 'r', encoding='utf-8') as file:
            htmls = json.load(file)

        information = []

        for html in htmls:
        
            transformed_data = {
                'title': self.__collect_title(html),
                'channel_id': self.__collect_channel_id(html),
                'channel_link': self.__collect_channel_link(html),
                'likes': self.__collect_likes(html),
                'views': self.__collect_views(html),
                'comments': self.__collect_comments(html),
                'duration': self.__collect_duration(html),
                'date': self.__collect_date(html),
                'thumb': self.__collect_thumb(html),
                'family_friend': self.__collect_family_safe(html),
                'extraction_date': (datetime.today()).strftime('%Y-%m-%d')
            }
            information.append(transformed_data)

        return information
    
    def __collect_title(self, html):
        match = None
        pattern = r'</title><meta content=\"(.*?)\"'
        match = re.search(pattern, html)

        if match:
            title = match.group(1)
            return title
        
        return None
    
    def __collect_channel_id(self, html):
        match = None
        pattern = r'"externalChannelId\":\"([^"]{1,50})"'
        
        match = re.search(pattern, html)

        if match:
            channel_id = match.group(1)
            return channel_id
        else:
            pattern = r'<link href=\"http://www.youtube.com/@(.{1,30}?)\"'
            match = re.search(pattern, html)
            if match:
                channel_id = match.group(1)
                return channel_id
            
        return None
    
    def __collect_channel_link(self, html):
        match = None
        pattern = r'"externalChannelId\":\"([^"]{1,50})"'
        
        match = re.search(pattern, html)

        if match:
            channel_id = match.group(1)
            channel_id = 'http://www.youtube.com/channel/' + channel_id
            return channel_id

        return None
    
    def __collect_likes(self,html):
        match = None
        pattern = r'\\\"Gostei\\\" com mais (.*?) pessoas'
        match = re.search(pattern, html)
        
        if match:
            likes = match.group(1)
            likes = likes.replace('.', '')
            return likes
        
        return None
    
    def __collect_views(self, html):
        match = None
        pattern = r'\"views\":{\"simpleText\":\"(.{1,50}?) visualizações'
        match = re.search(pattern, html)
        
        if match:
            views = match.group(1)
            views = views.replace('.', '')
            return views
        
        return None
    
    def __collect_date(self, html):
        match = None
        pattern = r'itemprop=\"datePublished\"/><meta content=\"(.{1,30}?)\"'
        match = re.search(pattern, html)

        if match:
            date = match.group(1)

            return date
            
        return None
    
    def __collect_comments(self, html):
        match = None
        pattern = r'Comentários\"\}\]\},\"contextualInfo\":\{\"runs\":\[\{\"text\":\"(.{1,20}?)\"'
        match = re.search(pattern, html)

        if match:
            comments = match.group(1)
            comments = comments.replace('\xa0', ' ')
            comments = comments.replace(',', '.')

            multiplier = 1

            values = comments.split(' ')

            values.append('')
            
            if values[1] == 'mil':
                multiplier = 1000
            elif values[1] == 'mi':
                multiplier = 1000000

            comments_total = int(float(values[0]) * multiplier)

            return comments_total
        
        return None
    
    def __collect_duration(self, html):
        match = None
        pattern = r',\"approxDurationMs\":\"(.{1,30}?)\"}'
        
        match = re.search(pattern, html)

        if match:
            duration = match.group(1)

            timestamp = int(duration) / 1000  # ms to seconds 
            time_obj = datetime.fromtimestamp(timestamp, tz=timezone.utc)
            duration = time_obj.strftime('%H:%M:%S')

            return duration
        else:
            pattern = r'\"approxDurationMs\":\"(\d+?)\"'
            match = re.search(pattern, html)
            if match:
                duration = match.group(1)

                timestamp = int(duration) / 1000  # ms to seconds 
                time_obj = datetime.fromtimestamp(timestamp, tz=timezone.utc)
                duration = time_obj.strftime('%H:%M:%S')
            
                return duration

        
        return None
    
    def __collect_thumb(self, html):
        match = None
        pattern = r'"thumbnails":\[\{"url":"https://i\.ytimg\.com/vi/([\w-]{11})/hqdefault\.jpg'
        match = re.search(pattern, html)

        if match:
            id = match.group(1)
            thumb = 'http://img.youtube.com/vi/' + id + '/maxresdefault.jpg'
            return thumb
        
        return None
    
    def __collect_family_safe(self, html):
        match = None
        pattern = r'\"isFamilySafe\":\s*([^,]+)'
        match = re.search(pattern, html)

        if match:
            family_friend = match.group(1)
            family_friend = family_friend.capitalize()
            if family_friend == 'True':
                value = 1
            elif family_friend == 'False':
                value = 0
            else:
                return None
            
            return value
        
        return None