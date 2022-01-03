import requests
from config import wechat_config


def notify(title, message):
    """ Function to send notification using WeChat """
    url = wechat_config['url']
    data = {
        'title': str(title),
        'desp': str(message),
    }
    requests.post(url, data)
