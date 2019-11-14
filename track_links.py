import re
import time
import logging

import requests


def get_vqd(keywords):
    url = 'https://duckduckgo.com/'
    while True:
        try:
            params = {
                'q': keywords
            }
            res = requests.post(url, data=params)
            searchObj = re.search(r'vqd=(\d-\d+-\d+)', res.text, re.M | re.I)
            return searchObj.group(1)
        except ValueError as e:
            logging.debug("Hitting Url Failure - Sleep and Retry: %s", url)
            time.sleep(5)
            continue


def search(keywords):
    try:
        vqd = get_vqd(keywords)
    except AttributeError as ae:
        logging.error(ae)
        return []
    url = 'https://duckduckgo.com/'
    headers = {
        'dnt': '1',
        'x-requested-with': 'XMLHttpRequest',
        'accept-language': 'en-GB,en-US;q=0.8,en;q=0.6,ms;q=0.4',
        'user-agent':
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'referer': 'https://duckduckgo.com/',
        'authority': 'duckduckgo.com',
    }

    params = (
        ('l', 'wt-wt'),
        ('p', '-2'),
        ('s', '30'),
        ('ex', '-2'),
        ('ct', 'EN'),
        ('ss_mkt', 'us'),
        ('sp', '0'),
        ('ext', '1'),
        ('q', keywords),
        ('vqd', vqd),
    )

    request_url = url + "d.js"
    youtube_link_re = r"((http(s)?\:\/\/)?(www\.)?(youtube|youtu)((\.com|\.be)\/)(watch\?v=)?([0-z]{11}|[0-9a-zA-Z]{4}(\-|\_)[0-z]{4}|.(\-|\_)[0-z]{9}))"
    while True:
        try:
            res = requests.get(request_url, headers=headers, params=params)
            results = re.findall(youtube_link_re, res.text)
            return list(map(lambda result: result[0], results))
        except ValueError as e:
            logging.debug("Hitting Url Failure - Sleep and Retry: %s", request_url)
            time.sleep(5)
            continue
