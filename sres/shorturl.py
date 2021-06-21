from flask import current_app
import requests
import json

from sres.config import _get_proxies

def _get_groups():
    ret = {
        'success': False,
        'message': "",
        'group_guid': "",
        'groups': []
    }
    r = requests.get(
        url=current_app.config['SRES']['URL_SHORTENING']['BITLY']['API_ENDPOINT_ROOT'] + 'groups',
        headers={
            'Authorization': 'Bearer ' + current_app.config['SRES']['URL_SHORTENING']['BITLY']['ACCESS_TOKEN']
        },
        proxies=_get_proxies()
    )
    ret['groups'] = r.json()['groups']
    ret['group_guid'] = ret['groups'][0]['guid']
    ret['success'] = True
    return ret
    
def shorten(long_url):
    ret = {
        'success': False,
        'messages': [],
        'long_url': "",
        'short_url': ""
    }
    groups = _get_groups()
    payload = {
        'long_url': long_url,
        'group_guid': groups['group_guid'],
        'domain': current_app.config['SRES']['URL_SHORTENING']['BITLY']['DOMAIN']
    }
    print(payload)
    r = requests.post(
        url=current_app.config['SRES']['URL_SHORTENING']['BITLY']['API_ENDPOINT_ROOT'] + 'shorten',
        headers={
            'Authorization': 'Bearer ' + current_app.config['SRES']['URL_SHORTENING']['BITLY']['ACCESS_TOKEN'],
            'Host': "api-ssl.bitly.com",
            'Content-Type': 'application/json'
        },
        data=json.dumps(payload),
        proxies=_get_proxies()
    )
    if r.status_code == 200 or r.status_code == 201:
        ret['long_url'] = r.json()['long_url']
        ret['short_url'] = r.json()['link']
        ret['success'] = True
    else:
        ret['messages'].append(("Code {}".format(r.status_code), "danger"))
    return ret
    
    
    