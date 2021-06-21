from flask import request
import urllib

def get_scanner_launch_uri(return_url):
    if _is_ios():
        return 'mochabarcode://' + 'CALLBACK=' + return_url
    else:
        return 'zxing://scan/?' + 'ret=' + urllib.parse.quote_plus(return_url) + '&SCAN_FORMATS=QR_CODE,CODE_39,CODE_128,CODABAR'

def _is_ios():
    return request.user_agent.platform.lower() in ['ipad', 'iphone', 'ipod']
