from flask import url_for, escape
from bs4 import BeautifulSoup

def make_urls_trackable(text, log_uuid):
    """
        Modifies the href of each anchor tag of text (string) so that it is trackable, logged
        to the specified log_uuid (string uuid). Returns text (string) with modified anchor hrefs.
    """
    if 'href' in text:
        new_urls = []
        soup = BeautifulSoup(text, 'html.parser')
        for i, link in enumerate(soup.find_all('a')):
            old_url = link.get('href')
            if not old_url:
                continue
            new_url = url_for('tracking.url', u=old_url, l=log_uuid, _external=True)
            new_urls.append(new_url)
            soup.find_all('a')[i]['href'] = new_url
        # unescape
        text = str(soup)
        for new_url in new_urls:
            escaped_url = escape(new_url)
            text = text.replace(escaped_url, new_url)
    return text

def get_beacon_html(log_uuid):
    """
        Returns HTML to render a tracking beacon
    """
    beacon_url = url_for('tracking.get_beacon', u=log_uuid, _external=True)
    return '<img src="{}">'.format(beacon_url)

