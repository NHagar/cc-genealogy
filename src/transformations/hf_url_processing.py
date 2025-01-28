from urllib.parse import urlparse


def safe_get_domain(url):
    try:
        return urlparse(url).netloc
    except (AttributeError, TypeError):
        return None
