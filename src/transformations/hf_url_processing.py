import tldextract


def get_tld(url):
    return (
        tldextract.extract(url).domain + "." + tldextract.extract(url).suffix
        if url
        else None
    )
