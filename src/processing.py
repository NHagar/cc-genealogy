import tldextract


def get_tld(batch):
    urls = batch["url"]
    domains = []
    for url in urls:
        ext = tldextract.extract(url)
        domains.append(ext.domain + "." + ext.suffix)
    return {"domain": domains}
