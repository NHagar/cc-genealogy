import tldextract


def get_tld(url):
    return (
        tldextract.extract(url).domain + "." + tldextract.extract(url).suffix
        if url
        else None
    )


def get_tld_partitions(df):
    df["domain"] = df["url"].apply(get_tld)
    return df
