import tldextract


def extract_domain(url: str) -> str:
    if url is None:
        return None
    # Use tldextract to parse the URL
    extracted = tldextract.extract(url)

    # Return the full domain information as a formatted string
    return f"{extracted.domain}.{extracted.suffix}"
