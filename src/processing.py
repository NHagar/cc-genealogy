import tldextract


def get_tld(batch):
    urls = batch["url"]
    domains = []
    for url in urls:
        ext = tldextract.extract(url)
        domains.append(ext.domain + "." + ext.suffix)
    return {"domain": domains}


def extract_urls(ds, extraction_config, num_proc=1):
    """
    Extract URLs from a dataset based on the specified extraction configuration.

    Args:
        ds: The dataset to extract URLs from
        extraction_config: Dictionary with extraction configuration
        num_proc: Number of processes to use for parallel processing

    Returns:
        Dataset with only the URL column
    """
    extraction_type = extraction_config["type"]

    if extraction_type == "direct":
        # Direct column selection
        column = extraction_config["column"]
        return ds.select_columns([column])

    elif extraction_type == "eval_dict":
        # Evaluate a string as dict and extract field
        column = extraction_config["column"]
        field = extraction_config["field"]

        def extract_field_batch(batch):
            urls = []
            for item in batch[column]:
                try:
                    if isinstance(item, str):
                        value_dict = eval(item)
                    else:
                        value_dict = item
                    urls.append(value_dict[field])
                except (KeyError, SyntaxError, TypeError):
                    urls.append(None)
            return {"url": urls}

        return ds.map(
            extract_field_batch,
            batched=True,
            num_proc=num_proc,
            load_from_cache_file=False,
        )

    elif extraction_type == "nested_field":
        # Extract from nested JSON structure
        column_path = extraction_config["column_path"]

        def extract_nested_batch(batch):
            urls = []
            for i in range(len(next(iter(batch.values())))):
                current = {k: batch[k][i] for k in batch.keys()}
                try:
                    value = current
                    for key in column_path:
                        value = value[key]
                    urls.append(value)
                except (KeyError, TypeError):
                    urls.append(None)
            return {"url": urls}

        return ds.map(
            extract_nested_batch,
            batched=True,
            num_proc=num_proc,
            load_from_cache_file=False,
        )

    else:
        raise ValueError(f"Unknown URL extraction type: {extraction_type}")
