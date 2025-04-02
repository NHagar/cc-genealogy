from collections import Counter

from datasets import load_dataset
from tqdm import tqdm

cc_main_list = [
    "CC-MAIN-2023-06",
    "CC-MAIN-2022-49",
    "CC-MAIN-2022-40",
    "CC-MAIN-2022-33",
    "CC-MAIN-2022-27",
    "CC-MAIN-2022-21",
    "CC-MAIN-2022-05",
    "CC-MAIN-2021-49",
    "CC-MAIN-2021-43",
    "CC-MAIN-2021-39",
    "CC-MAIN-2021-31",
    "CC-MAIN-2021-25",
    "CC-MAIN-2021-21",
    "CC-MAIN-2021-17",
    "CC-MAIN-2021-10",
    "CC-MAIN-2021-04",
    "CC-MAIN-2020-50",
    "CC-MAIN-2020-45",
    "CC-MAIN-2020-40",
    "CC-MAIN-2020-34",
    "CC-MAIN-2020-29",
    "CC-MAIN-2020-24",
    "CC-MAIN-2020-16",
    "CC-MAIN-2020-10",
    "CC-MAIN-2020-05",
    "CC-MAIN-2019-51",
    "CC-MAIN-2019-47",
    "CC-MAIN-2019-43",
    "CC-MAIN-2019-39",
    "CC-MAIN-2019-35",
    "CC-MAIN-2019-30",
    "CC-MAIN-2019-26",
    "CC-MAIN-2019-22",
    "CC-MAIN-2019-18",
    "CC-MAIN-2019-13",
    "CC-MAIN-2019-09",
    "CC-MAIN-2019-04",
    "CC-MAIN-2018-51",
    "CC-MAIN-2018-47",
    "CC-MAIN-2018-43",
    "CC-MAIN-2018-39",
    "CC-MAIN-2018-34",
    "CC-MAIN-2018-30",
    "CC-MAIN-2018-26",
    "CC-MAIN-2018-22",
    "CC-MAIN-2018-17",
    "CC-MAIN-2018-13",
    "CC-MAIN-2018-09",
    "CC-MAIN-2018-05",
    "CC-MAIN-2017-51",
    "CC_MAIN_2017_47",
    "CC-MAIN-2017-43",
    "CC-MAIN-2017-39",
    "CC-MAIN-2017-34",
    "CC-MAIN-2017-30",
    "CC-MAIN-2017-26",
    "CC-MAIN-2017-22",
    "CC-MAIN-2017-17",
    "CC-MAIN-2017-13",
    "CC-MAIN-2017-09",
    "CC-MAIN-2017-04",
    "CC-MAIN-2016-50",
    "CC-MAIN-2016-44",
    "CC-MAIN-2016-40",
    "CC-MAIN-2016-36",
    "CC-MAIN-2016-30",
    "CC-MAIN-2016-26",
    "CC-MAIN-2016-22",
    "CC-MAIN-2016-18",
    "CC-MAIN-2016-07",
    "CC-MAIN-2015-48",
    "CC-MAIN-2015-40",
    "CC-MAIN-2015-35",
    "CC-MAIN-2015-32",
    "CC-MAIN-2015-27",
    "CC-MAIN-2015-22",
    "CC-MAIN-2015-18",
    "CC-MAIN-2015-14",
    "CC-MAIN-2015-11",
    "CC-MAIN-2015-06",
    "CC-MAIN-2014-52",
    "CC-MAIN-2014-49",
    "CC-MAIN-2014-42",
    "CC-MAIN-2014-41",
    "CC-MAIN-2014-35",
    "CC-MAIN-2014-23",
    "CC-MAIN-2014-15",
    "CC-MAIN-2014-10",
    "CC-MAIN-2013-48",
    "CC-MAIN-2013-20",
]


def get_top_n_sites(dataset, n=20):
    counter = Counter()
    for batch in tqdm(dataset.iter(batch_size=10_000)):
        counter.update(batch["domain"])

    return counter.most_common(n)


def filter_domains(batch):
    domains = batch["domain"]
    return [domain == "nytimes.com" for domain in domains]


def extract_date_and_section(batch):
    urls = batch["url"]
    dates = []
    yearmonths = []
    sections = []
    for url in urls:
        try:
            pathsplit = url.split(".com/")[-1].split("/")
            date = "-".join(pathsplit[0:3])
            section = pathsplit[3]
            dates.append(date)
            yearmonth = date[:7]
            yearmonths.append(yearmonth)
            sections.append(section)
        except Exception:
            dates.append(None)
            yearmonths.append(None)
            sections.append(None)
    batch["date"] = dates
    batch["yearmonth"] = yearmonths
    batch["section"] = sections
    return batch


def count_and_pct_breakdown(dataset, column):
    counter = Counter()
    counter.update(dataset[column])
    total = dataset.num_rows
    all_counts = counter.most_common()
    all_counts = [(k, v, v / total) for k, v in all_counts]
    return all_counts


def main():
    c4 = load_dataset("nhagar/c4_urls_en", split="train")
    falcon = load_dataset("nhagar/falcon-refinedweb_urls", split="train")
    cc_main_c4 = load_dataset("nhagar/CC-MAIN-2019-18_urls", split="train")
    cc_nyt_c4 = load_dataset("nhagar/CC-MAIN-2019-18_nyt_urls", split="train")
    cc_main_falcon = [
        load_dataset(f"nhagar/{cc}_urls", split="train") for cc in cc_main_list
    ]
    cc_nyt_falcon = [
        load_dataset(f"nhagar/{cc.replace('_', '-')}_nyt_urls", split="train")
        for cc in cc_main_list
    ]

    # get top 20 sites for c4 and falcon, and output to CSVs
    c4_top_sites = get_top_n_sites(c4, n=20)
    falcon_top_sites = get_top_n_sites(falcon, n=20)

    with open("data/c4_top_sites.csv", "w") as f:
        f.write("domain,count\n")
        for domain, count in c4_top_sites:
            f.write(f"{domain},{count}\n")
    with open("data/falcon_top_sites.csv", "w") as f:
        f.write("domain,count\n")
        for domain, count in falcon_top_sites:
            f.write(f"{domain},{count}\n")
    print(
        "Top sites for C4 and Falcon datasets have been saved to c4_top_sites.csv and falcon_top_sites.csv."
    )

    # sort cc_main_c4 by url_count and output top 20 to CSV
    cc_main_c4_sorted = cc_main_c4.sort("url_count", reverse=True)
    cc_main_c4_top_sites = cc_main_c4_sorted.select(range(20))
    cc_main_c4_top_sites = cc_main_c4_top_sites.to_pandas()
    cc_main_c4_top_sites.to_csv("data/cc_main_c4_top_sites.csv", index=False)
    print(
        "Top sites for CC-MAIN-2019-18 dataset have been saved to cc_main_c4_top_sites.csv."
    )

    # iterate through cc_main_falcon, sum url_counts across all datasets, and output top 20 to CSV
    cc_main_falcon_sums = Counter()
    total_url_count = 0
    for dataset in cc_main_falcon:
        for row in dataset:
            domain = row["domain"]
            url_count = row["url_count"]
            cc_main_falcon_sums[domain] += url_count
            total_url_count += url_count
    cc_main_falcon_top_sites = cc_main_falcon_sums.most_common(20)
    with open("data/cc_main_falcon_top_sites.csv", "w") as f:
        f.write("domain,count\n")
        for domain, count in cc_main_falcon_top_sites:
            f.write(f"{domain},{count}\n")
    print(
        "Top sites for falcon cc datasets have been saved to cc_main_falcon_top_sites.csv."
    )

    # get slice of c4 and falcon with domain of nytimes.com
    c4_nyt = c4.filter(
        filter_domains,
        batched=True,
        batch_size=10_000,
        num_proc=4,
    )

    falcon_nyt = falcon.filter(
        filter_domains,
        batched=True,
        batch_size=10_000,
        num_proc=4,
    )

    # calculate percentage of c4 and falcon that is nytimes.com
    c4_nyt_pct = c4_nyt.num_rows / c4.num_rows * 100
    falcon_nyt_pct = falcon_nyt.num_rows / falcon.num_rows * 100
    # calculate percentange of CC data that is nytimes.com
    cc_c4_nyt_pct = cc_nyt_c4.num_rows / cc_main_c4.num_rows * 100
    total_nyt_falcon_rows = sum(dataset.num_rows for dataset in cc_nyt_falcon)
    cc_falcon_nyt_pct = total_nyt_falcon_rows / total_url_count * 100
    # write percentages to CSV
    with open("data/nyt_pct.csv", "w") as f:
        f.write("dataset,percentage\n")
        f.write(f"c4,{c4_nyt_pct}\n")
        f.write(f"falcon,{falcon_nyt_pct}\n")
        f.write(f"cc_c4,{cc_c4_nyt_pct}\n")
        f.write(f"cc_falcon,{cc_falcon_nyt_pct}\n")
    print("Percentage of data that is nytimes.com has been saved to nyt_pct.csv.")

    # extract date and section from c4 and falcon
    c4_nyt = c4_nyt.map(
        extract_date_and_section,
        batched=True,
        batch_size=10_000,
        num_proc=4,
    )
    falcon_nyt = falcon_nyt.map(
        extract_date_and_section,
        batched=True,
        batch_size=10_000,
        num_proc=4,
    )
    cc_nyt_c4 = cc_nyt_c4.map(
        extract_date_and_section,
        batched=True,
        batch_size=10_000,
        num_proc=4,
    )
    cc_nyt_falcon = [
        dataset.map(
            extract_date_and_section,
            batched=True,
            batch_size=10_000,
            num_proc=4,
        )
        for dataset in cc_nyt_falcon
    ]

    # get count and percentage breakdown of section and yearmonth for each dataset, then output to CSV
    c4_nyt_section_counts = count_and_pct_breakdown(c4_nyt, "section")
    falcon_nyt_section_counts = count_and_pct_breakdown(falcon_nyt, "section")
    cc_nyt_c4_section_counts = count_and_pct_breakdown(cc_nyt_c4, "section")
    cc_nyt_falcon_section_agg_counter = Counter()
    total_nyt_url_count = 0
    for dataset in cc_nyt_falcon:
        total_nyt_url_count += dataset.num_rows
        for batch in dataset.iter(batch_size=10_000):
            valid_items = [item for item in batch["section"] if item is not None]
            cc_nyt_falcon_section_agg_counter.update(valid_items)
    cc_nyt_falcon_section_counts = cc_nyt_falcon_section_agg_counter.most_common()
    cc_nyt_falcon_section_counts_final = [
        (k, v, v / total_nyt_url_count) for k, v in cc_nyt_falcon_section_counts
    ]
    with open("data/section_counts.csv", "w") as f:
        f.write("dataset,section,count,percentage\n")
        for section, count, pct in c4_nyt_section_counts:
            f.write(f"c4,{section},{count},{pct}\n")
        for section, count, pct in falcon_nyt_section_counts:
            f.write(f"falcon,{section},{count},{pct}\n")
        for section, count, pct in cc_nyt_c4_section_counts:
            f.write(f"cc_c4,{section},{count},{pct}\n")
        for section, count, pct in cc_nyt_falcon_section_counts_final:
            f.write(f"cc_falcon,{section},{count},{pct}\n")
    print("Section counts and percentages have been saved to section_counts.csv.")
    # get count and percentage breakdown of yearmonth for each dataset, then output to CSV
    c4_nyt_yearmonth_counts = count_and_pct_breakdown(c4_nyt, "yearmonth")
    falcon_nyt_yearmonth_counts = count_and_pct_breakdown(falcon_nyt, "yearmonth")
    cc_nyt_c4_yearmonth_counts = count_and_pct_breakdown(cc_nyt_c4, "yearmonth")
    cc_nyt_falcon_yearmonth_agg_counter = Counter()
    for dataset in cc_nyt_falcon:
        for batch in dataset.iter(batch_size=10_000):
            valid_items = [item for item in batch["yearmonth"] if item is not None]
            cc_nyt_falcon_yearmonth_agg_counter.update(valid_items)
    cc_nyt_falcon_yearmonth_counts = cc_nyt_falcon_yearmonth_agg_counter.most_common()
    cc_nyt_falcon_yearmonth_counts_final = [
        (k, v, v / total_nyt_url_count) for k, v in cc_nyt_falcon_yearmonth_counts
    ]
    # write counts and percentages to CSV
    with open("data/yearmonth_counts.csv", "w") as f:
        f.write("dataset,yearmonth,count,percentage\n")
        for yearmonth, count, pct in c4_nyt_yearmonth_counts:
            f.write(f"c4,{yearmonth},{count},{pct}\n")
        for yearmonth, count, pct in falcon_nyt_yearmonth_counts:
            f.write(f"falcon,{yearmonth},{count},{pct}\n")
        for yearmonth, count, pct in cc_nyt_c4_yearmonth_counts:
            f.write(f"cc_c4,{yearmonth},{count},{pct}\n")
        for yearmonth, count, pct in cc_nyt_falcon_yearmonth_counts_final:
            f.write(f"cc_falcon,{yearmonth},{count},{pct}\n")


if __name__ == "__main__":
    main()
