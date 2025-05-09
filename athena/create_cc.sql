CREATE EXTERNAL TABLE IF NOT EXISTS cc (
    crawl STRING,
    url_host_name STRING,
    url_count INT
)
PARTITIONED BY (
    dataset_name STRING -- This will correspond to your top-level directory names
)
STORED AS PARQUET
LOCATION 's3://hf-datasets-nh/'
TBLPROPERTIES (
    'projection.enabled'='true',
    'projection.dataset_name.type'='enum',
    'projection.dataset_name.values' = 'CC_MAIN_2017_47_urls,CC_MAIN_2024_18_urls,CC-MAIN-2013-20_urls,CC-MAIN-2013-48_urls,CC-MAIN-2014-10_urls,CC-MAIN-2014-15_urls,CC-MAIN-2014-23_urls,CC-MAIN-2014-35_urls,CC-MAIN-2014-41_urls,CC-MAIN-2014-42_urls,CC-MAIN-2014-49_urls,CC-MAIN-2014-52_urls,CC-MAIN-2015-06_urls,CC-MAIN-2015-11_urls,CC-MAIN-2015-14_urls,CC-MAIN-2015-18_urls,CC-MAIN-2015-22_urls,CC-MAIN-2015-27_urls,CC-MAIN-2015-32_urls,CC-MAIN-2015-35_urls,CC-MAIN-2015-40_urls,CC-MAIN-2015-48_urls,CC-MAIN-2016-07_urls,CC-MAIN-2016-18_urls,CC-MAIN-2016-22_urls,CC-MAIN-2016-26_urls,CC-MAIN-2016-30_urls,CC-MAIN-2016-36_urls,CC-MAIN-2016-40_urls,CC-MAIN-2016-44_urls,CC-MAIN-2016-50_urls,CC-MAIN-2017-04_urls,CC-MAIN-2017-09_urls,CC-MAIN-2017-13_urls,CC-MAIN-2017-17_urls,CC-MAIN-2017-22_urls,CC-MAIN-2017-26_urls,CC-MAIN-2017-30_urls,CC-MAIN-2017-34_urls,CC-MAIN-2017-39_urls,CC-MAIN-2017-43_urls,CC-MAIN-2017-51_urls,CC-MAIN-2018-05_urls,CC-MAIN-2018-09_urls,CC-MAIN-2018-13_urls,CC-MAIN-2018-17_urls,CC-MAIN-2018-22_urls,CC-MAIN-2018-26_urls,CC-MAIN-2018-30_urls,CC-MAIN-2018-34_urls,CC-MAIN-2018-39_urls,CC-MAIN-2018-43_urls,CC-MAIN-2018-47_urls,CC-MAIN-2018-51_urls,CC-MAIN-2019-04_urls,CC-MAIN-2019-09_urls,CC-MAIN-2019-13_urls,CC-MAIN-2019-18_urls,CC-MAIN-2019-22_urls,CC-MAIN-2019-26_urls,CC-MAIN-2019-30_urls,CC-MAIN-2019-35_urls,CC-MAIN-2019-39_urls,CC-MAIN-2019-43_urls,CC-MAIN-2019-47_urls,CC-MAIN-2019-51_urls,CC-MAIN-2020-05_urls,CC-MAIN-2020-10_urls,CC-MAIN-2020-16_urls,CC-MAIN-2020-24_urls,CC-MAIN-2020-29_urls,CC-MAIN-2020-34_urls,CC-MAIN-2020-40_urls,CC-MAIN-2020-45_urls,CC-MAIN-2020-50_urls,CC-MAIN-2021-04_urls,CC-MAIN-2021-10_urls,CC-MAIN-2021-17_urls,CC-MAIN-2021-21_urls,CC-MAIN-2021-25_urls,CC-MAIN-2021-31_urls,CC-MAIN-2021-39_urls,CC-MAIN-2021-43_urls,CC-MAIN-2021-49_urls,CC-MAIN-2022-05_urls,CC-MAIN-2022-21_urls,CC-MAIN-2022-27_urls,CC-MAIN-2022-33_urls,CC-MAIN-2022-40_urls,CC-MAIN-2022-49_urls,CC-MAIN-2023-06_urls,CC-MAIN-2023-14_urls,CC-MAIN-2023-23_urls,CC-MAIN-2023-40_urls,CC-MAIN-2023-50_urls,CC-MAIN-2024-10_urls',
    'storage.location.template'='s3://hf-datasets-nh/${dataset_name}/' -- Template for partition locations
);

