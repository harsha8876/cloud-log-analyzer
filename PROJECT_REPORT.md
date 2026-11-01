# Project Title

Cloud-Based Web Log Analyzer using Hadoop MapReduce

## Student Name(s)

- Harsha Shetty — 230905536
- Kevin Vataliya — 230905366

## Objective

Implement web-log aggregation by page, IP address, and HTTP status using Hadoop MapReduce concepts, and validate the saved reports against an independent Python calculation for 10,000 records.

## Problem Statement

Raw web logs contain individual requests rather than usable totals. Summarizing request frequency by page, client address, and response code helps characterize traffic and identify unsuccessful responses. This project tests whether a Streaming aggregation pipeline produces complete and correct counts on a controlled dataset.

## Methodology / Tools Used

The stated project environment uses Python 3, Hadoop 3.4.3, Java 11, HDFS, and one AWS EC2 instance. Python code uses only standard-library modules, including `random`, `datetime`, `pathlib`, and `collections.Counter`.

The reported configuration is HDFS replication factor 1 and `mapreduce.framework.name=local`. YARN was not configured. HDFS supplies storage while the local MapReduce runner performs computation on the same host. The original configuration files and execution transcript are not supplied locally; environment claims are based on the project setup provided, while report correctness was checked directly.

Method: generate a reproducible dataset, map each request to a selected key with count 1, sort/group by key, reduce counts, export reports, and compare each exported key/count pair with independent counters. The file layout and full reproduction commands are documented in [README.md](README.md).

## Implementation

`generate_logs.py` uses a fixed random seed of 42 to generate exactly 10,000 four-field records in `large.log`: `IP timestamp page HTTP_status`. It selects from 50 IPs (`192.0.2.1`–`192.0.2.50`) and six pages (`/home`, `/about`, `/products`, `/contact`, `/login`, `/missing`). Timestamps start at `2026-11-01T10:00:00` and advance one second, ending at `2026-11-01T12:46:39`. `/missing` receives 404; other pages receive mostly 200 and occasional 500 (5% selection probability).

`mapper.py` reads stdin, checks for exactly four fields, warns to stderr for malformed records, and emits `page<TAB>1`. It only implements page selection. `reducer.py` sums sorted tab-separated pairs and handles empty input without output. The same reducer can aggregate IP and status keys. Separate IP/status mapper scripts are absent from the supplied files; the README provides inline field-selection commands for reproducing those reports rather than attributing them to the page mapper.

The intended storage and processing flow is:

```text
synthetic logs → HDFS → Hadoop Streaming mapper → sorting/grouping
               → reducer → HDFS reports → local TSV exports
```

The saved sample reports are in `results/`. The larger reports are in `results/large/`. `verify_reports.py` independently reads `large.log` using `collections.Counter`, checks every expected and reported key/count, rejects duplicates and unexpected keys, and requires a total of exactly 10,000 per report. It exits nonzero on missing files or mismatches and prints PASS or FAIL for each report. It does not use mapper/reducer output as its expected values.

## Results

The existing code and saved results were inspected before writing this report. Running `python3 verify_reports.py` produced:

```text
PASS pages: every key and count matches; total = 10000
PASS ip: every key and count matches; total = 10000
PASS status: every key and count matches; total = 10000
```

**All three reports matched independent counts for 10,000 records.**

| Report | Distinct keys | Total records | Independent verification |
| --- | ---: | ---: | --- |
| `results/large/pages.tsv` | 6 | 10,000 | PASS |
| `results/large/ip.tsv` | 50 | 10,000 | PASS |
| `results/large/status.tsv` | 3 | 10,000 | PASS |

Page totals:

| Page | Records |
| --- | ---: |
| `/about` | 1709 |
| `/contact` | 1677 |
| `/home` | 1689 |
| `/login` | 1649 |
| `/missing` | 1659 |
| `/products` | 1617 |

Status totals:

| HTTP status | Records |
| --- | ---: |
| `200` | 7915 |
| `404` | 1659 |
| `500` | 426 |

The complete 50-IP table is in [README.md](README.md#ip-addresses--resultslargeiptsv) and the source report `results/large/ip.tsv`. Counts range from 169 requests (`192.0.2.14`) to 225 (`192.0.2.35`). The six-record sample has page totals `/about=2`, `/home=3`, `/missing=1`, and status totals `200=5`, `404=1`.

These results establish aggregation correctness for the saved dataset. No execution times, speedups, costs, or security features are claimed. The saved TSV values and local validation do not by themselves provide an execution trace of the remote Hadoop jobs. The screenshot checklist identifies additional environment and execution evidence to capture.

## Conclusion

The saved page, IP, and status reports are correct for all 10,000 synthetic records, as confirmed by independent counters. The implementation illustrates log generation, Streaming key/count aggregation, HDFS input/output, and validation.

The dataset is synthetic, the environment has one EC2 instance, HDFS replication is 1, and MapReduce uses `mapreduce.framework.name=local` without YARN. These constraints mean the project demonstrates correctness, not distributed performance or fault tolerance. Multi-node scheduling, redundant storage, production data handling, and recovery behavior have not been demonstrated.

## References

- Apache Hadoop 3.4.3: [Hadoop Streaming](https://hadoop.apache.org/docs/r3.4.3/hadoop-streaming/HadoopStreaming.html).
- Apache Hadoop 3.4.3: [Setting up a Single Node Cluster](https://hadoop.apache.org/docs/r3.4.3/hadoop-project-dist/hadoop-common/SingleCluster.html).
- Apache Hadoop 3.4.3: [File System Shell](https://hadoop.apache.org/docs/r3.4.3/hadoop-project-dist/hadoop-common/FileSystemShell.html).
- Uploaded course slides: **[Add exact uploaded course slide title(s)]**. The slides and their titles were unavailable in this workspace when this documentation was written; this reference requires completion from the original upload.
