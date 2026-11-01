# Screenshot checklist

Save actual captures in `screenshots/`. No screenshots have been captured by this documentation task; leave boxes unchecked until the corresponding evidence exists. Capture the real environment and command output. Exclude private keys, credentials, access tokens, and unrelated account details from frames.

- [ ] `screenshots/01-ec2-instance.png`: EC2 instance running the project; show instance state and instance type. Do not imply multiple worker instances.
- [ ] `screenshots/02-hadoop-java-versions.png`: `hadoop version` showing Hadoop 3.4.3 and `java -version` showing Java 11.
- [ ] `screenshots/03-hdfs-services.png`: `jps` showing NameNode/DataNode and `hdfs dfsadmin -report` showing the single-node HDFS service state.
- [ ] `screenshots/04-configuration.png`: Evidence of `dfs.replication=1` and `mapreduce.framework.name=local` from the active configuration/job output. Record that YARN was not configured; do not describe this as a YARN job.
- [ ] `screenshots/05-input-data.png`: `head large.log`, `wc -l large.log` showing 10,000 records, and `hdfs dfs -ls <actual-input-directory>` showing the uploaded input. Replace the placeholder with the real HDFS path.
- [ ] `screenshots/06-pages-report.png`: `cat results/large/pages.tsv`; include all six page totals. If available, also show `hdfs dfs -cat '<actual-page-output>/part-*'` to connect the export to HDFS.
- [ ] `screenshots/07-ip-report.png`: `cat results/large/ip.tsv`; capture all 50 IP/count pairs across multiple images if needed. Include actual HDFS output/path evidence where available.
- [ ] `screenshots/08-status-report.png`: `cat results/large/status.tsv` showing 200 → 7915, 404 → 1659, 500 → 426; include actual HDFS output/path evidence where available.
- [ ] `screenshots/09-verification.png`: `python3 verify_reports.py` showing PASS for pages, IP, and status, each with total 10000; show the zero exit status using `echo $?` immediately afterward.

Expected page totals are `/about=1709`, `/contact=1677`, `/home=1689`, `/login=1649`, `/missing=1659`, `/products=1617`. A screenshot is evidence of the command shown, not a performance benchmark or fault-tolerance test.
