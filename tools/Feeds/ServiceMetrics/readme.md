#### Tools:

- servicemetrics.py: downloads and outputs a list of current ServiceMetrics tracked by Spur to the terminal

- servicemetrics-listandcount.py: will download the latest service metrics feed from Spur to YYMMDDServiceMetricsAll.json, perform a line count and report the count to the user as well as export a list of the service tags to YYYYMMDDServiceMetricsList.txt for future comparison if desired.

- servicemetricsdiff.py: relies on ServiceMetricsLists created by servicemetrics-listandcount.py or servicemetrics.py >> outputlist.txt. The script will diff them (presumably each list is a different date) and creates an output to the terminal highlighting the added and removed tags between them. Order of the input files matters, e.g. **servicemetricsdiff.py file1.txt file2.txt** where file1.txt is the older ServiceMetricsList file.

- servicemetrics-manual-diff-enrich.py: will do a diff the same as the previous diff script via user supplied ServiceMetricsList files such as what may have been downloaded/converted by other scripts, but will also then enrich each of the diff'd tags against the tag lookup API and output a jsonl with the details for each tag. Order of the input files argument matters. **Usage: servicemetrics-manual-diff-enrich.py ServiceMetricsList1.txt ServiceMetricsList2.txt**


#### Extra:

- servicemetrics-listmaker.py: will convert a full Service Metrics feed file into a list of service tags which can be used by any of these scripts where the script takes an input file of service tags, e.g. **servicemetrics-manual-diff-enrich.py YYYYMMDDServiceMetricsAll.json**

#### Archived:
- servicemetrics-auto-diff-enrich.py: will download the newest full service metrics feed and asks the user for an older service tags list file to compare against. It will then do a diff and then enrich each of the added tags against the tag lookup API and output a jsonl of the details for each tag.

- serviceMetrics-findbadservices.py: a script that will download the complete service metrics feed, decompress it, grep it for malicious|malware|trojan, and export the services with those words in their description to a jsonl file

- serviceMetrics-findresidentialproxies.py: Will download the latest ServiceMetrics feed and grep for "residential_proxy" to help find interesting res proxy services.
