Tools:

servicemetrics.py: downloads and outputs a file of current ServiceMetrics tracked by Spur

servicemetrics-listandcount.py: will download the latest service metrics feed from Spur to YYMMDDServiceMetricsAll-Full.json, perform a line count and report the count to the user as well as export a list of the service tags to YYYYMMDDServiceMetrics.txt for comparison if desired.

servicemetricsdiff.py: will diff two txt file outputs of servicemetrics.py (presumably done on different dates) and create an output highlighting the added and removed tags between them

servicemetrics-manual-diff-enrich.py: will do a diff the same as the previous diff script via user supplied files such as what may have been downloaded by servicemetrics.py, but will also then enrich each of the since-added tags against the tag lookup API and output a jsonl with the details for each tag

servicemetrics-auto-diff-enrich.py: will download the newest list of service tags, ask the user for an older service tags list file to compare against, do a diff, and then enrich each of the added tags against the tag lookup API and output a jsonl of the details for each tag.

serviceMetrics-findbadservices.py: a script that will download the complete service metrics feed, decompress it, grep it for malicious|malware|trojan, and export the services with those words in their description to a jsonl file
serviceMetrics-findresidentialproxies.py: Will download the latest ServiceMetrics feed and grep for "residential" to help find interesting res proxy services.
