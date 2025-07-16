Tools:

servicemetrics.py: downloads and outputs a list of current ServiceMetrics tracked by Spur

servicemetricsdiff.py: will diff two txt file outputs of servicemetrics.py (presumably done on different dates) and create an output highlighting the added and removed tags between them

servicemetrics-manual-diff-enrich.py: will do a diff the same as the previous diff script via user supplied files such as what may have been downloaded by other scripts, but will also then enrich each of the since-added tags against the tag lookup API and output a jsonl with the details for each tag

servicemetrics-listandcount.py: will download the latest service metrics feed from Spur to YYMMDDServiceMetricsAll.json, perform a line count and report the count to the user as well as export a list of the service tags to YYYYMMDDServiceMetricsList.txt for future comparison if desired.

servicemetrics-auto-diff-enrich.py: will download the newest list of service tags, ask the user for an older service tags list file to compare against, do a diff, and then enrich each of the added tags against the tag lookup API and output a jsonl of the details for each tag.

serviceMetrics-customfilter.py: A custom keyword parser you can use against a service metric feed file. This script will download a new copy of the feed first, prompt if the the user would like to search against a specific column, if yes then asks for the keyword and output filename, if no then just asks for the keyword to search for and then the output filename.

serviceMetrics-findbadservices.py: a script that will download the complete service metrics feed, decompress it, grep it for malicious|malware|trojan, and export the services with those words in their description to a jsonl file

serviceMetrics-findresidentialproxies.py: Will download the latest ServiceMetrics feed and grep for "residential_proxy" to help find interesting res proxy services.

servicemetrics-listmaker.py: will convert a full Service Metrics feed file into a list of service tags which can be used by any of these scripts where the script takes an input file of service tags, e.g. servicemetrics-manual-diff-enrich.py
