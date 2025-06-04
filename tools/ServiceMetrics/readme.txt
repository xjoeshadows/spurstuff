Tools:

servicemetrics.py: downloads and creates a file of current ServiceMetrics tracked by Spur

servicemetricsdiff.py: will diff two txt file outputs of servicemetrics.py (presumably done on different dates) and create an output highlighting the added and removed tags between them

servicemetricsdiff-enriched.py: will do a diff the same as the previous diff script, but will also then enrich each of the added tags against the tag lookup API and output a jsonl of the details for each tag

serviceMetrics-findbadservices.py: a script that will download the service metrics feed, decompress it, grep it for malicious|malware|trojan, and export services with those words in the description to a jsonl file
