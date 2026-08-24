## A collection of tools to interact with Spur's Feeds. Substitute your own token variable for the placeholder in the scripts. 
### *All scripts expect a Spur token to be set as $TOKEN, otherwise will prompt the user to paste it in.

## Enrichment Tools:

- #### contextAPIeasyenrichment.py: 
  - An easy way to do context api enrichment. Accepts a filename of IPs (comma, newline, or space separated) as an argument as well as pasting into the terminal.
  
- #### spurcurrentipenrichment.py
  - a simple tool to do an enrichment on the user's current external IP with colorized output

- #### contextAPIFlexibleFileEnrichment.py:
  - Enriches a CSV of IPs and outputs to JSONL. Requires at least a column entitled IPs, IP Addresses, etc...and also accepts a Timestamp column for historical Context API lookups. Will take any additional columns (such as a transaction ID column) and appends to the output enrichment e.g. for correlation purposes. The script has a variable number of workers for parallel lookup requests.
 
- #### contextAPIEnrichmentHelper.py:
  - I created this tool to help provide enrichments for IPs which the original FlexibleFileEnrichment tool may have not found enrichments for based on the original timestamps provided. It allows you to specify the NoEnrichment output file from that script and specify a new timeframe to attempt to enrich against the Spur API with. 
 
- #### contextAPI_timelineanalysis.py:
  - Will enrich an IP or IPs with multiple dates based on the user timeline specified and return the changes to the data over that timeline for quick temporal analysis.

- #### contextAPI_HistoricEnrichmentDiffer.py:
  - A full differ across multiple enrichment files of IPs with two operating modes:
    - Mode 1 will do a strict A-to-B diff If an IP appears more than once in either file, only its LATEST occurrence is kept (earlier duplicates are dropped). Best for a quick 'what changed since last time' comparison.
    - Mode 2 keeps EVERY historical record for an IP (indexed by Timestamp) instead of collapsing to just the latest, so you can see how
      an IP's data evolved across multiple past dates before being compared against the current file. Best for forensic/investigative work: spotting attributes that appeared and disappeared over time, multi-stage changes, or brand-new context that never showed up historically.

## Data Management Tools:
- #### contextAPI_JsonAnalyzer.py:
  - Designed to break down a JSON of Spur's data, either from an enrichment or a feed to provide a deep statistical analysis of the keys witin it.
- #### contextAPI_HistoricEnrichmentDiffer.py:
  - Designed to help analyze the differences between two JSON files, either at a high level statistical analysis of the differences between the files or a file export of the deep IP by IP level breakdown of key/value changes/adds/deletions.    

- #### IPs+Timestamps.csv:
  - Sample CSV file






