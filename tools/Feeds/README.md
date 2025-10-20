## A collection of tools to interact with Spur's Feeds.  
#### *All scripts expect a Spur token to be set as $TOKEN, otherwise will prompt the user to paste it in.

- ### spurfeedmultifilter.py: 
  - A Spur feed downloader/decompressor with keyword parsing ability. Will output lines matching the keyword(s) to a new file.

## Archived:
- ### feedsandqueries.py:
  - Will prompt the user to either download a new feed file or reference an existing file. Then will prompt the user for which feed they would like to download, followed by running queries against it including a shuf command, export all queries to .txt files, and convert them to csv. May expect filenames of provided files to include YYYYMMDD and the type of feed, e.g. AnonRes, Anonymous, or AnonResRT.
