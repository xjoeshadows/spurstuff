## A collection of tools to interact with Spur's Feeds. Substitute your own token variable for the placeholder in the scripts.

- ### dailyfeedcounts.py: 
  - Will grab the line count of each of the feeds, report them to the user, and export to a YYYYMMDDDailyFeedCount.txt file. Handy for adding to your .zshrc file and for doing date over date comparisons of growth of the feeds.

- ### spurfeedmultifilter.py: 
  - A Spur feed downloader/decompressor with keyword parsing ability. Will output lines matching the keyword(s) to a new file.

## Archived:
- ### feedsandqueries.py:
  - Will prompt the user to either download a new feed file or reference an existing file. Then will prompt the user for which feed they would like to download, followed by running queries against it including a shuf command, export all queries to .txt files, and convert them to csv. May expect filenames of provided files to include YYYYMMDD and the type of feed, e.g. AnonRes, Anonymous, or AnonResRT.
