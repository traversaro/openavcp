openavcp
========

Script for scraping and processing data for italian tenders

Dependencies
------------
You need some python libraries to run this script:
  * [`requests`](http://docs.python-requests.org/en/latest/)
  * [`dataset`](https://dataset.readthedocs.org/en/latest/)
  
Run the script
--------------
To run the script, just launch it with the following arguments:
`./portaletrasparenza-avcp-scraper.py download extract`
This will download the data in raw xml files and extract them in
a sqlite database named `avcp_contracts.db`. 

It is then possible to dump a csv version of this database using 
the [datafreeze](https://dataset.readthedocs.org/en/latest/freezefile.html) tool:

``
datafreeze openspending_freeze.yaml 
``
