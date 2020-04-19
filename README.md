
# Scraping information from TBS, TNT and TruTV

A python approach of scraping infromation from 3 provider sites _TBS_, _TNT_ and _TruTV_.

## Required libraries


```python
import pandas as pd
from bs4 import BeautifulSoup
import requests
import re
import json
```

## There are two different scripts

For shows and for movies.

### Shows

Scripts to download shows from the providers are in the respective folders. In each of them you can find two different folders with json files scraped from the web-pages. Among the folders is also the _python file_ to scrape the data and also a _csv_ file with all the data scraped.
The approach that I take is:
- First I get all the urls from the web-page,
- Then I select only the relevant ones,
- After that I open each url and scrape the data under type 'application/ld+json'
- Transform the JSON files into python dictionaries and get the relevant information,
- Further more we scrape other data under the type drupal-settings-json,
- And finally we combine the data.

### Movies

For Movies we use the same approach as for the shows. The only difference is that we do not iterate over each episode.

## Final product

After having all the wanted data scraped we merge all of it in one csv file by running _merger.py_. The final csv file is under the name __finalproduct.csv__.
