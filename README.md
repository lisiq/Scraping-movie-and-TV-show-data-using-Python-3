
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

## There are five different scripts

One type is to scrape data from the web-pages for shows, one is to scrape data from the web-pages for movies.
We then have _dataframes.py_ which is the same for shows, and one for the movies. I could have done it all in the same script but I decided to do it this way since I always changed some details.
The last one is _merger.py_.

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

## Special links

When there are special links refere to Shaq's Life, NBA on TNT or Wrestling to scrape them
