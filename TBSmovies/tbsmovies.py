import pandas as pd
from bs4 import BeautifulSoup
import requests
import re
import json

# we first get a US proxy and the web-page that we want to scrape
proxies = {
  'http': 'http://92.119.177.89',
}
url = 'https://www.tbs.com'
urlmovies = 'https://www.tbs.com/movies'

# we scrape all the urls from the web-page
# we only keep the ones that have /movies/ in it
r = requests.get(urlmovies, proxies = proxies)
c = r.content
soup = BeautifulSoup(c, 'html.parser')
result = []
for a in soup.find_all('a', href=True):
    if a['href'][:8] == '/movies/':
        result.append(a['href'])

# there can be some urls repeating so we remove duplicates
result = list(set(result))


# now we scrape the json files and save so we can play with the code in the future
# without the need to redownload it. 
for i in result:
    r = requests.get(url + i, proxies = proxies)
    c = r.content
    soup = BeautifulSoup(c, 'html.parser')
    with open('/home/lisi/Desktop/MediaBiz/TBSmovies/TBSmovies_ldjson/' + i.encode("utf-8").hex() + '.json', 'w') as j:
        j.write(json.dumps(
            str(soup.find_all(lambda tag: tag.name == 'script' and 
                              tag.get('type') == 'application/ld+json'))[36:-10]))


# we open the saved jsons and replace some values to make the syntax python friendly
# some of the files do not have all the info that we need so we ingore them
# and we transfom them into python dictionaries
dict_list = []
for i in result:
    try:
        with open('/home/lisi/Desktop/MediaBiz/TBSmovies/TBSmovies_ldjson/' + i.encode("utf-8").hex() + '.json', 'r') as j:
            dict_list.append(dict(eval(json.load(j).replace('null', '"NaN"')\
                                           .replace('true',"True")\
                                           .replace('false',"False"))))
    except SyntaxError:
        pass


# we define this helper function to get only the data that we are interested in
def get_info(n):
    m = n['potentialAction'][0]
    url = "\\/" + "/".join(n['@id'].split("/", 4)[3:])
    genre = m['@type']
    language = m['target']['inLanguage']
    bot_country = m['actionAccessibilityRequirement']['eligibleRegion']['name']
    availabilityStarts = m['actionAccessibilityRequirement']['availabilityStarts']
    availabilityEnds = m['actionAccessibilityRequirement']['availabilityEnds']
    bot_system = m['actionAccessibilityRequirement']['requiresSubscription']['name']
    offer_type = m['actionAccessibilityRequirement']['requiresSubscription']['authenticator']['name']
    return [url, genre, language, bot_country, availabilityStarts, availabilityEnds, bot_system, offer_type]


# we apply the above function to all the files and store the values in a list
flat_list = []
for i in dict_list:
    try:
        flat_list.append(get_info(i))
    except KeyError:
        pass


# here we create a data fram from the list overhead and name the columns
tbsmovies_ldjson = pd.DataFrame(flat_list, 
                               columns=['url', 'genre', 'language', 'bot_country', 'availabilityStarts', 
                                        'availabilityEnds', 'bot_system', 'offer_type'])


# finally we save the data as a csv file
tbsmovies_ldjson.to_csv('/home/lisi/Desktop/MediaBiz/TBSmovies/tbsmovies_ldjson.csv', index=False)

# now there is another json file with more info so we scape also that and save them
for i in result:
    r = requests.get(url + i, proxies = proxies)
    c = r.content
    soup = BeautifulSoup(c, 'html.parser')
    with open('/home/lisi/Desktop/MediaBiz/TBSmovies/TBSmovies_drupla-settings-json/' + i.encode("utf-8").hex() + '.json', 'w') as j:
        j.write(json.dumps(
            str(soup.find_all(lambda tag: tag.name == 'script' and 
                              tag.get('type') == 'application/json' and
                            tag.get('data-drupal-selector') == 'drupal-settings-json')[0])[76:-9]))


# again we make the json files syntex python friendly
flat_list = []
dict_list = []
for i in result:
    try:
        with open('/home/lisi/Desktop/MediaBiz/TBSmovies/TBSmovies_drupla-settings-json/' + i.encode("utf-8").hex() + '.json', 'r') as j:
            dict_list.append(dict(eval(json.load(j).replace('null', '"NaN"')\
                                       .replace('true',"True")\
                                       .replace('false',"False")))['turner_playlist'])
    except KeyError:
        pass
        
# from a list of lists we create a flat list
flat_list = [item for sublist in dict_list for item in sublist]     

# create the data frame for these data
tbsmovies_drupla_settings_json = pd.DataFrame(flat_list)

# save the other data frame
tbsmovies_drupla_settings_json.to_csv('/home/lisi/Desktop/MediaBiz/TBSmovies/tbsmovies_drupla_settings_json.csv', index=False)

# merge both datasets and save them

df_tbsmovies = pd.merge(tbsmovies_drupla_settings_json, tbsmovies_ldjson, how='left', on='url')

df_tbsmovies.to_csv('/home/lisi/Desktop/MediaBiz/TBSmovies/tbsmovies.csv', index=False)