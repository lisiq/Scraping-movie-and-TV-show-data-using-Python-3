import pandas as pd
from bs4 import BeautifulSoup
import requests
import re
import json

# we first get a US proxy and the web-page that we want to scrape
proxies = {
  'http': 'http://92.119.177.92',
}
url = 'https://www.tbs.com'
urlshows = 'https://www.tbs.com/shows'


# we scrape all the urls from the web-page
# we only keep the ones that have /shows/ in it
r = requests.get(urlshows, proxies = proxies)
c = r.content
soup = BeautifulSoup(c, 'html.parser')
result = []
for a in soup.find_all('a', href=True):
    if ('/shows/' in a['href'])  or ('/sports/' in a['href']) or ('/thedressupgang' in a['href']) or  ('/miseryindex' in a['href']):
        result.append(a['href'])


# now we open each link and get all the seasons and episodes per link
episodes = []
for i in result:
    r = requests.get(url + i + '/watch-now', proxies = proxies)
    c = r.content
    soup = BeautifulSoup(c, 'html.parser')
    for a in soup.find_all('a', href=True):
        if a['href'].startswith(i+'/s'):
            episodes.append(a['href'])


# we remove the duplicated links
# save all the links for furute needs
episodes = list(set(episodes))
for i in episodes:
    with open('/home/lisi/MediaBiz/Second scrape/TBS/episodes.txt', 'a') as f:
        f.write(i+'\n')


with open('/home/lisi/MediaBiz/Second scrape/TBS/episodes.txt', 'r') as file:
    lines = [line.strip() for line in file]
    

# open all the links and scrape the json file that has some data about the series
# and save them so we do not have to redowload them if we make some mistakes in the future
for i in lines:
    r = requests.get(url + i, proxies = proxies)
    c = r.content
    soup = BeautifulSoup(c, 'html.parser')
    with open('/home/lisi/MediaBiz/Second scrape/TBS/TBSshows_ldjson/' + i.encode("utf-8").hex() + '.json', 'w') as j:
        j.write(json.dumps(
            str(soup.find_all(lambda tag: tag.name == 'script' and 
                              tag.get('type') == 'application/ld+json'))[36:-10]))


# we open the json files and make the syntax python friendly
# after that we transofrm them into python dictionaries
# and save them in a list. There can be some json files 
# that do not have the info that we need so we ignore them
dict_list = []
for i in lines:
    try:
        with open('/home/lisi/MediaBiz/Second scrape/TBS/TBSshows_ldjson/' + i.encode("utf-8").hex() + '.json', 'r') as j:
            dict_list.append(dict(eval(json.load(j).replace('null', '"NaN"')\
                                           .replace('true',"True")\
                                           .replace('false',"False"))))
    except SyntaxError:
        pass
        

# we define this helper function to get only the data 
# that we need form the dictionaries
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


# we apply the helper function to all the dictionaries 
# and ignore the files that do not have the data that we need
flat_list = []
for i in dict_list:
    try:
        flat_list.append(get_info(i))
    except KeyError:
        pass

# # here we create a data fram from the list overhead and name the columns
tbsshows_ldjson = pd.DataFrame(flat_list, 
                               columns=['url', 'genre', 'language', 'bot_country', 'availabilityStarts', 
                                        'availabilityEnds', 'bot_system', 'offer_type'])

# finally we save the data as a csv file
tbsshows_ldjson.to_csv('/home/lisi/MediaBiz/Second scrape/TBS/tbsshows_ldjson.csv', index=False)

# there is another json file that contain some more information
# we will scrape also that
# to scrape this we do not need all the link, we just need a link per season
# since it containas information for every other episode
new_lines = []
for i in lines:
    match = "/".join(i.split("/", 4)[:4]) #split until fourth occurence of /, and then get first 4 elements, after that join
    if match not in new_lines:
        new_lines.append(match)
        new_lines.append(i)

new_lines = new_lines[1::2]

# we scrape all the json files and save them 
for i in lines:
    r = requests.get(url + i, proxies = proxies)
    c = r.content
    soup = BeautifulSoup(c, 'html.parser')
    with open('/home/lisi/MediaBiz/Second scrape/TBS/TBSshows_drupla-settings-json/' + i.encode("utf-8").hex() + '.json', 'w') as j:
        j.write(json.dumps(
            str(soup.find_all(lambda tag: tag.name == 'script' and 
                              tag.get('type') == 'application/json' and
                            tag.get('data-drupal-selector') == 'drupal-settings-json')[0])[76:-9]))


# we open the files transform them into dictionaries, make the syntax python friendly
# and ignore the files that do not have the data that we need
flat_list = []
dict_list = []
for i in new_lines:
    try:
        with open('/home/lisi/MediaBiz/Second scrape/TBS/TBSshows_drupla-settings-json/' + i.encode("utf-8").hex() + '.json', 'r') as j:
            dict_list.append(dict(eval(json.load(j).replace('null', '"NaN"')\
                                       .replace('true',"True")\
                                       .replace('false',"False")))['turner_playlist'])
    except KeyError:
        pass

# # transform from a nested list into a list      
flat_list = [item for sublist in dict_list for item in sublist]

# # create a dataframe
tbsshows_drupla_settings_json = pd.DataFrame(flat_list)

# # finally save it
tbsshows_drupla_settings_json.to_csv('/home/lisi/MediaBiz/Second scrape/TBS/tbsshows_drupla_settings_json.csv', index=False)

# # we combine the two datasets in one
df_tbsshows = pd.merge(tbsshows_drupla_settings_json, tbsshows_ldjson, how='left', on='url')

# # save the merger
df_tbsshows.to_csv('/home/lisi/MediaBiz/Second scrape/TBS/tbsshows.csv', index=False)
