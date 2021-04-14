import pandas as pd
from bs4 import BeautifulSoup
import requests
import re
import numpy as np
import json

# # we first get a US proxy and the web-page that we want to scrape
proxies = {
  'http': 'http://92.119.177.92',
}
url = 'https://www.adultswim.com/'
urlshows = 'https://www.adultswim.com/videos'



# # we scrape all the urls from the web-page
# # we only keep the ones that have /vidoes/ in it
r = requests.get(urlshows)
c = r.content
soup = BeautifulSoup(c, 'html.parser')
result = []
for a in soup.find_all('a', href=True):
    if a['href'][:8] == '/videos/':
           result.append(a['href'])


# # we remove the duplicated links
# # save all the links for furute needs
shows = list(set(result))
for i in shows:
    with open('/home/lisi/Desktop/AdultSwim/shows.txt', 'a') as f:
        f.write(i+'\n')


with open('/home/lisi/Desktop/AdultSwim/shows.txt', 'r') as file:
    lines = [line.strip() for line in file]

# we now save all the episodes from all the shows in his file
episodes = []
for i in lines:
    r = requests.get(url + i)
    c = r.content
    soup = BeautifulSoup(c, 'html.parser')
    for a in soup.find_all('a', href=True):
        if a['href'][:8] == '/videos/':
            episodes.append(a['href'])

# remove the duplicaes and save for future needs
episodes = list(set(episodes))
for i in episodes:
    with open('/home/lisi/Desktop/AdultSwim/episodes.txt', 'a') as f:
        f.write(i+'\n')
        
with open('/home/lisi/Desktop/AdultSwim/episodes.txt', 'r') as file:
    lines = [line.strip() for line in file]
    

# open all the links and scrape the json file that has some data about the series
# and save them so we do not have to redowload them if we make some mistakes in the future
for i in lines:
    r = requests.get(url + i)
    c = r.content
    soup = BeautifulSoup(c, 'html.parser')
    with open('/home/lisi/Desktop/AdultSwim/ldjson/' +  i. replace('/','') + '.json', 'w') as j:
        j.write(json.dumps(
            str(soup.find_all(lambda tag: tag.name == 'script' and 
                              tag.get('type') == 'application/json'))[52:-10]))


# we open the json files and make the syntax python friendly
# after that we transofrm them into python dictionaries
# and save them in a list. There can be some json files 
# that do not have the info that we need so we ignore them
dict_list = []
for i in lines:
    try:
        with open('/home/lisi/Desktop/AdultSwim/ldjson/' + i.replace('/','') + '.json', 'r') as j:
                dict_list.append(dict(eval(json.load(j).replace('null', '"NaN"')\
                                               .replace('true',"True")\
                                               .replace('false',"False"))))
    except SyntaxError:
        try: # dragon ball super is a special case
            with open('/home/lisi/Desktop/AdultSwim/ldjson/' + i.replace('/','') + '.json', 'r') as j:
                    dict_list.append(dict(eval(json.load(j).replace('null','0').replace('true',"True").replace('false',"False"))))
        except SyntaxError:
            pass

# get info from json files
def get_info(n):
    list_list = list(map(list,n['props']['pageProps']['__APOLLO_STATE__'].items()))
    ep = []
    for i in list_list:
        if 'Video:' in i[0]:
            ep.append(i[1])
    return ep


# create dataframe
df = pd.DataFrame()
for i in dict_list:
    df = pd.concat([df, pd.DataFrame(get_info(i))], join = 'outer', axis=0)
    df = df.drop_duplicates().reset_index(drop=True)


df.set_axis(['source_id', 'offer_type', 'description','viewable_runtime',
             'episode_number', 'provider_cease_date', 'provider_release_date',
             'poster', 'season_number', 'program_title', 'maturity_rating', 
             'is_movie', 'type', 'series_title', 'release_date','slug'],axis=1, inplace=True)


# get series id from another key in json
series_id = dict()

for i in dict_list:
    list_list = list(map(list,i['props']['pageProps']['__APOLLO_STATE__'].items()))
    try:
        list_l = next(iter(i['props']['pageProps']['__APOLLO_STATE__']))
    except:
        pass
    for j,k in zip(list_list, range(len(list_list))):
        try:
            if "VideoCollection" == j[0][:15]:
                series_id[eval(re.search(r"{.*?}", list_l ).group(0))['slug']] = list_list[k][0]
        except:
            pass


# get program urls
program_url = dict()

for i in lines:
    program_url['/'.join(i.split("/", 4)[3:4])] = url +  i


# get series urls
series_url = dict()

for i in lines:
    series_url['/'.join(i.split("/", 3)[2:3])] = urlshows + '/' + '/'.join(i.split("/", 3)[2:3])



# put seires id in dataframe
s =  []
for i in range(len(df)):
    try:
        s.append(series_id[df.series_title[i]])
    except:
        s.append(np.nan)
        
df['series_source_id'] = s


# put series url in dataframe
uu = []
for i in range(len(df)):
    try:
        uu.append(series_url[df.series_title[i]])
    except:
        uu.append(np.nan)

df['series_url'] = uu


# put program url in dataframe
u = []
for i in range(len(df)):
    try:
        u.append(program_url[df.slug[i]])
    except:
        u.append(np.nan)

df['program_url'] = u


# put program and series key in dataframe
progkey = []
serkey = []
for i in range(len(df)):
    try:
        progkey.append('E|' + df.series_title[i].replace('-','_') + '|' + df.release_date[i][:4])
        serkey.append('S|' + df.slug[i].replace('-','_') + '|' + df.release_date[i][:4])
    except:
        progkey.append(np.nan)
        serkey.append(np.nan)


df['program_key'] = progkey
df['series_key'] = serkey


# some dataframe manipulations
df['capture_date'] = pd.Series([pd.to_datetime('05/17/2020')] * df.shape[0]).dt.strftime('%m/%d/%Y')
df['bot_version'] = pd.Series(['1.0.0'] * df.shape[0])
df = df[df['is_movie'] != 'clip'].reset_index(drop=True)
df['provider_release_date']= pd.to_datetime(df['provider_release_date']).dt.strftime('%m/%d/%Y')
df['provider_cease_date']= pd.to_datetime(df['provider_cease_date']).dt.strftime('%m/%d/%Y')
df['release_date']= pd.to_datetime(df['release_date']).dt.strftime('%m/%d/%Y')
df['viewable_runtime'] = df['viewable_runtime'].apply(lambda x: np.float64(np.nan) if (pd.isnull(x) == True) else (np.int64(x))).astype('Int64')
df['season_number'] = pd.to_numeric(df.season_number, errors = 'coerce').astype('Int64')
df['episode_number'] = pd.to_numeric(df.episode_number, errors = 'coerce').astype('Int64')
df['bot_system'] = pd.Series(['adultswim'] * df.shape[0])
df['is_movie'] = df['is_movie'].apply(lambda x: 1 if (x == 'movie') else 0)
df['bot_country'] = pd.Series(['US'] * df.shape[0])
df['original_language'] = pd.Series(['en'] * df.shape[0])
df['offer_type'] = df['offer_type'].apply(lambda x: 'Free' if x == False else 'TVE')
df['series_title'] = df['series_title'].apply(lambda x: np.float64(np.nan) if (pd.isnull(x) == True)else (x.title())).apply(lambda x: np.float64(np.nan) if (pd.isnull(x) == True) else (x.replace("-", " ")))
df = df.drop(['type','description','poster','slug'], axis=1)


# imdb data for some extra columns
imdb = pd.read_csv('/home/lisi/Downloads/title.akas.tsv',sep='\t')
basics = pd.read_csv('/home/lisi/Downloads/title.basics.tsv',sep='\t')
ratings = pd.read_csv('/home/lisi/Downloads/title.ratings.tsv',sep='\t')


# add imdb id to dataframe
im = dict(zip(imdb.title, imdb.titleId))
att = []
for i in range(len(df)):
    try:
        att.append(im[df.series_title[i]])
    except:
        att.append(np.nan)

df['imdb_id'] = att


# add imdb genre to dataframe
im = dict(zip(basics.tconst, basics.genres))
att = []
for i in range(len(df)):
    try: 
        att.append(im[df.imdb_id[i]]) #change to imdb_id
    except:
        att.append(np.nan)

df['genre'] = att


# add imdb ratings to dataframe
im = dict(zip(ratings.tconst, ratings.averageRating))
att = []
for i in range(len(df)):
    try: 
        att.append(im[df.imdb_id[i]]) #change to imdb_id
    except:
        att.append(np.nan)

df['imdb_rating'] = att

# reorder the columns
cols_to_order = ['bot_system', 'bot_version', 'bot_country','capture_date', 'is_movie',
                 'offer_type', 'program_key', 'series_key', 'series_title', 'season_number', 
                 'episode_number', 'program_title', 'provider_release_date', 'provider_cease_date', 
                 'release_date', 'viewable_runtime', 'maturity_rating','original_language', 
                 'series_url', 'program_url', 'series_source_id', 'source_id'] # reordering

new_columns = cols_to_order + (df.columns.drop(cols_to_order).tolist())
df = df[new_columns]
df = df.drop_duplicates().reset_index(drop=True)


# save dataframe
df.to_csv('/home/lisi/Desktop/AdultSwim/final_product.csv', index=False)
