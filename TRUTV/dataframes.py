import pandas as pd
import json
import re

with open('/home/lisi/MediaBiz/TRUTV/episodes.txt', 'r') as file:
    lines = [line.strip() for line in file]


# we open the json files and make the syntax python friendly
# after that we transofrm them into python dictionaries
# and save them in a list. There can be some json files 
# that do not have the info that we need so we ignore them
dict_list = []
for i in lines:
    try:
        with open('/home/lisi/MediaBiz/TRUTV/TRUTVshows_ldjson/' + i.encode("utf-8").hex() + '.json', 'r') as j:
            dict_list.append(dict(eval(json.load(j).replace('null', '"NaN"')\
                                           .replace('true',"True")\
                                           .replace('false',"False"))))
    except SyntaxError:
        pass
        

# # we define this helper function to get only the data 
# # that we need form the dictionaries
def get_info(n):
    m = n['potentialAction'][0]
    url = "\\/" + "/".join(n['@id'].split("/", 4)[3:])
    url_original = n['url']
    series_title =  n['partOfSeries']['name']
    series_url = n['partOfSeries']['@id']
    episode_number = n['episodeNumber']
    partof_season = n['partOfSeason']['seasonNumber']
    language = n['@context'][1]['@language']
    bot_country = m['actionAccessibilityRequirement']['eligibleRegion']['name']
    availabilityStarts = m['actionAccessibilityRequirement']['availabilityStarts']
    availabilityEnds = m['actionAccessibilityRequirement']['availabilityEnds']
    bot_system = re.findall(r'www.(.*?).com',url_original)[0].upper()
    if m['actionAccessibilityRequirement']['category'] == 'nologinrequired':
        offer_type = 'FREE'
    else:
        offer_type = m['actionAccessibilityRequirement']['requiresSubscription']['authenticator']['name']
    return [url, language, bot_country, availabilityStarts, 
            availabilityEnds, bot_system, offer_type, url_original, series_title,
           series_url, episode_number, partof_season]


# # we apply the helper function to all the dictionaries 
# # and ignore the files that do not have the data that we need
flat_list = []
for i in dict_list:
    try:
        flat_list.append(get_info(i))
    except KeyError:
        pass

# # here we create a data fram from the list overhead and name the columns
trutvshows_ldjson = pd.DataFrame(flat_list, columns=['url', 'language', 'bot_country', 
                                                   'availabilityStarts', 'availabilityEnds', 
                                                   'bot_system', 'offer_type', 'episode_url',
                                                   'series_title', 'series_url', 'episode', 'season'])


# finally we save the data as a csv file
#trutvshows_ldjson.to_csv('/home/lisi/MediaBiz/TRUTV/trutvshows_ldjson.csv', index=False)

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


# we open the files transform them into dictionaries, make the syntax python friendly
# and ignore the files that do not have the data that we need
flat_list = []
dict_list = []
for i in new_lines:
    try:
        with open('/home/lisi/MediaBiz/TRUTV/TRUTVshows_drupla-settings-json/' + i.encode("utf-8").hex() +'.json', 'r') as j:
            dict_list.append(dict(eval(json.load(j).replace('null', '"NaN"')\
                                       .replace('true',"True")\
                                       .replace('false',"False")))['turner_playlist'])
    except KeyError:
        pass

# transform from a nested list into a list      
flat_list = [item for sublist in dict_list for item in sublist]

# # create a dataframe
to_keep = ['tvRating', 'title', 'url', 'duration', 'videoType', 'seriesLogoImage', 'seriesTitleId','videoId']
trutvshows_drupla_settings_json = pd.DataFrame(flat_list)[to_keep]

# # finally save it
# trutvshows_drupla_settings_json.to_csv('/home/lisi/MediaBiz/TRUTV/trutvshows_drupla_settings_json.csv', index=False)

# # we combine the two datasets in one
df_trutvshows = pd.merge(trutvshows_drupla_settings_json, trutvshows_ldjson, how='left', on='url')

# # save the merger
#df_trutvshows.to_csv('/home/lisi/MediaBiz/TRUTV/trutvshows.csv', index=False)

df_trutvshows.set_axis(['maturity_rating', 'episodes_title', 'url', 
                    'viewable_runtime', 'is_movie', 'provider_original', 'series_source_id', 'source_id', 'original_language',
                    'bot_country', 'provider_release_date', 'provider_cease_date',
                    'bot_system', 'offer_type', 'episode_url', 'series_title',
                    'series_url', 'episode_number', 'season_number'
                  ],axis=1,inplace=True)

# a dictionaie of originals and non originals
trutv_originals = df_trutvshows[df_trutvshows['provider_original'].\
                  notnull()][['series_title','provider_original']].\
                reset_index(drop=True).\
                set_index('series_title').\
                T.to_dict('list')

# fill the empty originals column
for i in range(len(df_trutvshows)):
    try:
        if pd.isnull(df_trutvshows['provider_original'].iloc[i]) == True:
            df_trutvshows['provider_original'].iloc[i] = trutv_originals[df_trutvshows['series_title'].iloc[i]][0]
    except KeyError:
        pass


# # save the merger
df_trutvshows.to_csv('/home/lisi/MediaBiz/TRUTV/trutvshows.csv', index=False)