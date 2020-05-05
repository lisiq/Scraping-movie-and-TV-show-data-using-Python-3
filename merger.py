import pandas as pd
import numpy as np
import re
import json

# Load the data
df_tbsmovies =pd.read_csv('/home/lisi/MediaBiz/TBSmovies/tbsmovies.csv')
df_tntdramamovies= pd.read_csv('/home/lisi/MediaBiz/TNTDRAMAmovies/tntdramamovies.csv')
df_tbsshows= pd.read_csv('/home/lisi/MediaBiz/TBS/tbsshows.csv')
df_tntdramashows= pd.read_csv('/home/lisi/MediaBiz/TNTDRAMA/tntdramashows.csv')
df_trutvshows= pd.read_csv('/home/lisi/MediaBiz/TRUTV/trutvshows.csv')
df_nba= pd.read_csv('/home/lisi/MediaBiz/TNTDRAMA/tntdramanba.csv')

# Merging all together
all_df = [df_tbsmovies, 
            df_tntdramamovies,
             df_tbsshows, 
             df_tntdramashows, 
             df_trutvshows,
             df_nba]

merged_df = pd.concat(all_df, join='outer', axis=0).drop_duplicates().reset_index(drop=True)

merged_df['capture_date'] = pd.Series([pd.to_datetime('04/19/2020')] * merged_df.shape[0]).dt.strftime('%m/%d/%Y') # adding the date that the data got scraped
merged_df['bot_version'] = pd.Series(['1.0.0'] * merged_df.shape[0]) # adding bot version

merged_df = merged_df.drop(columns=['url'], axis = 1) # dropping column that I won't need anymore

merged_df['program_url'] = merged_df['program_url'].str.replace('\\','') # making it more pleasant to read

merged_df['episode_url'] = merged_df['episode_url'].str.replace('\\','') # making it more pleasant to read

merged_df['series_url'] = merged_df['series_url'].str.replace('\\','') # making it more pleasant to read

merged_df = merged_df[merged_df['is_movie'] != 'clip'].reset_index(drop=True) # removing clips from dataframe

merged_df['provider_release_date']= pd.to_datetime(merged_df['provider_release_date']).dt.strftime('%m/%d/%Y') # transforming the column to datetime

merged_df['provider_cease_date']= pd.to_datetime(merged_df['provider_cease_date']).dt.strftime('%m/%d/%Y') # transforming the column to datetime

merged_df['viewable_runtime'] = merged_df['viewable_runtime'].apply(lambda x: np.float64(np.nan) if (pd.isnull(x) == True)
                                                                    else (np.int64(x))).astype('Int64') # transforming the values into integers

merged_df['season_number'] = merged_df['season_number'].astype('Int64') # transforming the values into integers

merged_df['episode_number'] = merged_df['episode_number'].astype('Int64') # transforming the values into integers

merged_df['bot_system'] = merged_df['bot_system'].apply(lambda x: np.nan if(pd.isnull(x) == True) else x.lower()) # transforming the values into lower cases


merged_df['provider_original'] = merged_df['provider_original'].apply(lambda x: 
                               np.nan if(pd.isnull(x) == True) 
                               else (1  if (bool(re.search('Original', x))==True)  
                                     else 0)).astype('Int64')       # make 0 or 1 if not original or original

merged_df['series_source_id'] = merged_df['series_source_id'].astype('Int64') # to int


#merged_df['source_id'] = merged_df['source_id'].astype('Int64') # to int

merged_df['is_movie'] = merged_df['is_movie'].apply(lambda x: 1 if (x == 'movie') else 0) # transforming from str to int

merged_df['program_title'] = merged_df['program_title'].fillna(merged_df['episodes_title']) # adding all titles in one column

merged_df['program_url'] = merged_df['program_url'].fillna(merged_df['episode_url']) # adding all urls in one column

merged_df = merged_df.drop(columns=['episode_url', 'episodes_title'], axis = 1) # dropping column that I won't need anymore



cols_to_order = ['bot_system', 'bot_version', 'bot_country', 'capture_date', 'is_movie',
                 'offer_type', 'series_title', 'season_number', 'episode_number', 
                 'program_title', 'provider_release_date', 'provider_cease_date', 
                 'viewable_runtime', 'maturity_rating','original_language', 
                 'series_url', 'program_url', 'series_source_id', 'source_id', 
                 'provider_original'] # reordering

new_columns = cols_to_order + (merged_df.columns.drop(cols_to_order).tolist())
merged_df = merged_df[new_columns]

# some added remarks that were pointed out to me will be resolved in the following lines before svaing the csv
# json file was missing some data for one instance so I will fill some of the rows manually
null = 0
for i in range(len(merged_df)):
    if pd.isnull(merged_df['bot_system'].iloc[i]):
        null = i

merged_df.iloc[null,0] = 'trutv'
merged_df.iloc[null,2] = 'US'
merged_df.iloc[null,5] = 'TVE'
merged_df.iloc[null,6] = 'Adam Ruins Everything'
merged_df.iloc[null,7] = np.int64(2)
merged_df.iloc[null,8] = np.int64(4)
merged_df.iloc[null,10] = pd.to_datetime('04/02/2020').strftime('%m/%d/%Y')
merged_df.iloc[null,11] = pd.to_datetime('05/01/2020').strftime('%m/%d/%Y')
merged_df.iloc[null, 14] = 'en'
merged_df.iloc[null, 15] = 'https://www.trutv.com/shows/adam-ruins-everything'
merged_df.iloc[null, 16] = 'http://www.trutv.com/shows/adam-ruins-everything/season-2/episode-4/adam-ruins-dating'
merged_df.iloc[null, -1] = np.int64(0)

# changin capture date to Shaq's life
for i in range(len(merged_df)):
    if merged_df.iloc[i,6] == 'Shaq Life':
        merged_df.iloc[i,3] = pd.to_datetime('05/01/2020').strftime('%m/%d/%Y')

# changin capture date to NBA
for i in range(len(merged_df)):
    if merged_df.iloc[i,6] == 'NBA on TNT 19-20':
        merged_df.iloc[i,3] = pd.to_datetime('05/04/2020').strftime('%m/%d/%Y')


merged_df.to_csv('/home/lisi/MediaBiz/finalproduct.csv', index=False)


