import pandas as pd
import json

# Load the data
df_tbsmovies =pd.read_csv('/home/lisi/MediaBiz/TBSmovies/tbsmovies.csv')
df_tntdramamovies= pd.read_csv('/home/lisi/MediaBiz/TNTDRAMAmovies/tntdramamovies.csv')
df_tbsshows= pd.read_csv('/home/lisi/MediaBiz/TBS/tbsshows.csv')
df_tntdramashows= pd.read_csv('/home/lisi/MediaBiz/TNTDRAMA/tntdramashows.csv')
df_trutvshows= pd.read_csv('/home/lisi/MediaBiz/TRUTV/trutvshows.csv')

# Merging all together
all_df = [df_tbsmovies, 
            df_tntdramamovies,
             df_tbsshows, 
             df_tntdramashows, 
             df_trutvshows]

merged_df = pd.concat(all_df, join='outer', axis=0).drop_duplicates().reset_index(drop=True)

merged_df['capture_date'] = pd.Series([pd.to_datetime('19/04/2020')] * merged_df.shape[0]) # adding the date that the data got scraped
merged_df['bot_version'] = pd.Series(['1.0.0'] * merged_df.shape[0]) # adding bot version

merged_df = merged_df.drop(columns=['episode_url', 'url', 'episodes_title'], axis = 1) # dropping two columns that I won't need anymore

merged_df['program_url'] = merged_df['program_url'].str.replace('\\','') # making it more pleasant to read

merged_df['series_url'] = merged_df['series_url'].str.replace('\\','') # making it more pleasant to read

merged_df = merged_df[merged_df['is_movie'] != 'clip'].reset_index(drop=True) # removing clips from dataframe

merged_df['provider_release_date']= pd.to_datetime(merged_df['provider_release_date']).dt.date # transforming the column to datetime

merged_df['provider_cease_date']= pd.to_datetime(merged_df['provider_cease_date']).dt.date # transforming the column to datetime

merged_df['viewable_runtime'] = merged_df['viewable_runtime'].apply(lambda x: int(x)) # transforming the values into integers

merged_df['is_movie'] = merged_df['is_movie'].apply(lambda x: 1 if (x == 'movie') else 0) # transforming from str to int

cols_to_order = ['bot_system', 'bot_version', 'bot_country', 'program_key', 'offer_type', 
                    'capture_date', 'provider_release_date', 'provider_cease_date', 'series_key',
                    'program_title', 'season_number', 'episode_number', 'is_movie', 'viewable_runtime',
                    'maturity_rating', 'original_language', 'program_url', 'series_title',
                    'series_url'] # reordering

new_columns = cols_to_order + (merged_df.columns.drop(cols_to_order).tolist())
merged_df = merged_df[new_columns]

merged_df.to_csv('/home/lisi/MediaBiz/finalproduct.csv', index=False)


