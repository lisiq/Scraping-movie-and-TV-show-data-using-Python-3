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

merged_df['capture_date'] = pd.Series([pd.to_datetime('19/04/2020')] * merged_df.shape[0]) #adding the date that the data got scraped
merged_df['bot_version'] = pd.Series(['1.0.0'] * merged_df.shape[0]) #adding bot version

cols_to_order = ['bot_system', 'bot_version', 'bot_country', 'capture_date', 'offer_type'] #reordering first 5 columns
new_columns = cols_to_order + (merged_df.columns.drop(cols_to_order).tolist())
merged_df = merged_df[new_columns]

merged_df.to_csv('/home/lisi/MediaBiz/finalproduct.csv', index=False)


