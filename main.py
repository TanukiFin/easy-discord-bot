import pandas as pd
import datetime
import time
import tweepy
import random 
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe

# read google sheet
credentials = ServiceAccountCredentials.from_json_keyfile_name('YOUR_JSON_KEYFILE_NAME', 'https://spreadsheets.google.com/feeds')
gss_client = gspread.authorize(credentials)
spreadsheet_key = 'YOUR_SPREADSHEET_KEY' 
kol_list_sheet = gss_client.open_by_key( spreadsheet_key ).worksheet('kol_list')
df_kol = pd.DataFrame(kol_list_sheet.get_all_values()[1:], columns=kol_list_sheet.get_all_values()[0])

# connect twitter
twitter1 = tweepy.Client(bearer_token=
             "YOUR_BEARER_TOKEN")
twitter2 = tweepy.Client(bearer_token=
             "YOUR_BEARER_TOKEN")
twitter3 = tweepy.Client(bearer_token=
             "YOUR_BEARER_TOKEN")
twitter_list = [twitter1,twitter2,twitter3]

# main 
list_new_kol = [] 
list_new_following = []
for kol_index in range( len(df_kol['KOL']) ):
  try:
    # handle 取得 ID
    username = df_kol['KOL'].loc[kol_index]
    print(kol_index, username)
    user_id = twitter_list[random.randint(0, len(twitter_list)-1)].get_user(username=username, user_fields=["public_metrics"])[0].id
    data = twitter_list[random.randint(0, len(twitter_list)-1)].get_users_following(user_id, max_results = 10).data
    following_list = [ username ]
    for i in range( len(data) ):
      following_list.append( data[i].username )
    list_new_kol.append(following_list)
    time.sleep(5)

    # 找出new following
    old_following = df_kol.loc[kol_index].tolist()
    intersection = set( old_following ) & set( following_list )
    new_following = set( following_list ) - intersection
    if len(new_following) > 0:
      for i in range( len(new_following) ):
        list_new_following.append( [ username, list(new_following)[i], str(datetime.datetime.now())[0:16] ] )

  except:
    print('ERROR: sleep 30 sec...')
    kol_index = kol_index-1
    time.sleep(30)
    pass
df_new_kol = pd.DataFrame(list_new_kol)
print('find', len(list_new_following), 'new following')

# save
df_new_kol = pd.DataFrame(list_new_kol)
set_with_dataframe(kol_list_sheet, df_new_kol, row = 2, col = 1, include_index = False, include_column_header = False)
kol_list_sheet.update_cell(1, 12, str(datetime.datetime.now())[0:16] )

# update new following
kol_new_following_sheet = gss_client.open_by_key( spreadsheet_key ).worksheet('kol_new_following')
df_new_following = pd.DataFrame( list_new_following )
set_with_dataframe(kol_new_following_sheet, df_new_following, row = len( kol_new_following_sheet.get_all_values() )+1  \
          , col = 1, include_index = False, include_column_header = False)

print('\n=== Completed successfully ===')
