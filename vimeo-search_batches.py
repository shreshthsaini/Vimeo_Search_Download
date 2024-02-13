'''
    Vimeo search using vimeo_search_python library
    - search_vimeo(csv_file, extra_keyword, batch_size, limit, filter_criterion, searched_csv_root='./searched_csv/')
    - verify_merge(df_temp, df, df_prev, keyword)
    - main()
    
    - Shreshth Saini, 2021
    * Structure is taken from YT search code; refere to: https://github.com/shreshthsaini/YT_Search_Down

'''

import os
import sys
import pandas as pd
import numpy as np
from tqdm import tqdm
import argparse

from censor_word import censor_words
from vimeo_search_python.vimeo_search import VimeoSearch
import warnings
warnings.filterwarnings('ignore')

#--------------------------------------------------------------*****--------------------------------------------------------------#
def verify_merge(df_temp, df, df_prev, keyword):
    '''
        Check if the IDs are unique or not before merging
        NOTE: Checking previous search results.

        Args:
        - df_temp (dataframe): dataframe of search results
        - df (dataframe): dataframe of search results
        - df_prev (list): list of IDs of previously collected data/csv
        - keyword (str): keyword used for search
    ''' 
    # now compare the IDs are unique or not
    for k in range(len(df_temp)):
        if df_temp['id'][k] in df['id'].values:
            print('ID already exists: ',df_temp['id'][k])
            df_temp.drop(k, inplace=True)
        # remove is exists in previously collected data/csv
        elif len(df_temp)!=0:
            if df_temp['id'][k] in df_prev:
                print('ID already exists in previous csv: ',df_temp['id'][k])
                df_temp.drop(k, inplace=True)    

    return df_temp

def search_vimeo(csv_file, extra_keyword, batch_size, filter_criterion, searched_csv_root='./searched_csv/'):
    # Create directory for saving csv file batches
    keyword_batch_root = searched_csv_root+csv_file.split("/")[-1].split(".")[0]
    print("Keywords Batch Files Directory: ", keyword_batch_root)
    os.makedirs(keyword_batch_root, exist_ok=True)

    # Read keywords from csv file
    keywords = list(pd.read_csv(csv_file, on_bad_lines='skip')['keyword'].values)
    # print keywords length
    print('Total Keywords length: ', len(keywords))
    
    # check if the cleaned version of the keywords already exists 
    if os.path.exists(keyword_batch_root+'/cleaned_keywords.csv'):
        keywords = list(pd.read_csv(keyword_batch_root+'/cleaned_keywords.csv').iloc[:,0].values)
        print('Cleaned Keywords length: ', len(keywords))
    else:
        # cleaning the keywords list
        print('Cleaning the keywords list')
        keywords = censor_words(keywords).censor_words()
        # print keywords length
        print('Cleaned Keywords length: ', len(keywords))
        # saving the cleaned keywords list 
        pd.DataFrame(keywords).to_csv(keyword_batch_root+'/cleaned_keywords.csv', index=False)

    ''' 
        Checking the previous generated csv files and remove searches if ID already exists
        NOTE: Checking previous search results. 
    '''
    df_prev = []
    for i in os.listdir(searched_csv_root):
        if os.path.isfile(searched_csv_root+i):
            df_prev += pd.read_csv(searched_csv_root+i)['id'].values.tolist()

    ''' 
        NOTE: Divide keywords in batches and save the csv file after each batch to avoid losing collected data due to long run time or system stalls
    '''
    for b in tqdm(range(0,len(keywords),batch_size)):
        
        print('Current Batch: ', b/batch_size)
        batch_keywords = keywords[b:b+batch_size]

        # check if the batch csv file already exists 
        if os.path.exists(keyword_batch_root+'/batch_'+ str(int(b/batch_size))+'.csv'):
            print('Batch csv file already exists: ',keyword_batch_root+'/batch_'+ str(int(b/batch_size))+'.csv')
            print('Checking if search results already exists in the csv file....')

            # check if the searched results already exists in the csv file 
            if os.path.exists(searched_csv_root+filter_criterion+"_"+extra_keyword.split(" ")[-1]+"_"+csv_file.split(".")[0].split("/")[-1]+'_batch_'+str(int(b/batch_size))+'.csv'):
                print('Searched results already exists for current batch! Skipping...')
                continue
        else:
            # Save the batch of keywords to csv file; can be used to resume the search
            pd.DataFrame(batch_keywords).to_csv(keyword_batch_root+'/batch_'+ str(int(b/batch_size))+'.csv', index=False)

        # Empty dataframe with columns for storing search results
        df = pd.DataFrame(columns=['url','id','play_time','date','channel','title','license','keyword'])

        # Iterate over each keyword in the batch
        for keyword in batch_keywords:      
            print('Keyword: ',keyword)
            
            ''' 
                Core search function
                return: search results in json format 
            '''
            try: 
                # Initialize search object with keyword and filter criterion 
                # filter_criterion = hdr+price+license+resolution
                searcher = VimeoSearch(base_vimeo_url="https://vimeo.com/search", hdr=filter_criterion.split("+")[0], price=filter_criterion.split("+")[1], license=filter_criterion.split("+")[2], resolution=filter_criterion.split("+")[3])
                
                # search the total pages 
                total_pages = searcher.get_total_pages(keyword)
                if total_pages:
                    print(f"Total pages: {total_pages}")    
                else:
                    print("Could not determine the total number of pages.")
                
                # searches all pages and return single list of dicts; improved version from YT search implement
                # search the pages;  return the list of dicts 
                search_results = searcher.search_vimeo(keyword+extra_keyword)

            except:
                try:
                    print('Error occured: ',search_results.result())
                except:
                    print('Error occured: No results found!')
                    
                print("keyword: ",keyword)
                continue

            # Check if the IDs are unique or not before merging
            df_temp = searcher.results_to_df(search_results) # convert the list of dicts to dataframe
            df_temp = verify_merge(df_temp, df, df_prev, keyword)

            # if dataframe lenght zero skip updating. 
            if len(df_temp) == 0:
                print('Length of the temp dataframe: ',len(df_temp))
                continue

            # print the first title result from df_temp 
            print(df_temp.head(1))

            #appending in the dataframe df 
            df = df.append(df_temp)

        # save the csv if df is not empty
        if len(df) == 0:
            print('Skipping saving; Length of the dataframe: ',len(df))
            continue
        # Saving as csv file after each batch
        df.to_csv(searched_csv_root+filter_criterion+"_"+extra_keyword.split(" ")[-1]+"_"+csv_file.split(".")[0].split("/")[-1]+'_batch_'+str(int(b/batch_size))+'.csv', index=False)
        # print the csv length and first 5 rows
        print('CSV length: ',len(df))
        print(df.head())

#--------------------------------------------------------------*****--------------------------------------------------------------#
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv_file", help="Path to the CSV file")
    parser.add_argument("--extra_keyword", type=str, default="", help="Use this spaced keyword or hastag to impprove the searching. #shorts tend to return YT Shorts") # 
    parser.add_argument("--batch_size", type=int, default=10000, help="Batch size")
    parser.add_argument("--filter_criterion", default="hdr+free+allCC+any", help="Filter criterion, select option and write with +:  hdr: any, hdr, hdr10, dolby_vision; price: any, free, paid; resolution: any, 4k; license: any, by, cc0, by-nd, by-nc, by-sa, by-nc-nd, by-nc-sa, allCC")
    args = parser.parse_args()
    
    search_vimeo(args.csv_file, args.extra_keyword, args.batch_size, args.filter_criterion)

#--------------------------------------------------------------*****--------------------------------------------------------------#
if __name__ == "__main__":
    main()


""""
IMPORTANT: 

For batchwise: check all batches of csv and drop duplicate ids as it was not handled in this batchwise code!!

"""

""" 

Help address - shreshth 

/media/ss223464/Expansion/Shreshth_LIVE/Datasets/YT-HDR/raw_keywords/All_Combined_Keywords.csv

/media/ss223464/Expansion/Shreshth_LIVE/Datasets/YT-HDR/raw_keywords/10k_wordlist.csv

"""