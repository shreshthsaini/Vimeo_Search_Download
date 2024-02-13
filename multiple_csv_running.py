# for download_vimeo_urls.py, running with multiple csv files in sequence 

#--------------------------------------------------------------*****--------------------------------------------------------------# 
import os 
import subprocess 
import pandas as pd 
import glob

# list all csv files using glob 
csv_files = glob.glob('./searched_csv/*.csv')
#print(csv_files[0])

# run the download script for each csv file 
for csv_file in csv_files:
    print("Downloading videos from csv file: ", csv_file)
    # run the download script 
    os.system('python download_vimeo_urls.py --csv_file '+csv_file+' --save_folder /media/ss223464/Expansion1/Shreshth-2/Datasets/Vimeo/All_Combined_keywords/')