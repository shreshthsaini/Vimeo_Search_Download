import os 
import glob 
import numpy as np 
import pandas as pd
import statistics 

from pymediainfo import MediaInfo as mi
import ffmpeg as fpeg
import ffprobe as fprobe

from matplotlib import pyplot as plt 
import seaborn as sns

import subprocess
import json 

#helper function 
def get_number(s):
    try:
        return int(s.split('.')[0].split("_")[-1])
    except ValueError:
        return None

# all collected videos and raw keywords
base_path = "/media/ss223464/Expansion/Shreshth_LIVE/Datasets/YT-HDR/"
YT8M_10K_keywords = "CC_HDR_under4_video_#shorts_10k_wordlist_8m_entities.csv"
urbandict2dot5_keywords = "CC_HDR_under4_video_#shorts_urbandict-2dot5M.csv"
raw_vid_folders = ["HDR_#Shorts_YT_keywords_8M_10kwords/", "HDR_#shorts_YT_urbandict-2dot5M/"]

#listing all unique downloaded videos
raw_vids_8M_10K = os.listdir(base_path+raw_vid_folders[0])
raw_vids_urbandict2dot5 = os.listdir(base_path+raw_vid_folders[1])
raw_vids_all = raw_vids_8M_10K+raw_vids_urbandict2dot5
#counting total videos 
count_8M_10K = len(raw_vids_8M_10K)
count_urbandict2dot5 = len(raw_vids_urbandict2dot5)
count_total_vids = count_8M_10K + count_urbandict2dot5
print("Total Video in 8M_10K: {}; Total Video in urbandict2dot5: {}; Total Video: {}.".format(count_8M_10K, count_urbandict2dot5, count_total_vids))

#checking primaries and video shape

#Directly count
def count_stats(address, dataset_vid):
    color_bt2020 = 0
    vertical = 0
    for i in dataset_vid:
        output = mi.parse(address+i)
        for track in output.tracks:
            if track.track_type == 'Video':
                output = track.color_primaries        
                if output == "BT.2020": 
                    color_bt2020 += 1
                widht = track.width
                height = track.height
                if widht < height: 
                    vertical += 1
    return color_bt2020, vertical

#return stats for each file
def check_stats(address, dataset_vid):
    color_primaries = []
    shape_HxW = []
    for i in dataset_vid:
        output = mi.parse(address+i)
        for track in output.tracks:
            if track.track_type == 'Video':
                output = track.color_primaries        
                color_primaries.append(output)
                width = track.width
                height = track.height
                shape_HxW.append([height,width])
    return color_primaries, shape_HxW

#updated list  
raw_vids_8M_10K = [i for i in raw_vids_8M_10K if i.split('.')[-1] == 'webm' or i.split('.')[-1] == 'mp4']
raw_vids_urbandict2dot5 = [i for i in raw_vids_urbandict2dot5 if i.split('.')[-1] == 'webm' or i.split('.')[-1] == 'mp4']

#get the array for color prim and HxW
if False:        
    color_prim_8M_10K, HXW_8M_10K = check_stats(base_path+raw_vid_folders[0], raw_vids_8M_10K)
    color_prim_urbandict, HXW_urbandict = check_stats(base_path+raw_vid_folders[1], raw_vids_urbandict2dot5)


    #get the YT Video ID (split name with '_' and take [0]) - verify. Might be the case where actual name/ID includes '_'!
    all_names = raw_vids_8M_10K+raw_vids_urbandict2dot5 
    all_ids = ["_".join(i.split('.')[0].split("_")[:-1]) for i in all_names]

    #Gettting relevant columns
    base_path_ids = [base_path+raw_vid_folders[0]]*len(raw_vids_8M_10K) + [base_path+raw_vid_folders[1]]*len(raw_vids_urbandict2dot5)
    color_prims = color_prim_8M_10K + color_prim_urbandict
    Heights = [i[0] for i in HXW_8M_10K] + [i[0] for i in HXW_urbandict]
    Widths = [i[1] for i in HXW_8M_10K] + [i[1] for i in HXW_urbandict]
    start_time = [0]*len(base_path_ids)
    end_time = [get_number(i.split('.')[0].split("_")[-1]) for i in all_names]
    #may add more columns based on features/Info needed

    # Creating dataframe to store all.
    df = pd.DataFrame(
        {
            "ID":all_ids,
            "Name(+Duration)":all_names,
            "Base_Path":base_path_ids,
            "color_prims":color_prims,
            "Height":Heights,
            "Width":Widths,
            "start_time": start_time,
            "end_time": end_time
        }
    )

    #keep raw 
    df_raw = df.copy()

    #clean the dataframe 
    #clean: remove all None duration videos and save them as separate csv. These are the video which i have downloaded but removed from YT. 
    df_removed_shorts = df[df['Name(+Duration)'].str.contains('_None')]
    df_removed_shorts.to_csv("Analyzed_HDR_Shorts_list_YT_Removed.csv", index=False)
    if True: 
        df_removed_shorts.to_excel("Analyzed_HDR_Shorts_list_YT_Removed.xlsx", index=False)

    df = df[~df['Name(+Duration)'].str.contains('_None')]
    df.to_csv("Analyzed_HDR_Shorts_list.csv")
    if True:
        df.to_excel("Analyzed_HDR_Shorts_list.xlsx", index=False)

    # clean and drop duplicates // There are no duplicates but to be on the safe side.
    df = df.drop_duplicates(subset=['ID'])

    #filtering out the videos 
    # HDR videos    
    df_hdr = df[df['color_prims']=='BT.2020']
    df_hdr.to_csv("True_HDR_Shorts_list.csv")
    if True:
        df_hdr.to_excel("True_HDR_Shorts_list.xlsx", index=False)

    # vertical videos
    df_vertical = df[df['Height'] > df['Width']]
    df_vertical.to_csv("True_Vertical_Shorts_list.csv")
    if True:
        df_vertical.to_excel("True_Vertical_Shorts_list.xlsx", index=False)

    # True HDR vertical videos
    df_shorts = df_hdr[df_hdr['Height'] > df_hdr['Width']]
    df_shorts.to_csv("True_HDR_Vertical_Shorts_list.csv")
    if True:
        df_shorts.to_excel("True_HDR_Vertical_Shorts_list.xlsx", index=False)

    #making batches for true_hdr
    df_hdr_vertical = pd.read_csv("True_HDR_Vertical_Shorts_list.csv")
    for i in range(len(df_hdr_vertical)//500): 
        df_hdr_vertical[i*500:(i+1)*500].to_csv("True_HDR_Vertical_Shorts_Batch_{}.csv".format(i+1), index=False)
        if True:
            df_hdr_vertical[i*500:(i+1)*500].to_excel("True_HDR_Vertical_Shorts_Batch_{}.xlsx".format(i+1), index=False)


"""
analyzing the average play time and estimations for total volume of data after clipping/trimming. 

"""
if False:
    # Only for True HDR Short videos
    df_hdr_vertical = pd.read_csv("csv_files/True_HDR_Vertical_Shorts_list.csv")
    run_times = df_hdr_vertical["end_time"].to_list()

    min_runtime, max_runtime, median_runtime, avg_runtime = np.min(run_times), np.max(run_times), statistics.median(run_times), np.mean(run_times)
    print("Run times for all True HDR Short Videos. Min: {}, Max: {}, Median: {}, Mean: {},".format(min_runtime, max_runtime, median_runtime, avg_runtime))

    # Create a column which shows the number of clips which can be created from raw based on trimming/clipping.
    # Assume each short clip run time to be: 7-10 secs. 
    clip_duration = 10 #in secs
    total_videos = sum([i//clip_duration +1 for i in run_times])
    print("Original Video Count:{}".format(len(run_times)))
    print("Clipped Video Count:{}".format(total_videos))


    #plot histogram of runtime data show these statistics in the figure. 
    # Create subplots
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(8, 8))

    # Plotting the histogram
    ax1.hist(run_times, bins=200, edgecolor='black', alpha=0.5)
    ax1.set_xlabel('Run times')
    ax1.set_ylabel('Frequency')
    ax1.set_title('Histogram of Run times for True HDR Short Videos')

    # Plotting the KDE plot
    sns.kdeplot(run_times, ax=ax2, color='blue', fill=True)
    ax2.set_xlabel('Run times')
    ax2.set_ylabel('Density')
    ax2.set_title('KDE Plot of Run times for True HDR Short Videos')

    # Increase x-axis markings on the KDE plot
    x_ticks = np.linspace(min(run_times), max(run_times), num=10)
    ax2.set_xticks(x_ticks)

    # Adjust spacing between subplots
    plt.subplots_adjust(hspace=0.4)

    # Adding statistics as text in the figure
    plt.text(0.7, 0.9, f"Min: {min(run_times)}", transform=plt.gca().transAxes)
    plt.text(0.7, 0.85, f"Max: {max(run_times)}", transform=plt.gca().transAxes)
    plt.text(0.7, 0.8, f"Median: {sorted(run_times)[len(run_times)//2]}", transform=plt.gca().transAxes)
    plt.text(0.7, 0.75, f"Mean: {sum(run_times)/len(run_times)}", transform=plt.gca().transAxes)

    # Display the figure
    #plt.show()
    plt.savefig("Average_Runtime_Dist.png")


"""
Sanity check for all True HDR Shorts videos 

"""
# collect all the video meta data and information 
# Simply all Key values in the csv  

#!mediainfo '/media/ss223464/Expansion/Shreshth_LIVE/Datasets/YT-HDR/HDR_#Shorts_YT_keywords_8M_10kwords/xvi3bEmCCDs_41.webm'
    
# You can now use the 'info_dict' variable to access the information in a structured way
def get_info_dict(file_name):
        
    ''' Typical Output
    General
    Complete name                            : /media/ss223464/Expansion/Shreshth_LIVE/Datasets/YT-HDR/HDR_#Shorts_YT_keywords_8M_10kwords/xvi3bEmCCDs_41.webm
    Format                                   : WebM
    Format version                           : Version 4
    File size                                : 107 MiB
    Duration                                 : 40 s 833 ms
    Overall bit rate                         : 22.0 Mb/s
    Writing application                      : google/video-file
    Writing library                          : google/video-file

    Video
    ID                                       : 1
    Format                                   : VP9
    HDR format                               : SMPTE ST 2086, HDR10 compatible
    Codec ID                                 : V_VP9
    Duration                                 : 40 s 833 ms
    Bit rate                                 : 21.1 Mb/s
    Width                                    : 2 160 pixels
    Height                                   : 3 840 pixels
    Display aspect ratio                     : 0.562
    Frame rate mode                          : Constant
    Frame rate                               : 24.000 FPS
    Color space                              : YUV
    Bits/(Pixel*Frame)                       : 0.106
    Stream size                              : 103 MiB (96%)
    Language                                 : English
    Default                                  : Yes
    Forced                                   : No
    Color range                              : Limited
    Color primaries                          : BT.2020
    Transfer characteristics                 : HLG
    Matrix coefficients                      : BT.2020 non-constant
    Mastering display color primaries        : Display P3
    Mastering display luminance              : min: 0.0050 cd/m2, max: 1000 cd/m2
    '''

    #"mediainfo /media/ss223464/Expansion/Shreshth_LIVE/Datasets/YT-HDR/HDR_#Shorts_YT_keywords_8M_10kwords/xvi3bEmCCDs_41.webm"
    output = subprocess.check_output("mediainfo "+file_name, shell=True).decode()

    info_dict = {}
    current_section = None

    lines = output.split('\n')
    for line in lines:
        line = line.strip()
        if line:
            if ':' not in line:
                section = line[:]
                info_dict[section] = {}
                current_section = section
            elif ':' in line:
                key, value = line.split(':', 1)
                info_dict[current_section][key.strip()] = value.strip()

    #Remove un-necessary keys 
    """
    'Complete name' in General section
    'ID' in video section 
    'Height' in video section 
    'Width' in video section 
    """
    info_dict['General'].pop('Complete name')
    info_dict['Video'].pop('ID')
    info_dict['Video'].pop('Width')
    info_dict['Video'].pop('Height')

    return info_dict

temp_info_keys = [
    "Format"
    "Format version"
    "File size"
    "Duration"
    "Overall bit rate"
    "Writing application"
    "Writing library"
    "Format"
    "HDR format"
    "Codec ID"
    "Duration"
    "Bit rate"
    "Width"
    "Height"
    "Display aspect ratio"
    "Frame rate mode"
    "Frame rate"
    "Color space"
    "Bits/(Pixel*Frame)"
    "Stream size"
    "Language"
    "Default"
    "Forced"
    "Color range"
    "Color primaries"
    "Transfer characteristics"
    "Matrix coefficients"
    "Mastering display color primaries"
    "Mastering display luminance"
    ]

def update_csv_info(df_csv):
    list_info_dict = []
    for i,name in enumerate(df_csv["Name(+Duration)"].to_list()):
        file_name = df_csv["Base_Path"][i]+name
        info_dict = get_info_dict(file_name)
        #merge the "General" and "Video" sections
        list_info_dict.append({**info_dict["General"], **info_dict["Video"]})

    #Add keys as new columns in the existing dataframe.    
    for i in temp_info_keys:
        df_csv[i] = ''

    #add the values from info dict 
    for i,info_dict in enumerate(list_info_dict):
        df_csv.loc[i, list(info_dict.keys())] = list(info_dict.values())

    return df_csv

# For example, print the complete name and video format
#list of all csv: 
if True:
    list_csv = glob.glob("csv_files/*.csv")

    #check if the video is actuall HDR
    for i in list_csv:
        df_temp = pd.read_csv(i)

        if False:
            file_name = df_temp["Base_Path"][0]+df_temp["Name(+Duration)"][0]

            info_dict = get_info_dict(file_name)

            print(info_dict["Video"]["Format"])
            print(info_dict["Video"]["Codec ID"])

        #update csv and save
        df_temp = update_csv_info(df_temp)
        df_temp.to_csv(i)

