import os
import pandas as pd
import json
from youtube_statistics import YoutubeStatistics


API_KEY = 'AIzaSyBuTLrS7TZXFfEVB-EEx_c7B84nqaDOiT4' # Need to fill this
video_parts = ['statistics', 'contentDetails', 'snippet', 'status', 'topicDetails'] # Each part gives separate info about the YT video.

def _get_video_ids():
    yt_video_links_path = (os.getcwd() + '\\yt_video_links_input.csv')
    yt_video_links_df = pd.read_csv(yt_video_links_path)
    yt_video_links_list = yt_video_links_df['Video_Link'].apply(lambda x: [i.replace('v=', '') for i in x.split('?')[-1].split('&') if 'v=' in i][0]).unique().tolist()
    return yt_video_links_list

def _merge_video_part_data(parts):
    master_df = pd.DataFrame(data=_get_video_ids(), columns=['video_id'])
    for part in parts:
        files_to_be_merged = [file for file in os.listdir(os.getcwd()) if file.startswith(f'yt_video_{part}') and file.endswith('.json')]
        sub_master_df = pd.DataFrame()
        for file in files_to_be_merged:
            f = open(file)
            data = json.load(f)
            f.close()
            os.remove(file)
            df = pd.DataFrame(data).transpose().reset_index().rename(columns={'index': 'video_id'})
            sub_master_df = pd.concat([sub_master_df, df], ignore_index=True)
        sub_master_df.to_csv(os.getcwd() + f'\\yt_video_{part}.csv', index=False)
        master_df = master_df.merge(sub_master_df, how='left', on='video_id')
    master_df.to_csv(os.getcwd() + '\\yt_video_links_output.csv', index=False)

if __name__ == '__main__':
    yt = YoutubeStatistics(API_KEY, _get_video_ids(), video_parts)
    yt.get_video_data()
    _merge_video_part_data(video_parts)
