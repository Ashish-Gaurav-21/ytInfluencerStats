import requests
import json
from tqdm import tqdm


class YoutubeStatistics:

    def __init__(self, api_key, video_ids, video_parts):
        self.api_key = api_key
        self.video_ids = video_ids
        self.video_parts = video_parts

    def get_video_data(self):
        num_iterations, lower_limit, upper_limit = len(self.video_ids) // 48 + 1, 0, 48 if len(self.video_ids) > 48 else None # To get subset of video_ids. Since, we can only send request for 48 video_ids at a time.
        num_iterations = 1 if len(self.video_ids) <= 48 else num_iterations
        print(num_iterations, len(self.video_ids))
        for n in tqdm(range(num_iterations)):
            for part in self.video_parts:
                sub_video_ids = self.video_ids[lower_limit: upper_limit]
                request_url = f'https://www.googleapis.com/youtube/v3/videos?key={self.api_key}&part={part}&id={",".join(sub_video_ids)}'
                json_response = self._get_json_response(request_url)
                part_details = dict([(i.get('id', 'n/a'), i.get(part)) for i in json_response.get('items', [{}])]) # video_id: part_details key-value pair
                with open(f'yt_video_{part}_{str(n)}.json', 'w') as f: json.dump(part_details, f, indent=4)
            if upper_limit is None: break
            else: lower_limit, upper_limit = lower_limit + 48, (upper_limit + 48) if len(self.video_ids) > (upper_limit + 48) else None

    def _get_json_response(self, url):
        json_response = None
        response = requests.get(url)
        status_code = response.status_code
        if status_code == 200: json_response = json.loads(response.text)
        else: print('Response Issue!!!')
        return json_response
