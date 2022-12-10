import requests
import json
from tqdm import tqdm
import os
from lm_dataformat import Reader

class SpeakleashDataset(object):

    def __init__(self, name, url, replicate_dir):
        self.url = url
        self.name = name
        self.replicate_dir = replicate_dir
        self.manifest = self._download_manifest()

    def _download_file(self, file_name):

        ok = True
        url = self.url + self.name + ".jsonl.zst"
        file_path = os.path.join(self.replicate_dir, file_name)

        response = requests.get(url, stream=True)
        total_size_in_bytes = int(response.headers.get('content-length', 0))
        block_size = 1024
        progress_bar = tqdm(total=total_size_in_bytes, unit='iB', unit_scale=True)
        with open(file_path, 'wb') as file:
            for data in response.iter_content(block_size):
                progress_bar.update(len(data))
                file.write(data)
        progress_bar.close()
        if total_size_in_bytes != 0 and progress_bar.n != total_size_in_bytes:
            ok = False

        return ok

    def _download_manifest(self):

        try:
            r = requests.get(self.url + self.name + ".manifest")
            if r.ok:
                return json.loads(r.text)
        except:
            print("Error downloading manifest {0}".format(self.url + self.name + ".manifest"))
            return{}
                
        return {}

    @property
    def characters(self):
        s = self.manifest.get("stats",{}).get("characters",0)
        return s

    @property
    def documents(self):
        s = self.manifest.get("stats",{}).get("documents",0)
        return s

    @property
    def jsonl_zst_file_size(self):
        return self.manifest.get("file_size",0)

    @property
    def data(self):

        if not os.path.exists(self.replicate_dir):
            os.makedirs(self.replicate_dir, exist_ok=True)

        file_name_json_zst = os.path.join(self.name + ".jsonl.zst")
        file_path_json_zst = os.path.join(self.replicate_dir, file_name_json_zst)
        file_json_zst_exists = False

        if os.path.exists(file_path_json_zst):
            file_size = os.path.getsize(file_path_json_zst)
            if file_size == self.jsonl_zst_file_size:
                file_json_zst_exists = True

        if not file_json_zst_exists:
            if not self._download_file(file_name_json_zst):
                return None
        
        rdr = Reader(file_path_json_zst)
        return rdr.stream_data()

    def __repr__(self):
        return "SpeakleashDataset([{0},{1},{2}])".format(self.name, self.url, self.characters)

    def __str__(self):
        return "name: {0}, url: {1}, characters: {2}".format(self.name, self.url, self.characters)
