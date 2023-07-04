import requests
import json
import os
import hashlib
import glob
import tempfile
from tqdm import tqdm
from lm_dataformat import Reader
from datetime import datetime

class FileManager:
    @staticmethod
    def ensure_dir_exists(directory):
        os.makedirs(directory, exist_ok=True)

    @staticmethod
    def load_json(file):
        try:
            with open(file, 'r') as f:
                data = json.load(f)
            return data
        except:
            return None

    @staticmethod
    def save_json(data, file):
        try:
            with open(file, 'w') as f:
                json.dump(data, f)
            return True
        except:
            return False

    @staticmethod
    def load_text(file):
        try:
            with open(file, 'r') as f:
                lines = f.readlines()
            return [line.strip() for line in lines]
        except:
            return None

    @staticmethod
    def save_text(lines, file):
        try:
            with open(file, 'w', encoding="utf-8") as f:
                for line in lines:
                    f.write(line + "\n")
            return True
        except:
            return False

class WebRequester:
    session = requests.Session()

    @staticmethod
    def get_json(url):
        try:
            r = WebRequester.session.get(url)
            if r.ok:
                return json.loads(r.text)
        except:
            return None

    @staticmethod
    def get_text(url, encoding='utf-8'):
        try:
            r = WebRequester.session.get(url)
            r.encoding = encoding
            if r.ok:
                return r.text
        except:
            return None

    @staticmethod
    def download_file(url, filepath):
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()  # Raise an exception for HTTP errors
        except requests.exceptions.RequestException as e:
            print(f"An error occurred while making the request: {e}")
            return False
    
        try:
            total_size_in_bytes = int(response.headers.get('content-length', 0))
        except (ValueError, TypeError):
            print("An error occurred while calculating the file size")
            return False
    
        try:
            block_size = 1024
            progress_bar = tqdm(total=total_size_in_bytes, unit='iB', unit_scale=True)
            with open(filepath, 'wb') as file:
                for data in response.iter_content(block_size):
                    progress_bar.update(len(data))
                    file.write(data)
            progress_bar.close()
        except IOError as e:
            print(f"An error occurred while writing the file: {e}")
            return False

        return total_size_in_bytes == progress_bar.n

class StructureDownloader:
    def __init__(self, replicate_dir):
        self.replicate_dir = replicate_dir

    def _remove_old_files(self, url):
        hash = hashlib.md5(url.encode('utf-8')).hexdigest()
        filter = os.path.join(self.replicate_dir, hash + "-*.json")
        for f in glob.glob(filter):
            try:
                os.remove(f)
            except:
                pass

    def get_structure(self, url, hourly=True):
        FileManager.ensure_dir_exists(self.replicate_dir)
        ts = datetime.now().strftime("-%m_%d_%y_%H" if hourly else "-%m_%d_%y")
        hash = hashlib.md5(url.encode('utf-8')).hexdigest()
        file = os.path.join(self.replicate_dir, hash + ts + ".json")

        data = FileManager.load_json(file)
        if data:
            return data

        self._remove_old_files(url)
        data = WebRequester.get_json(url)
        FileManager.save_json(data, file)
        return data

class CategoryManager:
    def __init__(self):
        self.temp_dir = os.path.join(tempfile.gettempdir(), "speakleash")
        FileManager.ensure_dir_exists(self.temp_dir)

        self.categories_pl = self.__categories("pl")
        self.categories_en = self.__categories("en")

    def __categories(self, lang="pl"):
        url = f"https://speakleash.space/datasets_text/categories_{lang}.txt"
        file_path = os.path.join(self.temp_dir, f"{lang}_categories.txt")
        
        categories = FileManager.load_text(file_path)
        if categories:
            return categories

        data = WebRequester.get_text(url)
        categories = data.split("\n")
        FileManager.save_text(categories, file_path)

        return categories

    def categories(self, lang="pl"):
        return self.categories_pl if lang == "pl" else self.categories_en
    
    def __get_pl_category(self, name, lang):
        index = self.categories_en.index(name) if lang == "en" else None
        return self.categories_pl[index] if index is not None else None

    def check_category(self, meta, categories, cf, lang="pl"):
        if not meta or not categories:
            return False

        for category in categories:
            category_pl = self.__get_pl_category(category, lang) if lang != "pl" else category
            if category_pl:
                meta_categories = meta.get("category", {})
                if any(meta_category.upper() == category_pl.upper() and meta_categories[meta_category] >= cf
                       for meta_category in meta_categories):
                    return True
        return False

class Speakleash:
    def __init__(self, replicate_dir, lang="pl"):
        self.replicate_dir = replicate_dir
        self.datasets = []
        self.structure_downloader = StructureDownloader(replicate_dir)

        url = "https://speakleash.space/datasets_text/"
        structure_file = "speakleash.json"

        if lang == "hr":
            url = "https://speakleash.space/datasets_text_hr/"
            structure_file = "speakleash_hr.json"

        names = self.structure_downloader.get_structure(url + structure_file)

        if names:
            for item in names:
                if "name" in item:
                    self.datasets.append(SpeakleashDataset(item["name"], url, self.replicate_dir))

    def get(self, name):
        return next((d for d in self.datasets if d.name == name), None)

class SpeakleashDataset:
    def __init__(self, name, url, replicate_dir):
        self.url = url
        self.name = name
        self.replicate_dir = replicate_dir
        self.structure_downloader = StructureDownloader(self.replicate_dir)
        self.manifest = self._download_manifest()
        self.jsonl_zst
