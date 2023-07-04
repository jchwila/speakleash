import requests
import json
from tqdm import tqdm
import os
from lm_dataformat import Reader
import hashlib
from datetime import datetime
import glob
import tempfile

class FileManager:

    @staticmethod
    def ensure_dir_exists(directory):
        if not os.path.exists(directory):
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

    @staticmethod
    def get_json(url):
        try:
            r = requests.get(url)
            if r.ok:
                return json.loads(r.text)
        except:
            return None

    @staticmethod
    def get_text(url, encoding='utf-8'):
        try:
            r = requests.get(url)
            r.encoding = encoding
            if r.ok:
                return r.text
        except:
            return None

    @staticmethod
    def download_file(url, filepath):
        response = requests.get(url, stream=True)
        total_size_in_bytes = int(response.headers.get('content-length', 0))
        block_size = 1024
        progress_bar = tqdm(total=total_size_in_bytes, unit='iB', unit_scale=True)
        with open(filepath, 'wb') as file:
            for data in response.iter_content(block_size):
                progress_bar.update(len(data))
                file.write(data)
        progress_bar.close()
        return total_size_in_bytes == progress_bar.n


class StructureDownloader(object):

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

class CategoryManager(object):

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

class Speakleash(object):

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

class SpeakleashDataset(object):

    def __init__(self, name, url, replicate_dir):
        self.url = url
        self.name = name
        self.replicate_dir = replicate_dir
        self.structure_downloader = StructureDownloader(self.replicate_dir)
        self.manifest = self._download_manifest()

    def _download_manifest(self):
        data = self.structure_downloader.get_structure(self.url + self.name + ".manifest")
        if data:
            return data
        else:
            print(f"Error downloading manifest {self.url + self.name + '.manifest'}")
        return {}

    def _download_file(self, file_name):
        url = self.url + self.name + ".jsonl.zst"
        file_path = os.path.join(self.replicate_dir, file_name)
        return WebRequester.download_file(url, file_path)

    def check_file(self):
        FileManager.ensure_dir_exists(self.replicate_dir)

        file_name_json_zst = os.path.join(self.name + ".jsonl.zst")
        file_path_json_zst = os.path.join(self.replicate_dir, file_name_json_zst)

        file_json_zst_exists = os.path.exists(file_path_json_zst) and os.path.getsize(file_path_json_zst) == self.jsonl_zst_file_size

        if not file_json_zst_exists and not self._download_file(file_name_json_zst):
            return False, ""

        return True, file_path_json_zst

    def _get_data(self, get_meta=False):
        ok, file_path_json_zst = self.check_file()
        if not ok:
            return None

        rdr = Reader(file_path_json_zst)
        return rdr.stream_data(get_meta=get_meta)

    def _get_stat(self, *args):
        stat = self.manifest.get('stats', {})
        for arg in args:
            stat = stat.get(arg, {})
        return stat

    @property
    def data(self):
        return self._get_data()

    @property
    def ext_data(self):
        return self._get_data(get_meta=True)

    @property
    def samples(self):
        return self.structure_downloader.get_structure(self.url + self.name + ".sample", False) or []

    @property
    def description(self):
        return self.manifest.get('description', '')

    @property
    def license(self):
        return self.manifest.get('license', '')

    @property
    def category(self):
        return self.manifest.get('category', '')

    @property
    def sources(self):
        return self.manifest.get('sources', {})

    @property
    def characters(self):
        return self._get_stat('characters')

    @property
    def quality_metrics(self):
        return any(self._get_stat('quality', q) != 0 for q in ['HIGH', 'LOW', 'MEDIUM'])

    @property
    def categorization(self):
        return any(val > 0 for val in self.manifest.get('category=95%', {}).values())

    @property
    def categories(self):
        return self.manifest.get('category=95%', {})

    @property
    def quality(self):
        return self._get_stat('quality')

    @property
    def documents(self):
        return self._get_stat('documents')

    @property
    def stopwords(self):
        return self._get_stat('stopwords')

    @property
    def jsonl_zst_file_size(self):
        return self.manifest.get('file_size', 0)

    @property
    def nouns(self):
        return self._get_stat('nouns')

    @property
    def verbs(self):
        return self._get_stat('verbs')

    @property
    def symbols(self):
        return self._get_stat('symbols')

    @property
    def punctuations(self):
        return self._get_stat('punctuations')

    @property
    def sentences(self):
        return self._get_stat('sentences')

    @property
    def words(self):
        return self._get_stat('words')

    def __repr__(self):
        return f"SpeakleashDataset([{self.name},{self.url},{self.characters}])"

    def __str__(self):
        return f"name: {self.name}, url: {self.url}, characters: {self.characters}"
