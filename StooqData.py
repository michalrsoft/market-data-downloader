import warnings
import tables

from pathlib import Path
import requests
from io import BytesIO
from zipfile import ZipFile, BadZipFile

import numpy as np
import pandas as pd
import pandas_datareader.data as web
from sklearn.datasets import fetch_openml

class StooqDataDownloader: 

    STOOQ_URL = 'https://static.stooq.com/db/h/'

    data_folder = ''
    data_folder_path = None

    def __init__(self, data_folder): 
        pd.set_option('display.expand_frame_repr', False)
        self.data_folder = data_folder
        self.data_folder_path = Path(self.data_folder)
        if not self.data_folder_path.exists():
            self.data_folder_path.mkdir()

    def download_prices(self, market): 
        market_url = f'd_{market}_txt.zip'
        response = requests.get(self.STOOQ_URL + market_url).content
        with ZipFile(BytesIO(response)) as market_zip: 
            for i, file in enumerate(market_zip.namelist()): 
                if not file.endswith('.txt'):   
                    continue

                disk_file = self.data_folder_path / self.__file_path_append_prefix(file)
                disk_file.parent.mkdir(parents=True, exist_ok=True)
                with disk_file.open('wb') as output: 
                    for line in market_zip.open(file).readlines(): 
                        output.write(line)
                    output.close()

    def download_tickers(self, marketsassets_dict): 
        for (market, asset_class), code in marketsassets_dict.items(): 
            df = pd.read_csv(f'https://stooq.com/db/l/?g={code}', sep='        ').apply(lambda x: x.str.strip())
            df.columns = ['ticker', 'name']
            df = df.drop_duplicates('ticker').dropna()
            path = self.data_folder_path / 'tickers' / market
            if not path.exists(): 
                path.mkdir(parents=True)
            df.to_csv(path / f'{asset_class}.csv', index=False)

    def __file_path_append_prefix(self, file_path, prefix='/__'): 
        file_parts = file_path.rsplit('/', 1)
        return prefix.join(file_parts)