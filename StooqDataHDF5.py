import warnings
import tables

from StooqData import StooqDataDownloader
from pathlib import Path
import requests
from io import BytesIO
from zipfile import ZipFile, BadZipFile

import numpy as np
import pandas as pd
import pandas_datareader.data as web
from sklearn.datasets import fetch_openml

class StooqDataDownloaderHDF5(StooqDataDownloader): 

    def __init__(self, data_folder): 
        super().__init__(data_folder)

    def __fetch_prices_and_tickers(self, frequency, market, asset_class): 
        prices = []
        tickers = (pd.read_csv(self.data_folder_path / 'tickers' / market / f'{asset_class}.csv'))

        if frequency in ['5 min', 'hourly']: 
            parse_dates = [['date', 'time']]
            dt_label = 'date_time'
        else: 
            parse_dates = ['date']
            dt_label = 'date'

        names = ['ticker', 'freq', 'date', 'time', 'open', 'high', 'low', 'close', 'volume', 'openint']
        usecols = ['ticker', 'open', 'high', 'low', 'close', 'volume'] + parse_dates

        path = self.data_folder_path / 'data' / frequency / market / asset_class
        files = path.glob('**/*.txt')
        for i, file in enumerate(files, 1): 
            if file.stem.strip('_') not in set(tickers.ticker.str.lower()): 
                file.unlink()
            else: 
                try: 
                    df = (pd.read_csv(file, names=names, usecols=usecols, header=0, parse_dates=parse_dates))
                    prices.append(df)
                except pd.errors.EmptyDataError: 
                    file.unlink()

        prices = (pd.concat(prices, ignore_index=True)
                 .rename(columns=str.lower)
                 .set_index(['ticker', dt_label])
                 .apply(lambda x: pd.to_numeric(x, errors='coerce')))

        return prices, tickers

    def download_to_hdf5(self, hd5_path, markets_to_assets, frequency): 
        idx = pd.IndexSlice
        for market, asset_classes in markets_to_assets.items(): 
            for asset_class in asset_classes: 
                prices, tickers = self.__fetch_prices_and_tickers(frequency=frequency, market=market, asset_class=asset_class)

                prices = prices.sort_index().loc[idx[:, '2000': '2021'], :]
                names = prices.index.names
                prices = (prices.reset_index().drop_duplicates().set_index(names).sort_index())

                key = f'stooq/{market}/{asset_class.replace(" ", "/")}/'
                prices.to_hdf(hd5_path, key + 'prices', format='t')
                tickers.to_hdf(hd5_path, key + 'tickers', format='t')