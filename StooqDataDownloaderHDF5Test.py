from StooqData import StooqDataDownloader
from StooqDataHDF5 import StooqDataDownloaderHDF5

downloader = StooqDataDownloaderHDF5('D:\\stooqdata')



#downloader.download_prices('us')
#downloader.download_prices('jp')



#marketsassets = {
#    ('us', 'nasdaq etfs'): 69,
#    ('us', 'nasdaq stocks'): 27,
#    ('us', 'nyse etfs'): 70,
#    ('us', 'nyse stocks'): 28,
#    ('us', 'nysemkt stocks'): 26, 
#    ('jp', 'tse etfs'): 34,
#    ('jp', 'tse stocks'): 32,
#}

#downloader.download_tickers(marketsassets)



markets_to_listofassets = { 'us': ['nasdaq etfs', 'nasdaq stocks', 'nyse etfs', 'nyse stocks', 'nysemkt stocks'] }

frequency = 'daily'

downloader.download_to_hdf5('D:\\stooqdata\\us-data.h5', markets_to_listofassets, frequency)