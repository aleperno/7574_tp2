import pandas as pd
from multiprocessing import Pool

DEFAULT_CHUNK = 10000
DEFAULT_POOLSIZE = 4


def read_csv(file_path, chunksize=DEFAULT_CHUNK):
    with pd.read_csv(file_path, chunksize=chunksize, keep_default_na=False) as reader:
        for chunk in reader:
            yield chunk.to_dict(orient='records')


def multiproc_read_csv(file_path, func, chunksize=DEFAULT_CHUNK, poolsize=DEFAULT_POOLSIZE):
    reader = pd.read_table(file_path, chunksize=chunksize)
    dfs = [df for df in reader]
    with Pool(poolsize) as p:
        print("hola")
        p.map(func, dfs)
