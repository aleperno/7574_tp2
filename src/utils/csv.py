import pandas as pd


DEFAULT_CHUNK = 10000


def read_csv(file_path, chunksize=DEFAULT_CHUNK):
    with pd.read_csv(file_path, chunksize=chunksize, keep_default_na=False) as reader:
        for chunk in reader:
            yield chunk.to_dict(orient='records')
