import re
import os
from urllib.request import Request, urlopen
from mimetypes import guess_extension

BUFFSIZE = 1000
REGEX = "bytes (?P<start>\d+)-(?P<end>\d+)\/(?P<max_bytes>\d+)$"


def get_bytes_data(content_range_text):
    m = re.match(REGEX, content_range_text)
    if m:
        data = m.groupdict()
        return int(data['start']), int(data['end']), int(data['max_bytes'])


def get_type(url):
    response = urlopen(Request(url=url, method='HEAD'))
    mime_type = response.headers.get('Content-Type')
    return guess_extension(mime_type)


def image_downloader(url, buffersize=BUFFSIZE):
    max_bytes = 2
    current_bytes = 0

    while current_bytes+1 < max_bytes:
        request = Request(url)
        request.add_header('Range', f'bytes={current_bytes}-{current_bytes+buffersize-1}')

        response = urlopen(request)
        range = response.headers.get('Content-Range')
        start, end, maxbytes = get_bytes_data(range)

        max_bytes = maxbytes
        current_bytes = end + 1

        yield response.read()


def store_image(image_bytes, path):
    with open(path, 'w+b') as f:
        for chunk in image_bytes:
            f.write(chunk)


def download_and_store(url, path, buffersize=BUFFSIZE):
    ext = get_type(url)
    image_gen = image_downloader(url, buffersize=buffersize)
    image_path = os.path.join(path, f'best_meme{ext}')
    store_image(image_gen, image_path)
