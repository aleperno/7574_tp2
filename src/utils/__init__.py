import re


POST_REGEX = "https?:\/\/(?P<url>.*)\/r\/(?P<subreddit>.*)\/comments\/(?P<post_id>.*?)\/"


def transform_dataset(data, mapping):
    new_data = {}
    for key, value in data.items():
        if key in mapping:
            new_key = mapping[key]
            new_data[new_key] = value

    return new_data


def get_post_id(post_permalink):
    m = re.match(POST_REGEX, post_permalink)
    if m:
        return m.groupdict()['post_id']
