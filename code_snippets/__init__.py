PRODUCTORES = {
    'a': ('b', 'c')
}

STATUSES = {
    'b': (1, ),
    'c': (2, 3)
}

ALL_STATUSES = [status for producer in PRODUCTORES['a'] for status in STATUSES[producer]]

print(ALL_STATUSES)
