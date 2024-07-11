import sys
from collections import Counter
from json import loads as json_loads

urls = set()
c = Counter()
for line in sys.stdin:
    key, ts, metadata_json = line.rstrip('\n').split(' ', maxsplit=2)
    metadata = json_loads(metadata_json)
    langs = metadata['detected_langs']
    if len(langs) == 1:
        langs = {next(iter(langs)): 1.0}  # Normalise key to 1.0

    c[tuple([(k, v) for k, v in langs.items()])] += metadata['net_no_of_words']
    urls.add(metadata['url'])

for langs, count in c.most_common():
    print(count, dict(langs))

with open('urls.txt', 'w', encoding='UTF-8') as fh:
    for url in urls:
        print(url, file=fh)
