import json

## list taken from https://raw.githubusercontent.com/turalus/encycloDB/master/Dirty%20Words/DirtyWords.json

with open('DirtyWords.json') as f:
    data = json.load(f)

data = data['RECORDS']

dutch_badwords = [x.get('word') for x in data if x.get('language') in ['en', 'nl']]

import pprint
pprint.pprint(dutch_badwords)