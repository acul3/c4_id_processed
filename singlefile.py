import sys
import jsonlines
import gzip
import json
from clean import clean_text


PATH ="/data/KoPI-CC/2021_10/neardup_clean/"

def main(file):
    file_name = file.split('/')[-1]
    with gzip.open(f"{PATH}/cleaned_{file_name}", mode='wb') as writer:
        with gzip.open(f'{file}') as f:
            for line in f:
                obj = json.loads(line)
                cleaned = clean_text(obj['text'])
                if cleaned is not None:
                    obj['text'] = cleaned
                    writer.write(str.encode(json.dumps(obj)))
                    writer.write('\n'.encode("utf-8"))

if __name__ == '__main__':
    file = sys.argv[1]
    main(file)

