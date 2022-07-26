import sys
import jsonlines
import gzip
import json
from clean import clean_text
from fastavro import reader


PATH ="/media/yeb/NLP datasets/news_cc/nrc_crawl_20210223"



def main(file):
    newfilename = file.replace(".avro", "")
    with gzip.open(f"cleaned_{newfilename}.json.gz", mode='wb') as writer:
        with open(f'{PATH}/{file}', 'rb') as f:
            for obj in reader(f):
                cleaned = obj['fulltext']   ##clean_text(obj['fulltext'])
                if cleaned is not None:
                    new={"url": obj['url'],
                         "text": cleaned,
                         "timestamp": obj['warc_date']}
                    writer.write(str.encode(json.dumps(new)))
                    writer.write('\n'.encode("utf-8"))

if __name__ == '__main__':
    file = sys.argv[1]
    main(file)

