import sys
import jsonlines
import gzip
import json
import zstandard as zstd
from clean import clean_text

params = zstd.ZstdCompressionParameters.from_level(12)
cctx = zstd.ZstdCompressor(compression_params=params)


PATH ="/data/2021_49_raw_cleaned/"
def main(file):
    file_name = file.split('/')[-1]
    with zstd.open(f"{PATH}/cleaned_{file_name}",cctx=cctx,encoding="utf-8", mode='wb') as writer:
        with gzip.open(f'{file}') as f:
        #with zstd.open(open(f'{file}', "rb"), "rt", encoding="utf-8") as f:
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