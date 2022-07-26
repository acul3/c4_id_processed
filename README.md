this forked and edit from https://gitlab.com/yhavinga/c4nlpreproc


Code used to clean the c4 id corpus of non-sentences, too short texts and texts containing bad words.

Download 

```shell
GIT_LFS_SKIP_SMUDGE=1 git clone https://huggingface.co/datasets/allenai/c4
cd c4
git lfs pull --include "multilingual/c4-id.*.json.gz"
```


* main.py is a pyspark program to process all files
* singlefily.py is a simple python loop to process a single file


````
ls c4-nl* | parallel --gnu --jobs 96 --progress python ~/c4nlpreproc/singlefile.py {}
```
