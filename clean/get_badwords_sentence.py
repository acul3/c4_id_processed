import functools
import gzip
import hashlib
import heapq
import io
import re
import threading
import nltk

from langdetect import detect
from langdetect.lang_detect_exception import LangDetectException
from langdetect import DetectorFactory

DetectorFactory.seed = 0

# from absl import logging
import tensorflow.compat.v2 as tf
# import tensorflow_datasets.public_api as tfds

# WET file constants
_PAGE_DELIMITER = "WARC/1.0"
_URL_KEY = "WARC-Target-URI:"
_URL_DATE = "WARC-Date:"
_CONTENT_TYPE = "Content-Type:"
_CONTENT_LEN = "Content-Length:"
_METADATA_PREFIXES = ("WARC", "CONTENT-", "Content-")

# Filters
_MIN_WORDS_PER_LINE = 3
_MIN_NUM_SENTENCES = 5
_MAX_WORD_LENGTH = 250
_END_MARKS = (".", "?", "!", "\"")
_ELLIPSIS = "..."
_POLICY_SUBSTRINGS = [
    "terms of use", "privacy policy", "cookie policy", "uses cookies",
    "use of cookies", "use cookies", "elementen ontbreken aan deze printversie","kebijakan privasi"
]

# Memoized sentence tokenizer.
_SENTENCE_TOKENIZER = None

UNKNOWN_LANGUAGE = "und"

citation_regex = re.compile(r"\[\d*\]|\[edit\]|\[citation needed\]")

from .badwords_ennl import badword_list
badwords_regex = re.compile(r"(?:\W|^)({})(?:\W|$)".format("|".join(badword_list)))



def badwords_filter(text):
  badwords_found = badwords_regex.search(text.lower())
  if badwords_found is not None:
    return False
  return True


def clean_text(text,
               citation_regex=citation_regex,
               min_words_per_line=_MIN_WORDS_PER_LINE,
               min_num_sentences=_MIN_NUM_SENTENCES,
               max_word_length=_MAX_WORD_LENGTH):
  """Cleans a CommonCrawl page, yielding nothing if it should be skipped.

  Cleaning removes lines with no end marks or with too few words. After line
  filtering, pages are filtered out if they have too few sentences based on a
  simple count of end marks.

  Args:
    text: text of the page
    citation_regex: Regex to use for finding Wikipedia-like citations to filter.
    counter_inc_fn: function, a function taking the name of a counter to be
      incremented and the (optional) amount. Defaults to a beam Metric counter.
    min_words_per_line: int, the minimum number of words a line needs to not be
      removed.
    min_num_sentences: int, the minimum number of sentences a page needs to not
      be skipped.
    max_word_length: int, the maximum number of characters allowed in a word.
      Lines containing a word with too many characters are removed.

  Yields:
    The url and cleaned text for the page.
  """

  lines = text.splitlines()
  valid_lines = []
  num_sentences = 0
  if badwords_filter(text):
    counter_inc_fn("badword-filtered: not passed")
    return
  def line_has_too_long_word(line):
    for word in line.split():
      if len(word) > max_word_length:
        return True
    return False

  for line in lines:
    line = line.strip()
    if not line_has_too_long_word(line):
      counter_inc_fn("line-filtered:too_long_word")
      continue
    line = citation_regex.sub("", line)
    if line.endswith(_END_MARKS) or not line.endswith(_ELLIPSIS):
      counter_inc_fn("line-filtered:no_endmark")
      continue
    if len(line.split()) < min_words_per_line:
      counter_inc_fn("line-filtered:too_short")
      continue
    line_lower = line.lower()
    # Remove policy lines
    num_sentences += len(_get_sentences(line))
    valid_lines.append(line)
    counter_inc_fn("line-passed")

  if num_sentences < min_num_sentences:
    counter_inc_fn("filtered:too_few_sentences")
    return
  counter_inc_fn("passed")
  result = "\n".join(valid_lines).strip()
  if len(result) < 500 or len(result) > 50000:
    counter_inc_fn("filtered:size too small or too big")
    return

  return result


def _get_sentences(text):
  global _SENTENCE_TOKENIZER
  if not _SENTENCE_TOKENIZER:
    _SENTENCE_TOKENIZER = _load_sentence_tokenizer()
  return list(_SENTENCE_TOKENIZER.tokenize(tf.compat.as_text(text)))


_nltk_lock = threading.Lock()

def _load_sentence_tokenizer():
  """Returns a sentence tokenization function."""
  # Lock to avoid a race-condition in the creation of the download directory.
  with _nltk_lock:
    nltk.download("punkt")
    return nltk.data.load("nltk:tokenizers/punkt/english.pickle")


count_dict = dict()

def counter_inc_fn(what):
  if what in count_dict:
    count_dict[what] += 1
  else:
    count_dict[what] = 1



