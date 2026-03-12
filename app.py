import os
import time
import logging
from flask import Flask, jsonify
from flask_cors import CORS
app = Flask(__name__)
CORS(app)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_cache = {}
CACHE_TTL = 300

def get_cached(key):
  if key in _cache:
      data, ts = _cache[key]
      if time.time() - ts < CACHE_TTL:
          return data
  return None
              
def set_cache(key, data):
  _cache[key] = (data, time.time())

def find_col(df, names):
  for c in df.columns:
      if c.lower() in names:
           return c
  return None


              
