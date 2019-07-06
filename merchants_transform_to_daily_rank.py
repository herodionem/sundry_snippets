
import os

import pandas as pd

os.getcwd()

os.listdir()

base = pd.read_csv('base.csv')

base.pct_buy_box = base.pct_buy_box.apply(lambda x: int(100 * x))

base.fillna('', inplace=True)

meta = base.groupby(['tld','week_beginning','asin']).pct_buy_box.apply(lambda x: tuple(x)).reset_index()

meta['set_to_one'] = meta.pct_buy_box.apply(len).apply(lambda x: x == 1)

def gcd(a,b):
   while b:
     a,b=b,a%b
   return a


gcd(40,2)
2

gcd(40,4)
4

gcd(40,20)
20

gcd(40,13)
1

from functools import reduce

def lgcd(*args):
   return reduce(gcd, args)


lgcd(40,32,56)
8

meta['gcd'] = meta.pct_buy_box.apply(lambda x: lgcd(*x))

meta[(meta.pct_buy_box.apply(len).apply(lambda x: x==2)) & (meta.pct_buy_box.apply(lambda x: 33 in x))]

joined = base.set_index(['tld','week_beginning','asin']).join(meta.set_index(['tld','week_beginning','asin']), lsuffix='_base', rsuffix='_meta').reset_index()

json_string = '[' + ''.join([(r.to_json() * int(r['pct_buy_box_base'] / r['gcd'])) if r['gcd'] > 0 else r.to_json() for i,r in joined.iterrows()]).replace('}','},')[:-1] + ']'

from io import StringIO

final_df = pd.read_json(StringIO(json_string), orient='records')

snowflake_df = final_df[['week_beginning','merchant','asin','tld']]