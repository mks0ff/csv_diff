from pprint import pprint
import pandas as pd
import sys
import re
from hashlib import md5
import ast

keys = sys.argv[1]

cubi = sys.argv[2]
dpf = sys.argv[3]

out_file = sys.argv[4]

print(">> start comparing...")

keys = ast.literal_eval(keys)
print(f">> using keys {keys}")

print(f">> reading CUBI file {cubi}")
df1 = pd.read_csv(cubi, encoding='utf-8', sep='|', on_bad_lines='skip')
print(f">> reading DPF file {dpf}")
df2 = pd.read_csv(dpf, encoding='utf-8', sep='|', on_bad_lines='skip')

df1 = df1.convert_dtypes(convert_integer=True)
df2 = df2.convert_dtypes(convert_integer=True)

print(">> Original dtypes")
print('\n')
print(df1.dtypes)
print('\n')
print(df2.dtypes)
print('\n')

df1.columns = [re.sub(" ", "", x) for x in df1.columns]
df2.columns = [re.sub(" ", "", x) for x in df2.columns]

df1 = df1.sort_index().sort_index(axis=1)
df2 = df2.sort_index().sort_index(axis=1)

print(f">> CUBI file \n {df1}")
print(f">> DPF file \n {df2}")

check_duplicates_df1 = df1[keys].duplicated().any()
check_duplicates_df2 = df2[keys].duplicated().any()

print(f">> CUBI file contains duplicates \n {check_duplicates_df1}")
print(f">> DPF file contains duplicates \n {check_duplicates_df2}")

print('\n')
print(''.join(["="] * 200))
print('\n')

inner = pd.merge(df1, df2, on=keys, how='inner', indicator=True)
df3 = inner.query("_merge == 'both'").drop_duplicates(keys)
print(f">> Common rows \n {df3}")

print('\n')
print(''.join(["="] * 200))
print('\n')

outer = pd.merge(df1, df2, on=keys, how='outer', indicator=True)

df6 = outer.query("_merge == 'right_only'").drop_duplicates(keys)
print(f">> DPF rows missing in CUBI \n {df6}")

df6_ = outer.query("_merge == 'left_only'").drop_duplicates(keys)
print(f">> CUBI rows missing in DPF \n {df6_}")

print('\n')
print(''.join(["="] * 200))
print('\n')

df1["all"] = df1.astype(str).apply("-".join, axis=1)
# df1['hash'] = df1['all'].apply(hash)
df1['hash'] = [md5(val.encode()).hexdigest() for val in df1['all']]
# print(df1)

df2["all"] = df2.astype(str).apply("-".join, axis=1)
# df2['hash'] = df2['all'].apply(hash)
df2['hash'] = [md5(val.encode()).hexdigest() for val in df2['all']]
# print(df2)

keys.append('hash')
common = pd.merge(df1, df2, on=keys, how='inner', indicator=True) # .drop_duplicates(keys)
# common = pd.merge(df1, df2, on=['hash'], indicator=True).drop_duplicates(['hash'])
print(f">> Common rows based on keys + hash \n {common}")

print('\n')
print(''.join(["="] * 200))
print('\n')

df62 = df1[~df1.hash.isin(common.hash)].dropna()
df62_ = df62.drop(labels=["all", "hash"], axis = 1)
print(f">> CUBI rows missing in hash based Common rows \n {df62_}")

df63 = df2[~df2.hash.isin(common.hash)].dropna()
df63_ = df63.drop(labels=["all", "hash"], axis = 1)
print(f">> DPF rows missing in hash based Common rows \n {df63_}")

keys.remove('hash')
df9 = pd.concat([df62, df63]).drop(labels=["all", "hash"], axis = 1).drop_duplicates(keys)
print(f">> Updated and Missing rows in hash based Common rows \n {df9}")

print('\n')
print(''.join(["="] * 200))
print('\n')

print(">> Writing to files...")
print('\n')

out_common = f"{out_file}_common.csv"
out_missing = f"{out_file}_missing.csv"
out_final = f"{out_file}_final.csv"

df3.to_csv(out_common, sep='|', encoding='utf-8', index=False)
df6.to_csv(out_missing, sep='|', encoding='utf-8', index=False)
df9.to_csv(out_final, sep='|', encoding='utf-8', index=False) # missing + updated rows

print(out_common)
print(out_missing)
print(out_final)

print('\n')
print(">> done.")
print('\n')
print(''.join(["="] * 200))
print('\n')
