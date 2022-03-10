import pandas as pd
import glob
import re

"""
Quick and dirty script to combine individual CSV files into a single one.
"""

path = "./test-data/*"

df = None

for filename in glob.glob(path):
    try:
        if 'combined-data' not in filename:
            _df = pd.read_csv(filename)

            throughput_size = re.findall(r'\d+', filename)
            throughput = throughput_size[0]
            size = throughput_size[1]

            _df['throughput'] = throughput
            _df['size'] = size

            if df is None:
                df = _df
            else:
                df = df.append(_df) 
    except Exception as e:
        print(f"Error for {filename}: {e}")
   
print(df.head(100))

df.to_csv('./test-data/combined-data.csv', index=False)

