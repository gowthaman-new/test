import json
import pandas as pd
from pandas.io.json import json_normalize
import numpy as np

# Reading first json file and convert to data frame
with open("F:\Programs\Python\input1.json") as file1:
    json_data1 = json.load(file1)
    df1 = json_normalize(json_data1['content'])
    df1['status'] = 'y'

# Reading Second json file and convert to data frame
with open("F:\Programs\Python\input2.json") as file2:
    json_data2 = json.load(file2)
    df2 = json_normalize(json_data2)

# Merge two dataframes and change the status based on join condition
df3 = pd.merge(df2, df1, how='left', on=('name', 'location'))
df3['status'] = np.where(df3['age']>=0, 'Y', 'N')

# Filter only records which have status=N
df3 = df3[ df3.status == 'N' ]

# After filering merge it with df1
df1 = df1.append(df3)
print(df1)

# Finally convert this to json file from data frame
df1 = df1.to_json(orient = 'records')
with open('F:\Programs\Python\output.json', 'w') as op:
    op.write(df1)

# Upsert document in mongodb using json file from python
# http://stackoverflow.com/questions/27901835/how-to-insert-new-element-into-existed-mongodb-and-uptate-existed-document-from
