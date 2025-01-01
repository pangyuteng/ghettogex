
import os
import json
import pathlib
import pandas as pd

folder_list = [
    "tmp/SPX/2024-12-31/.SPXW241231C5920",
    "tmp/SPX/2024-12-31/.SPXW241231P5920",
]

#for x in folder_list
folder = folder_list[0]
summary_folder = os.path.join(folder,'summary')
timeandsale_folder = os.path.join(folder,'timeandsale')

open_interest = None
json_file_list = sorted([str(x) for x in sorted(pathlib.Path(summary_folder).rglob("*.json"))])
for json_file in json_file_list:
    with open(json_file,'r') as f:
        content = json.loads(f.read())
        #print(json_file,content['open_interest'])
        open_interest = content['open_interest']

mylist = []
json_file_list = sorted([str(x) for x in sorted(pathlib.Path(timeandsale_folder).rglob("*.json"))])
for json_file in json_file_list:
    with open(json_file,'r') as f:
        content = json.loads(f.read())
        content['json_file']=json_file
        content['open_interest']=open_interest
        key_list = ['type','aggressor_side','size','open_interest','json_file']
        item = {x:content[x] for x in key_list}
        mylist.append(item)
        print(json_file)
df = pd.DataFrame(mylist)
df.to_csv("ok.csv",index=False)
