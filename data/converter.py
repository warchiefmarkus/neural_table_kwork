import csv
import pandas as pd
import re
path_to='rb.csv'
with open('rb.txt', 'r') as in_file:
    stripped = (line.strip() for line in in_file)
    lines = (line.split(",") for line in stripped if line)
    with open(path_to, 'w',encoding='utf-8') as out_file:
        writer = csv.writer(out_file)
        #writer.writerow(('title', 'intro'))
        writer.writerows(lines)


df=pd.read_csv(path_to,sep='\t', error_bad_lines=False,encoding='utf-8',decimal=',')#раасшифровка
lest=list(df["ID_SP_NAR;NAME"][:269])
l_split=[re.split(r';',lest[i]) for i in range(len(lest))]
l_num=[int(l_split[i][0]) for i in range(len(l_split)-2)]
l_text=[str(l_split[i][1]) for i in range(len(l_split)-2)]

