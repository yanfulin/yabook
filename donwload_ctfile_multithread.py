import pandas as pd
import requests
import os
import shutil
from pathlib import Path
import multiprocessing
import random
import urllib.request
from concurrent.futures import ThreadPoolExecutor

#df=pd.read_csv("yabook.csv", index_col='index')
df=pd.read_csv("yabook.csv")


#book_id,bookname,title,publisher,publish_date,ISDN,ctfile_sn,ctfile_url,download_url,file_size
#df.dropna()

#print(df.book_id[0], df.title[0], df.download_url[0], df.file_size[0])
target_dir=Path("E:/yabook/")
def update_file_download_status(criteria=-10):
    for index, row in df.iterrows():
        size=row.file_size.split()
        #print(index, size, df.loc[index].file_size)
        #print(len(size))
        #print(fast_float(size[0])*1024*1024)
        if len(size)==2:
            if size[1]=="MB":
                df.loc[index, "size_bit"] = float(size[0].replace(',', ''))*1024*1024
                #print("MB", index, df.loc[index, "size_bit"])
            elif size[1] == "KB":
                df.loc[index, "size_bit"]= float(size[0].replace(',', ''))*1024
                #print("KB", index, df.loc[index, "size_bit"])
            elif size[1] == "B":
                df.loc[index, "size_bit"] = float(size[0].replace(',', ''))
                #print("B", index, df.loc[index, "size_bit"])
        else:
            pass
        #print(df.loc[index, "size_bit"])
        file_name = str(row.title)
        file_path = target_dir / file_name
        if file_path.exists() and file_path.stat().st_size > 100:
                df.loc[index, "real_size"]= float(file_path.stat().st_size)
                df.loc[index, "size_delta"] = df.loc[index, "real_size"] - df.loc[index, "size_bit"]
                df.loc[index, "size_delta_ratio"] = (df.loc[index, "size_delta"] / df.loc[index, "real_size"]).round(1)
                if df.loc[index, "size_delta_ratio"] > criteria:
                    df.loc[index, "downloaded"] = 1
                elif df.loc[index, "real_size"]==0:
                    df.loc[index, "downloaded"] = 0
        else:
                 df.loc[index, "downloaded"] = 0
        print (df.loc[index].book_id, df.loc[index].title, df.loc[index].file_size, df.loc[index, "downloaded"])
# def update_file_download_status():
#     for index, row in df.iterrows():
#         file_name = str(row.title)
#         file_path = target_dir / file_name
#         if file_path.exists() and file_path.stat().st_size > 100:
#                 print (df.loc[index].book_id, df.loc[index].title)
#                 df.loc[index].downloaded = 1

update_file_download_status()

def download_file(book_id,url, file):
    DYN_UA_FORMAT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.{:04d}.{:03d} Safari/537.39'
    def random_ua(id):
        return DYN_UA_FORMAT.format(random.randrange(9999), id + 1)

    headers2 = {
        'user-agent': random_ua(book_id)
    }
    local_filename = file
    #print (local_filename)

    with requests.get(url, headers=headers2, stream=True) as r:
        r.raise_for_status()

        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                # If you have chunk encoded response uncomment if
                # and set chunk _size parameter to None.
                if chunk:
                    #print("#",end ="")
                    f.write(chunk)
    print(f"{book_id}  download is done. start to move file.")
    shutil.move(local_filename, target_dir)
    df.loc[df["book_id"]==book_id, "downloaded"]=1
    df.to_csv("yabook.csv")
    print(f"{book_id}  FILE IS MOVED AND  CSV IS UPDATED")
    msg = f"Finished downloading {local_filename}"
    return msg


# #@timeit
# def download_all(urls):
#     """
#     Create a thread pool and download specified urls
#     """
#
#     futures_list = []
#     results = []
#
#     with ThreadPoolExecutor(max_workers=13) as executor:
#         for url in urls:
#             futures = executor.submit(download_one, url)
#             futures_list.append(futures)
#
#         for future in futures_list:
#             try:
#                 result = future.result(timeout=60)
#                 results.append(result)
#             except Exception:
#                 results.append(None)
#     return results


futures_list = []
results = []

with ThreadPoolExecutor(max_workers=13) as executor:
    for index, row in df.iterrows():
        print(row.book_id, row["title"], row.download_url, row.file_size)
        if str(row.title)=='nan' or str(row.download_url)=='nan':
            print ("download fail", row.title, row.download_url)
        elif row.downloaded==1:
            print (f"{row.book_id} {row.title} downloaded ===>No need to download")
        else:
            futures = executor.submit(download_file, row.book_id, row.download_url, row.title)
            futures_list.append(futures)

    for future in futures_list:
        try:
            result = future.result(timeout=60)
            results.append(result)
        except Exception:
            results.append(None)

            #print(row.book_id, row.title, row.download_url, "start to download")
            #print('Beginning file download_file module {n}'.format(n=row.download_url))
            #download_file(row.book_id, row.download_url, row.title)
            #print(row.book_id, row.title, row.download_url, "finished")
for result in results:
    print(result)