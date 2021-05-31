import pandas as pd
from pathlib import Path


df=pd.read_csv("backup/test_yabook.csv", index_col='index')
df["size_bit"]=0.0
#print (df.head())

# need to check the file size is correct!!
target_dir=Path("E:/yabook/")
def update_file_download_status(criteria=-10):
    for index, row in df.iterrows():
        size=row.file_size.split()
        print(index, size, df.loc[index].file_size)
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
        print (df.loc[index].book_id, df.loc[index].title, df.loc [index].file_size, df.loc[index, "downloaded"])

update_file_download_status()
print(df.info())
# file=df.loc[105].title
# print (df.loc[105])
# filepath = target_dir / file
# print (filepath, filepath.exists(), filepath.stat().st_size)
#if filepath.exists() and filepath.stat().st_size()>100:
# df['real_size'].fillna(0.0)
# df["size_bit"].fillna(0.0)
# df['real_size'].astype('float')
# df["size_bit"].astype('float')
#print(df.info())

#print(df.info())
def del_files(delta_ratio=-10):
    titles = df[df["size_delta_ratio"]<-10]["title"]
    for title in titles:
        file = target_dir / title
        if file.exists():
            Path.unlink(file)
            print (f"{file} is removed")
def check_downloaded():
    print(df.loc[df["real_size"]==0, ["title","downloaded"]])
del_files(-10)
df.to_csv("test_yabook.csv")