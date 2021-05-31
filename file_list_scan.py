
import requests
from bs4 import BeautifulSoup
import random
import pandas as pd
import time

base_url = "https://yabook.org"
UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.133 Safari/537.39'
DYN_UA_FORMAT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.{:04d}.{:03d} Safari/537.39'

def random_ua(id):
    return DYN_UA_FORMAT.format(random.randrange(9999), id + 1)

class Book():
    def __init__(self, id):
        self.id=id
        self.bookname=""
        self.title=""
        self.publisher=''
        self.publish_date=''
        self.ISDN=0
        self.ctfile_sn=""
        self.ctfile_url=''
        self.download_url=''
        self.file_size = 0
    def get_ctfile_url(self):
        payload = {
            'origin': base_url,
            'referer': base_url,
            'id': f'{self.id}.html'
        }
        headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Safari/537.36'
        }
        #url=f"https://yabook.org/post/{self.id}.html"
        resp = requests.post('https://yabook.org/e/DownSys/GetDown/ajax.php',data=payload, headers=headers)
        resp.encoding = resp.apparent_encoding
        resp.raise_for_status()
        #print(resp.text)
        soup = BeautifulSoup(resp.text, 'lxml')
        a_link = soup.find('a')
        #print(a_link)
        if a_link:
            download_url = base_url + a_link['href']
        else:
            return "fail"

        resp = requests.get(download_url, headers=headers)
        # print(resp2.text)
        soup = BeautifulSoup(resp.text, 'lxml')
        link2 = soup.find('a')['href'].rsplit('/')[-1]
        # link2=link2.split('/')
        print(link2)
        # start to download from ctfile
        rd = random.random()
        self.ctfile_sn = link2
        print (self.ctfile_sn)
        self.ctfile_url = 'https://webapi.ctfile.com/getfile.php?path=f&f=' + link2 + f'&passcode=2021&token=false&r={rd}&ref='
        print(self.ctfile_url)
        #url = 'https://webapi.ctfile.com/getfile.php?path=f&f=402712-493096506-583e68&passcode=2021&token=false&r=0.8967800232997811&ref='
        if link2:
            return "success"
        else:
            return "fail"

    def get_download_url(self):
        headers2 = {
            'accept': '*/*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'zh-TW,zh;q=0.8,en-US;q=0.6,en;q=0.4,zh-CN;q=0.2',
            'content-length': '0',
            'content-type': 'application/x-www-form-urlencoded;',
            'cookie':'PHPSESSID=in73uhbu439pg7flt8k1s41f53; pass_f493096506=2021; pass_f480223015=2021; pass_f489516410=2021; pass_f489516389=2021; pass_f489516396=2021; pass_f488925970=2021; pass_f489357960=2021; pass_f489358138=2021; pass_f489087954=2021; pass_f489087240=2021; pass_f489085655=2021; pass_f489085780=2021; pass_f489085587=2021; pass_f489086994=2021; pass_f488926056=2021; pass_f488926007=2021; pass_f488926288=2021; pass_f487850596=2021; pass_f487850584=2021; pass_f487850593=2021; pass_f487564190=2021; pass_f487564232=2021; pass_f487564150=2021',
            'host': 'webapi.ctfile.com',
            'origin': 'http://down.yabook.org',
            'referer': f'http://down.yabook.org/file/{self.ctfile_sn}',
            #'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36'
            'user-agent': random_ua(self.id)
            }

        resp = requests.get(self.ctfile_url, headers=headers2)
        resp.raise_for_status()
        if not resp:
            print("resp.status_code 503 failllllllllll", resp.status_code)
            #return "fail"
        file_link = resp.json()
        if file_link['code']==404 or file_link['code']==503:
            alterlink=self.ctfile_url.replace("path=f","path=file")
            resp = requests.get(alterlink, headers=headers2)
            resp.raise_for_status()
            if not resp:
                print("resp.status_code 503 failllllllllll", resp.status_code)
                # return "fail"
            file_link = resp.json()
            if file_link['code'] == 404 or file_link['code'] == 503:
                return

        print(file_link)
        #print(file_link['file_chk'])


        def parse_params(file):
            """
            Get required params from webpage.
            """
            file_data = file
            filename = file_data['file_name']
            userid = file_data['userid']
            file_id = file_data['file_id']
            folder_id = file_data.get('file_dir')
            file_chk = file_data['file_chk']
            file_size = file_data['file_size']
            mb = 0  # not mobile
            app = 0
            code = 200

            verifycode = ''
            rd = random.random()

            return userid, filename, file_id, folder_id, file_chk, file_size, mb, app, verifycode, rd

        userid, filename, file_id, folder_id, file_chk, file_size, mb, app, verifycode, rd = parse_params(file_link)
        get_file_api = f"/get_file_url.php?uid={userid}&fid={file_id}&folder_id={folder_id}&file_chk={file_chk}&mb={mb}&app={app}&acheck=1&verifycode={verifycode}&rd={rd}"
        baseurl = "https://webapi.ctfile.com" + get_file_api
        print(baseurl)
        #baseurl2 = 'https://webapi.ctfile.com/get_file_url.php?uid=402712&fid=493096506&folder_id=0&file_chk=8f1c7609cec0e043049d646914dbc84e&mb=0&app=0&acheck=1&verifycode=&rd=0.015085460542676454'

        resp3 = requests.get(baseurl, headers=headers2)
        #print("resp3", resp3.text)
        file_link2 = resp3.json()
        self.download_url=file_link2['downurl']
        self.file_size= file_size
        self.title=filename
        print(file_link2['downurl'])

df = pd.read_csv("yabook.csv")
#column= ["book_id","bookname","title","publisher","publish_date","ISDN","ctfile_sn","ctfile_url","download_url","file_size"]
#df = pd.DataFrame(columns=column)
start = df["book_id"].max()
fail_count=0
print(start)
for i in range(0, 200, 1):
    for j in range(0, 5, 1):
        k=i*5+j+1+start
        if k in [46,59,64,120,2114,2121,2123,2352, 2363,2366,2365,2372, 2376,2379,2382,2454, 3007 ]: continue
        book = Book(k)
        print(book.id)
        if book.get_ctfile_url()=="success":
            print(book.id, "download is success")
            book.get_download_url()
        else:
            print(book.id, "download is fail")
            fail_count+=1
            if fail_count >50:
                print ("fail count more than 100. Stop the program")
                break
            #pass
        data = {"book_id":book.id,
                "bookname":book.bookname,
                "title":book.title,
                "publisher":book.publisher,
                "publish_date":book.publish_date,
                "ISDN":book.ISDN,
                "ctfile_sn":book.ctfile_sn,
                "ctfile_url":book.ctfile_url,
                "download_url":book.download_url,
                "file_size":book.file_size
                }

        df.loc[len(df)]=data

    if fail_count > 50:
        #print("fail count more than 100. Stop the program")
        break
    print(df.tail())
    df.to_csv("yabook.csv", index_label=False)
    time.sleep(4)