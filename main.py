# -*- coding: utf-8 -*-

import sys
import os.path

# qiniu
from qiniu import Auth
from qiniu import BucketManager
import requests

# ucloud
from ufile import filemanager 

import pickledb
#config.set_default(uploadsuffix='.cn-bj.ufileos.com')
#set_log_file('/tmp/ufilelog.txt')

# 当前目录
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# 临时写文件目录
TMP_DIR = '/tmp/kodo'


def qiniu_test(
        access_key, secret_key, bucket_name, bucket_domain,
        u_bucket_name, u_publickey, u_privatekey):


    PickleDBPathSucc = os.path.join(BASE_DIR, bucket_name + 'succ.db')
    PickleDBPathFail = os.path.join(BASE_DIR, bucket_name + 'fail.db')
    db_succ = pickledb.load(PickleDBPathSucc, False)
    db_fail = pickledb.load(PickleDBPathFail, False)

    q = Auth(access_key, secret_key)

    u_handler = filemanager.FileManager(u_publickey, u_privatekey)
    # 枚举当前bucket下所有文件
    bucket = BucketManager(q)
    ret, eof, info = bucket.list(bucket_name)

    base_url_pattern = 'http://%s/%s'

    for item in ret['items']:

        # 对已抓取过的七牛(bucket,key)进行dedup去重
        if db_succ.get(item['key']) == True:
            continue

        base_url = base_url_pattern % (bucket_domain, item['key'])
        private_url = q.private_download_url(base_url)
        print(private_url)
        
        try:
            print(item['key'], '正在抓取')

            r = requests.get(private_url, stream=True)
            if r.status_code == 200:
                file_path = os.path.join(TMP_DIR, item['key'])
                f = open(file_path, 'wb')
                for chunk in r.iter_content(chunk_size=4096):
                    if chunk:
                        f.write(chunk)
                f.close()
                print(item['key'], '结束写入')

                ret, resp = u_handler.putfile(u_bucket_name, item['key'], file_path)
                if resp.status_code == 200:
                    # 本地记录ufile写入成功的key 
                    db_succ.set(item['key'], True)
                else:
                    db_fail.set(item['key'], True)
            else:
                db_fail.set(item['key'], True)
        except:
            db_fail.set(item['key'], True)

    db_succ.dump()
    db_fail.dump()

def main():

    # 临时存储目录
    if not os.path.exists(TMP_DIR):
        os.makedirs(TMP_DIR)

    q_access_key = '' # 七牛控制台ak
    q_secret_key = '' # 七牛控制台sk
    q_bucket_name = ''                            # 要迁移的bucket
    q_bucket_domain = ''                        # 七牛控制台->对象存储->空间概览->融合CDN加速域名

    u_bucket_name = ''                            # ucloud要写入的bucket
    u_publickey = ''# ufile -> 对象存储 -> 令牌管理 -> bucket的公钥和私钥
    u_privatekey = ''     # 私钥

    print(q_bucket_name, '开始抓取')
    qiniu_test(
        q_access_key, q_secret_key, q_bucket_name, q_bucket_domain,
        u_bucket_name, u_publickey, u_privatekey 
        )
    print(q_bucket_name, '结束抓取')

if __name__ == '__main__':
    main()
