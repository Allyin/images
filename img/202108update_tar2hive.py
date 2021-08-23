#!/usr/bin/env python3
# -*- coding=utf-8
from qcloud_cos import CosConfig
from qcloud_cos import CosS3Client
from qcloud_cos import CosServiceError
from qcloud_cos import CosClientError

import time
import pathlib
import base64
import sys
import tarfile
import logging
import os
import signal
import argparse

import sys
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from hashlib import sha256
from db.db import DB
from multiprocessing import Pool, Lock, Manager


logging.basicConfig(level=logging.INFO, stream=sys.stdout)

CPP_EXTENSIONS = [".cc", ".c", ".cpp", ".cxx", ".c++", ".cp", "cci"]
HEADER_EXTENSIONS = [".h", ".hpp"]
HIVE_SPECIAL_CHARS = [',', '|', '\n', ':']
DEBUG = False


def parameter_parser():
    parser = argparse.ArgumentParser(description="Update hive")
    parser.add_argument("-c", "--cpu", type=int, default=1, help="multiprocess number")
    parser.add_argument("-n", "--number", type=int, default=1, help="number of tags to parse")
    parser.add_argument("-o", "--output", type=str, default='/cfs/cfs-ei3z1jdn/source', help="output path")
    parser.add_argument("--hive_path", type=str, default=None, help="hive output path, in the format of YYYYMMDD")
    parser.add_argument("--overwrite", action="store_true", help="overwrite hive partition")
    return parser.parse_args()


def is_header_file(arg):
    tmp = arg.lower()
    for ext in HEADER_EXTENSIONS:
        if tmp.endswith(ext):
            return True
    return False


def is_source_file(arg):
    tmp = arg.lower()
    for ext in CPP_EXTENSIONS:
        if tmp.endswith(ext):
            return True
    return False


def replace_hive_special_chars(s):
    for c in HIVE_SPECIAL_CHARS:
        if c in s:
            s = s.replace(c, '_')
    return s


def parse_tag_tar(software_name, github_tag_name, tag_cfs_path, out_path):
    out_file = ''
    outfile_path = os.path.join(out_path, '%s-%s.txt' % (software_name, base64.b64encode(github_tag_name.encode('utf8', errors='ignore')).decode('utf8')))
    if os.path.exists(outfile_path):
        print('[+] Already exists: %s-%s' % (software_name, github_tag_name))
        return
    print('[+] Save to hive: %s-%s' % (software_name, github_tag_name))
    with tarfile.open(tag_cfs_path, 'r:gz') as tar:
        for tarinfo in tar.getmembers():
            if tarinfo.isfile():
                path = tarinfo.path
                name = os.path.split(path)[-1]
                if is_source_file(name) or is_header_file(name):
                    abs_path = path
                    github_tag_name = replace_hive_special_chars(github_tag_name)
                    rel_path = replace_hive_special_chars(path.split('/', 1)[1])
                    file_cont = bytes(tar.extractfile(abs_path).read())
                    file_cont_base64 = base64.b64encode(file_cont).decode()
                    file_sha256 = sha256(file_cont).hexdigest()
                    out_file += '%s|%s|%s|%s|%s\n' % (software_name, github_tag_name, rel_path, file_cont_base64, file_sha256)
    open(outfile_path, 'w').write(out_file)
    os.chmod(outfile_path, 0o664)


def upload_one(lock, db, tag, out_path, overwrite=False):
    tag_id, software_name, github_tag_name, tag_cfs_path = tag
    try:
        parse_tag_tar(software_name, github_tag_name, tag_cfs_path, out_path)
    except Exception as e:
        print(str(e))
        lock.acquire()
        db.update_hive_path(tag_id, 'INVALID')
        lock.release()
        return
    lock.acquire()
    db.update_hive_path(tag_id, 'binaryaistorm.t_sd_binaryaistorm_source_new/%s' % out_path.split('/')[-1])
    lock.release()


def main():
    start_time = time.time()
    args = parameter_parser()
    if args.hive_path is None:
        out_dir = time.strftime('%Y%m%d', time.localtime(time.time()))
    else:
        out_dir = args.hive_path
    out_path = os.path.join(args.output, out_dir)  # save hive txt file to source folder
    if os.path.exists(out_path):
        print('[*] output path already exists: %s' % out_path)
        if args.overwrite:
            os.system('rm -f %s/*' % out_path)
    else:
        pathlib.Path(out_path).mkdir(mode=0o775, parents=True, exist_ok=True)
    
    with DB() as db:
        tags = db.get_empty_hive_list(args.number)
        if len(tags) == 0:
            print('[*] No tag is fetched, exit')
            return
        pool = Pool(args.cpu)
        manager = Manager()
        lock = manager.Lock()
        print('[+] Total tags: %d' % len(tags))
        results = []
        for i in range(len(tags)):
            tag = tags[i]
            if DEBUG:
                upload_one(lock, db, tag, out_path, args.overwrite)
            else:
                res = pool.apply_async(upload_one, (lock, db, tag, out_path, args.overwrite,))
                results.append(res)
        pool.close()
        pool.join()
        for res in results:
            res.get()
        db.commit()
    end_time = time.time()
    print('[+] Time cost(%d cores): %s' % (args.cpu, (end_time - start_time)))


if __name__ == "__main__":
    main()
