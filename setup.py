import requests
import sys
import argparse

"""
 Class to download artifacts from maven (or other repositories)
 -  base_url: repository root, defaults to https://repo1.maven.org/maven2/
 
 usage: 
 python setup.py 
    -req <path to txt file containing one dependency per line (full path from base repo to artifact> 
    -url [Optional] <repository_base_url>

"""


class MavenDownloader:
    def __init__(self, base_url='https://repo1.maven.org/maven2/'):
        self.base_url = base_url

    def download(self, artifact):
        print('downloading ' + str(artifact))
        url = self.base_url + str(artifact)
        r = requests.get(url)

        filename = artifact.split('/')[-1]
        file_path = 'dependencies/%s' % filename

        with open(file_path, 'wb') as f:
            f.write(r.content)

        # Retrieve HTTP meta-data
        print(r.status_code)
        print(r.headers['content-type'])


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-url')
    parser.add_argument('-req')
    args = parser.parse_args()

    if args.url:
        downloader = MavenDownloader(args.url)
    else:
        downloader = MavenDownloader()

    with open(args.req, 'r') as f:
        for line in f.readlines():
            downloader.download(line.strip())
