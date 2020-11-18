import requests
import sys


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
    downloader = MavenDownloader()
    with open(sys.argv[1], 'r') as f:
        for line in f.readlines():
            downloader.download(line.strip())
