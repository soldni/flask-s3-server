import errno
import functools
import hashlib
import ipaddress
import itertools
import logging
import os
import shutil
import tempfile
from typing import Sequence, Union

import click
from flask import Flask, send_file
from s3fs import S3FileSystem


logger = logging.getLogger()


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


class LruPathEntry:
    def __init__(self, path):
        self.path = path

    def __str__(self):
        return self.path

    def __repr__(self):
        return self.__str__()

    def __del__(self):
        path = getattr(self, 'path', None)
        if path is not None and os.path.exists(path):
            logger.info(f'Invalidating cache at {path}')

            if os.path.isdir(path):
                shutil.rmtree(path)
            elif os.path.isfile(path):
                os.remove(path)




class FlaskWebServer:
    SEPARATOR: str = "-"
    HEADER: str = 'Bucket Prefixes:'
    LIST_SYMBOL: str = '-'
    INDENT_WIDTH: int = 2

    def __init__(self,
                 buckets: Sequence[str],
                 flask_app: Flask,
                 cache_prefix: str,
                 cache_size: int,
                 s3fs_kwargs={}):
        self.buckets = buckets

        self.cache_prefix = os.path.join(cache_prefix, self.__class__.__name__)
        mkdir_p(self.cache_prefix)

        self.fs = S3FileSystem(**s3fs_kwargs)
        self.cache_size = cache_size
        self.sep_width = max(map(len, buckets)) + len(self.LIST_SYMBOL) + self.INDENT_WIDTH + 1

        flask_app.route('/', defaults={'path': None})(
            flask_app.route('/<path:path>')(self.get_s3_prefix)
        )
        # add some LRU cache
        download_prefix = functools.lru_cache(maxsize=self.cache_size)(self.download_prefix)
        self.download_prefix = download_prefix

    def __del__(self):
        cache_prefix = getattr(self, 'cache_prefix', None)
        if cache_prefix and os.path.exists(cache_prefix):
            logger.info(f'Shutting down; removing all data from {cache_prefix}')
            shutil.rmtree(cache_prefix)

    def print_buckets(self):
        print(self.SEPARATOR * self.sep_width)
        print(self.HEADER)
        for bucket in self.buckets:
            print(' ' * self.INDENT_WIDTH + self.LIST_SYMBOL + f' {bucket}')
        print(self.SEPARATOR * self.sep_width)

    def hash_string(self, string: str) -> str:
        return hashlib.md5(string.encode('utf-8')).hexdigest()

    def _recursive_download(self, prefix, caching_dir, root=True):
        logger.debug(f's_recursive_download(prefix="{prefix}", caching_dir="{caching_dir}")')

        if self.fs.isdir(prefix):
            # get the name the current directory
            _, current_dir = os.path.split(prefix.rstrip('/'))

            # ignore the first item, which is the directory itself
            _, *prefixes_to_download = self.fs.ls(prefix)

            # create new caching path
            caching_dir = os.path.join(caching_dir, current_dir)
            mkdir_p(caching_dir)

            # ensure trailing '/'
            caching_dir = caching_dir.rstrip('/') + '/'

            # call itself on all its subprefixes
            cached_paths = list(itertools.chain.from_iterable(
                self._recursive_download(prefix=p, caching_dir=caching_dir)
                for p in prefixes_to_download
            ))
            return [caching_dir.rstrip('/')] + cached_paths
        else:
            _, fn = os.path.split(prefix)
            destination = os.path.join(caching_dir, fn)
            self.fs.download(prefix, destination)
            return [destination]

    def download_prefix(self, prefix) -> LruPathEntry:
        caching_dir = os.path.join(self.cache_prefix, self.hash_string(prefix))

        if self.fs.isdir(prefix):
            compressed_caching_dir = f'{caching_dir}.tar.gz'
        else:
            compressed_caching_dir = None

        # clean up previously cached files
        if os.path.isdir(caching_dir):
            shutil.rmtree(caching_dir)

        # recursively download files
        cached_paths = self._recursive_download(prefix=prefix, caching_dir=caching_dir)

        if len(cached_paths) == 0:
            raise FileNotFoundError()

        if len(cached_paths) > 1:
            if os.path.isfile(compressed_caching_dir):
                os.remove(compressed_caching_dir)

            # compress here!
            shutil.make_archive(caching_dir, 'gztar', caching_dir)

            # we don't need to keep the uncompressed dir
            shutil.rmtree(caching_dir)

            # return compressed destination
            return LruPathEntry(compressed_caching_dir)
        else:
            return LruPathEntry(cached_paths[0])

    def splash(self):
        msg = ('<p>You can request files matching any of the following buckets:</p>',
               '<ul>',
               '\n'.join(f'<li>{bucket}</li>' for bucket in self.buckets),
               '</ul>'
               f'<p>{self.download_prefix.cache_info()}</p>')
        return '\n'.join(msg)

    def access_error(self, path):
        return f"<p>I am afraid I cannot access `{path}`, Dave.</p>", 405

    def not_found_error(self, path):
        return f"<p>There is nothing at `{path}`.</p>", 405

    def generic_error(self, error):
        return str(type(error).__name__) + '\n' + str(error.args[0]), 500

    def get_s3_prefix(self, path: str = None):
        try:
            if path is None:
                return self.splash()
            if not any(path.startswith(p) for p in self.buckets):
                return self.access_error(path)
            if not self.fs.exists(path):
                return self.not_found_error(path)

            cached_path = self.download_prefix(path)
            return send_file(str(cached_path), as_attachment=True, max_age=86400)

        except PermissionError as e:
            logger.error(e)
            return self.access_error(path)
        except Exception as e:
            logger.error(e)
            return self.generic_error(e)


@click.command()
@click.option('-b', '--bucket', required=True, multiple=True, type=str)
@click.option('-s', '--server', default='127.0.0.1', type=ipaddress.ip_address)
@click.option('-p', '--port', default=5000, type=int)
@click.option('-c', '--cache-prefix', default=tempfile.gettempdir(), type=str)
@click.option('-z', '--cache-size', default=100, type=int)
def main(bucket: Sequence[str],
         server: Union[ipaddress.IPv4Address, ipaddress.IPv6Address],
         port: int,
         cache_prefix: str,
         cache_size: int):

    flask_app = Flask(__name__)

    fws = FlaskWebServer(buckets=(bucket, ) if isinstance(bucket, str) else bucket,
                         flask_app=flask_app,
                         cache_prefix=cache_prefix,
                         cache_size=cache_size)
    fws.print_buckets()

    flask_app.run(host=str(server), port=port)


if __name__ == '__main__':
    main()
