import json
import logging
from pathlib import Path
from typing import Union
from urllib.error import URLError
from urllib.request import Request, urlopen

from lxml import etree

from util import write_file, read_file

logger = logging.getLogger("opendapViz")


class ProviderError(Exception):

    def __init__(self, original_error, url):
        self.original_error = original_error


class Provider(object):
    def _get_raw_data(self, uri: str, **kwargs):
        raise NotImplementedError()

    def get_str_data(self, uri: str, **kwargs):
        return self._get_raw_data(uri, **kwargs,mode_postfix="b")

    def get_json_data(self, uri: str, **kwargs):
        return json.loads(self._get_raw_data(uri, **kwargs))

    def get_xml_data(self, uri, **kwargs):
        return etree.fromstring(self._get_raw_data(uri, mode_postfix="b", **kwargs))


class CachedProvider(Provider):

    def __init__(self, cache_dir: str):
        self.cache_dir = Path(cache_dir)
        self._get_catalog_data = self._read_cache_file

    def _read_cache_file(self, file_path: str, mode="rb") -> Union[str, bytes, None]:
        try:
            file_path = file_path.lstrip("/")
            return read_file(self.cache_dir / file_path, mode)
        except Exception as e:
            logger.exception("Failed to read file: %s", file_path)
            raise ProviderError(e, file_path)

    def _write_cache_file(self, file_path: str, content, mode="wb"):
        try:
            file_path = file_path.lstrip("/")
            write_file(content, self.cache_dir.joinpath(file_path), mode)
        except Exception as e:
            logger.exception(e, "Failed to write file: %s", file_path)
            raise ProviderError(e, file_path)

        return True

    def _get_raw_data(self, uri: str, **kwargs):
        mode = "r" + kwargs.get("mode_postfix", "")
        ext = kwargs.get("ext", "")
        return self._read_cache_file(uri + ext, mode)

    def _save_raw_data(self, file_path: str, content, **kwargs):
        mode = "w" + kwargs.get("mode_postfix", "")
        ext = kwargs.get("ext", "")
        self._write_cache_file(file_path + ext, content, mode)


class RemoteProvider(Provider):

    def __init__(self, base_url):
        self.base_url = base_url

    @staticmethod
    def request(url) -> bytes:
        request = Request(url)

        logger.debug("Requesting: %s", url)

        try:
            result = urlopen(request, timeout=10)
            data = result.read()
        except URLError as e:
            logger.exception("Failed to request url: %s", url)
            raise ProviderError(e, url)

        return data

    def _get_raw_data(self, uri, **kwargs):
        prefix = kwargs.get("prefix", "")
        return self.request(self.base_url + prefix + uri)


class CachedOrRemoteProvider(Provider):

    def __init__(self, cache_path, base_url, force_remote=False):
        self.cached_provider = CachedProvider(cache_path)
        self.remote_provider = RemoteProvider(base_url)
        self.force_remote = force_remote

    def _get_raw_data(self, uri: str, **kwargs):
        if not self.force_remote:
            raw_data = self.cached_provider._get_raw_data(uri, **kwargs)
            if raw_data is not None:
                return raw_data

        raw_data = self.remote_provider._get_raw_data(uri, **kwargs)
        kwargs["mode_postfix"] = "b"
        self.cached_provider._save_raw_data(uri, raw_data, **kwargs)

        return raw_data

