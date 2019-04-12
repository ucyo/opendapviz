import concurrent.futures
import logging
import re
from typing import List

from data.model import catalog_from_xml_data
from data.provider import CachedOrRemoteProvider
from data.ncml_parser import parse_ncml_file

logger = logging.getLogger("opendapViz")


class Filter:
    def test(self, to_test: str, **kwargs) -> bool:
        """
        Returns true if the test was passed.

        :param to_test:
        :param kwargs:
        :return:
        """
        raise NotImplementedError("Must be implemented")


class Exclude(Filter):

    def __init__(self, to_ignore):
        self._ignore_regex = re.compile(to_ignore)

    def test(self, to_test: str, **kwargs) -> bool:
        return not bool(self._ignore_regex.search(to_test))


class Include(Filter):

    def __init__(self, to_ignore):
        self._ignore_regex = re.compile(to_ignore)

    def test(self, to_test: str, **kwargs) -> bool:
        return bool(self._ignore_regex.search(to_test))


class Loader:

    def __init__(self, cache_dir, base_url, catalog_url_part):
        self.catalog_url_part = catalog_url_part
        self.provider = CachedOrRemoteProvider(cache_dir, base_url)
        self.catalog_base_uri = ""
        self._catalog_filters = []
        self._dataset_filters = []
        self._catalog_refs_to_load = []
        self._dataset_metas_to_load = []
        self.loaded_catalogs = []
        self.loaded_dataset_metas = []
        self.opendap_base_url = ""

    def _load_catalog(self, catalog_uri):
        logger.debug("Loading catalog: %s" % catalog_uri)
        catalog = catalog_from_xml_data(
            self.provider.get_xml_data(self.catalog_url_part + self.catalog_base_uri + catalog_uri))
        self._apply_filters(self._catalog_filters, catalog.catalog_refs, "id", self._queue_catalog_refs_to_load)
        self._apply_filters(self._dataset_filters, catalog.datasets, "id", self._queue_datasets_to_load)

        self.ncml_base_url = catalog.ncml_base_url
        if catalog.opendap_base_url is not None:
            self.opendap_base_url = catalog.opendap_base_url

        self._load_queued_catalog_refs()
        self._load_queued_dataset_metas()
        return catalog

    def load_opendap_data(self, uri, variable, count, parse_values=lambda x: x):
        data = self.provider.get_str_data(
            self.opendap_base_url + uri + ".ascii?%s[0:1:%d]" % (variable, count - 1)).decode("utf-8")
        # Probably not the most stable way...
        parts = data.strip().split("\n")
        values = parts[-1].split(",")
        if parse_values is not None:
            return tuple(map(parse_values, values))
        return values

    def _load_dataset_meta(self, dataset_uri):
        if self.ncml_base_url is not None:
            ncml_url = self.ncml_base_url + dataset_uri
            logger.debug("Loading dataset meta: %s" % ncml_url)
            return parse_ncml_file(self.provider.get_xml_data(ncml_url), dataset_uri)
        else:
           raise NotImplementedError("NCML service endpoint required.")

    def _apply_filters(self, filters, iterable, attr_name, on_success):
        if len(filters) == 0:
            rem = iterable[:]
        else:
            rem = list(
                filter(lambda item: all(map(lambda filter: filter.test(getattr(item, attr_name), item=item), filters)),
                       iterable))
        logger.debug("Filter result: %d/%d" % (len(rem), len(iterable)))
        on_success(rem)

    def add_filter(self, f: Filter, type="dataset"):
        if type in ("both", "dataset"):
            self._dataset_filters.append(f)
        if type in ("both", "catalog"):
            self._catalog_filters.append(f)

    def load_catalog_recursively(self, base_uri, uri):
        self.catalog_base_uri = base_uri
        return self._load_catalog(uri)

    def _queue_catalog_refs_to_load(self, refs: List):
        logger.debug("Queued: %d catalogs" % len(refs))
        self._catalog_refs_to_load = refs

    def _queue_datasets_to_load(self, ds):
        logger.debug("Queued: %d datasets" % len(ds))
        self._dataset_metas_to_load = ds

    def _load_queued_catalog_refs(self):
        refs = self._catalog_refs_to_load[:]

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            future_to_url = {executor.submit(self._load_catalog, ref.href): ref.href for ref in refs}
            results = self._handle_future_result(future_to_url)

        self._catalog_refs_to_load.clear()
        self.loaded_catalogs.extend(results)

    def _load_queued_dataset_metas(self, ):
        ds_to_load = self._dataset_metas_to_load[:]

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            future_to_url = {executor.submit(self._load_dataset_meta, ds.url_path): ds.url_path for ds in ds_to_load}
            results = self._handle_future_result(future_to_url)

        self._dataset_metas_to_load.clear()
        self.loaded_dataset_metas.extend(results)

    def _handle_future_result(self, future_to_url):
        results = []
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                data = future.result()
            except Exception as exc:
                #logger.exception('%r generated an exception' % (url), exc)
                raise exc
            else:
                results.append(data)

        return results
