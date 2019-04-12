import json
import logging
import re
from functools import partial
from typing import List, Dict, Callable

from lxml import etree
from xarray import Dataset
from xarray.core.utils import decode_numpy_dict_values, ensure_us_time_resolution

from util import write_file, DotDict

logger = logging.getLogger("opendapViz")


def dataset_info_from_xml_element(ns, xml_element: etree.ElementTree, parent=None):
    parent = parent
    name = xml_element.get("name")
    id = xml_element.get("ID")
    if id is None:
        id = name
    url_path = xml_element.get("urlPath")
    size_element = xml_element.find("{%s}dataSize" % ns)
    size = "size unknown"
    if size_element is not None:
        size = size_element.text + size_element.get("units")

    dsi = XMLDatasetInfo(id, name, parent, url_path, size)
    child_xml_elements = xml_element.findall('.//{%s}dataset' % ns)
    dsi.children = list(map(lambda child: dataset_info_from_xml_element(ns, child, dsi), child_xml_elements))
    return dsi


def parse_catalog_ref(element: etree.ElementTree):
    xlink_ns = element.nsmap.get("xlink")
    href = element.get("{%s}href" % xlink_ns)
    title = element.get("{%s}title" % xlink_ns)
    id = element.get("ID")
    if id is None:
        id = title
    return DotDict(href=href, title=title, id=id)


def catalog_from_xml_data(data):
    if isinstance(data, str):
        xml_element = etree.fromstring(data)
    xml_element = data
    _ns = xml_element.nsmap.get(None, "")
    # logger.debug("Using XML namespace: %s", ns)
    opendap_service = xml_element.find('.//{%s}service[@serviceType="OPENDAP"]' % _ns)
    ncml_service = xml_element.find('.//{%s}service[@serviceType="NCML"]' % _ns)
    opendap_base_url = None
    if opendap_service is not None:
        opendap_base_url = opendap_service.get("base")
    ncml_base_url = None
    if ncml_service is not None:
        # if ncml_service is not None:
        ncml_base_url = ncml_service.get("base")

    # According to the XSD this is a mandatory field ?!
    base_dataset_element = xml_element.find('{%s}dataset' % _ns)
    name = base_dataset_element.get("name")
    catalog_refs = base_dataset_element.findall('.//{%s}catalogRef' % _ns)
    dataset_elements = base_dataset_element.findall('.//{%s}dataset' % _ns)

    datasets = list(map(partial(dataset_info_from_xml_element, _ns), dataset_elements))
    refs = list(map(parse_catalog_ref, catalog_refs))
    # get all datasets and sub-catalog catalogRef-s
    dataset = dataset_info_from_xml_element(_ns, base_dataset_element, None)
    return CatalogInfo(name, datasets, refs, opendap_base_url, ncml_base_url)


def dataset_from_json(jdata):
    j_coords = jdata.get("coords", dict())
    j_data_vars = jdata.get("data_vars", {})
    attrs = jdata.get("attrs", {})
    dims = jdata.get("dims", {})

    coords = Dataset.from_dict(j_coords)  # {name: DatasetVariable(name, **j_coords[name]) for name in j_coords}
    data_vars = {name: DatasetVariable(name, **j_data_vars[name]) for name in j_data_vars}

    return DatasetMeta(attrs, dims, coords, data_vars)


def dataset_meta_to_dict(ds, save_data=False, coords_as_dataset=True):
    # copied from xarray.dataset.to_dict without the data export
    d = {'coords': {}, 'attrs': decode_numpy_dict_values(ds.attrs),
         'dims': dict(ds.dims), 'data_vars': {}}

    if coords_as_dataset:
        d['coords'] = ds.coords.to_dataset().to_dict()  # keep coordinates as own dataset!
    else:
        for k in ds.data_vars:
            entry = {'dims': ds[k].dims, 'attrs': decode_numpy_dict_values(ds[k].attrs)}
            if save_data:
                entry["data"] = ensure_us_time_resolution(ds[k].values).tolist()
            d['coords'].update({k: entry})

    for k in ds.data_vars:
        entry = {'dims': ds[k].dims, 'attrs': decode_numpy_dict_values(ds[k].attrs)}
        if save_data:
            entry["data"] = ensure_us_time_resolution(ds[k].values).tolist()
        d['data_vars'].update({k: entry})
    return d


class XMLDatasetInfo:

    def __init__(self, id, name, parent, url_path, size):
        self.parent = parent
        self.name = name
        self.id = id
        self.url_path = url_path
        self.size = size
        self.children = []

    def __eq__(self, other):
        if not isinstance(other, XMLDatasetInfo):
            return False

        return self.id == other.id

    def __repr__(self):
        return "%s %s (%d children)" % (
            self.id, self.size, len(self.children))  # + "\n\t".join(map(str, self.children))


class CatalogInfo:

    def __init__(self, name, datasets, catalog_refs, opendap_base_url, ncml_base_url):
        self.name = name
        self.opendap_base_url = opendap_base_url
        self.ncml_base_url = ncml_base_url
        self.datasets = datasets
        self.catalog_refs = catalog_refs

    def filter_child_datasets(self, pattern):
        p = re.compile(pattern)
        return filter(lambda child: p.search(child.name), self.datasets)

    def __repr__(self):
        return "%s (%d catalog refs and %d datasets)" % (self.name, len(self.catalog_refs), len(self.datasets))


class DatasetVariable:

    def __init__(self, name, dims, attrs, data=None):
        self._name = name
        self.dims = dims
        self.attrs = attrs
        self.data = data or {}

    @property
    def name(self):
        return self._name

    def __eq__(self, other: "DatasetVariable"):
        return self._name == other._name

    def __hash__(self):
        return self._name.__hash__()


class DatasetMeta(object):

    def __init__(self, attrs, dims, vars):
        self.dimensions = dims
        self.attributes = attrs
        self.variables = vars

    def toJson(self):
        return {
            "dimensions": self.dimensions,
            "variables": self.variables,
            "attributes": self.attributes}

    @staticmethod
    def mismatched_dimensions(dims1, dims2):
        return set(dims1) ^ set(dims2)

    @staticmethod
    def compare_lists_as_set(d1, d2):
        s1 = set(d1)
        s2 = set(d2)
        return s1 ^ s2

    @staticmethod
    def compare_dicts(d1, d2, cmp_func=lambda v1, v2: v1 == v2):
        s1 = set(d1.keys())
        s2 = set(d2.keys())
        common = s1 & s2
        uncommon = list(s1 ^ s2)
        mismatched = []
        if cmp_func is not None:
            mismatched = list(filter(lambda key: not cmp_func(d1[key], d2[key]), common))
        return mismatched + uncommon

    def __eq__(self, other):

        if not isinstance(other, DatasetMeta):
            return False

        nc_dv = self.compare_dicts(self.variables, other.variables, self._compare_var)
        if nc_dv:
            logger.debug("Data variables mismatch: %s" % nc_dv)
            return False

        ncd = self.compare_dicts(self.dimensions, other.dimensions, None)
        if ncd:
            logger.debug("Dimension mismatch: %s" % (ncd))
            return False
        """
        nc_attrs = self.compare_dicts(self.attributes, other.attributes)
        if nc_attrs:
            logger.debug("Attributes mismatch : %s -> IGNORED" % (nc_attrs))
        """
        return True

    def _compare_var(self, var, other_var):

        if var.type != other_var.type:
            logger.debug("Mismatching types!")
            return False

        mismatched_dims = self.compare_lists_as_set(var.shape, other_var.shape)
        if mismatched_dims:
            logger.debug("Variable `%s` dimension mismatch: %s" % (var.name, mismatched_dims))
            return False

        mismatched_attrs = self.compare_dicts(var.attributes, other_var.attributes)
        if mismatched_attrs:
            logger.debug("Variable `%s` attributes mismatch : %s -> IGNORED" % (var.name, mismatched_attrs))

        return True


class DatasetInfo(object):

    def __init__(self, id, meta: DatasetMeta):
        self.id = id
        self.meta = meta


class MetaInformationMismatchError(Exception):
    pass


class DatasetsIndex:

    def __init__(self, base_url: str, loader, coordinate_data_to_retrieve: Dict[str, Callable]):
        self.base_url = base_url
        self.loader = loader
        self.meta_information = None
        self.datasets = []
        self.coordinate_data_to_retrieve = coordinate_data_to_retrieve

    def add_dataset(self, dsi: DatasetInfo, keep_attributes=False):  # dsi: DatasetInfo,
        if self.meta_information is None:
            self.meta_information = dsi.meta
            for to_keep in self.coordinate_data_to_retrieve:
                if to_keep not in dsi.meta.dimensions:
                    raise MetaInformationMismatchError("No such dimension to retrieve: " + to_keep)

        elif self.meta_information != dsi.meta:
            logger.info("Meta information mismatch! Ignoring dataset: " + dsi.id)
            return

        data = {}

        for to_keep in self.coordinate_data_to_retrieve:
            dim_count = int(dsi.meta.dimensions[to_keep])
            parse_func = self.coordinate_data_to_retrieve[to_keep]
            data[to_keep] = self.loader.load_opendap_data(dsi.id, to_keep, dim_count, parse_func)

        info = {"id": dsi.id, "data": data}  # "name": dsi.name, "url_path": dsi.url_path,
        if keep_attributes:
            info["attributes"] = dsi.meta.attributes
        self.datasets.append(info)

    def save(self, file_path):
        def parse(o):
            return o.toJson() if hasattr(o, "toJson") else o.__dict__

        jdata = json.dumps({"base_url": self.base_url, "opendap_url": self.base_url + self.loader.opendap_base_url,
                            "meta": self.meta_information,
                            "datasets": self.datasets}, default=parse)
        write_file(jdata, file_path, "w")
