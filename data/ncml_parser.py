import logging
import os

from data.model import DatasetMeta, DatasetInfo
from util import DotDict

logger = logging.getLogger("ncmlParser")

from lxml import etree

ncml_namespace = 'http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2'


def process_attribute_tag(target, a):
    attr_name = a.attrib.get("name")
    if attr_name is None:
        logger.error("No 'name' attribute supplied on the <attribute /> tag.  Skipping.")
        return

    tipe = a.attrib.get("type")
    value = a.attrib.get("value")

    if value is not None:
        if tipe is not None:
            if tipe.lower() in ['float', 'double']:
                value = float(value)
            elif tipe.lower() in ['int', 'long', 'short']:
                try:
                    value = int(value)
                except Exception as ex:
                    logger.exception("Failed to parse value: ", ex)
        logger.debug("Setting attribute '{0}' to '{1!s}''".format(attr_name, value))

    return attr_name, value


def parse_ncml_file(ncml, id):
    # Based on: https://github.com/axiom-data-science/pyncml
    if isinstance(ncml, str) and os.path.isfile(ncml):
        root = etree.parse(ncml).getroot()
    elif isinstance(ncml, str):
        root = etree.fromstring(ncml)
    elif etree.iselement(ncml):
        root = ncml
    else:
        raise ValueError("Could not parse ncml. \
                         Did you pass in a valid file path, xml string, or etree Element object?")

    global_attributes = {}
    dimensions = {}
    variables = {}
    thredds_meta = {}
    thredds_xml_meta = root.find('.//{%s}group[@name="THREDDSMetadata"]' % ncml_namespace)
    if thredds_xml_meta is not None:
        id = thredds_xml_meta.find('.//{%s}attribute[@name="id"]' % ncml_namespace).get("value")
        od_service = thredds_xml_meta.find('.//{%s}attribute[@name="opendap_service"]' % ncml_namespace).get("value")
        thredds_meta = {"opendap_url": od_service, "id": id}

    # Variables
    for v in root.findall('{%s}variable' % ncml_namespace):

        var_name = v.attrib.get("name")
        attributes = {}

        for a in v.findall('{%s}attribute' % ncml_namespace):
            name, value = process_attribute_tag(attributes, a)
            attributes[name] = {"value": value}

        shape = v.attrib.get("shape").split(" ")
        variables[var_name] = DotDict(
            {"type": v.attrib.get("type"), "shape": shape, "attributes": attributes})

        # Global attributes
    for a in root.findall('{%s}attribute' % ncml_namespace):
        name, value = process_attribute_tag(global_attributes, a)
        global_attributes[name] = {"value": value}

    # Dimensions
    for d in root.findall('{%s}dimension' % ncml_namespace):
        dim_name = d.attrib.get('name')
        dim_length = d.attrib.get("length")
        dimensions[dim_name] = dim_length

    """return DotDict(
        {"variables": DotDict(variables), "dimensions": DotDict(dimensions), "attributes": DotDict(global_attributes),
         "thredds_meta": DotDict(thredds_meta)})
    """
    meta = DatasetMeta(global_attributes, dimensions, variables)
    return DatasetInfo(id, meta)


if __name__ == "__main__":
    agg = parse_ncml_file("playground/2016033000-ART-chemtracer_grid_reg_DOM01_ML_0021.ncml")
    print(agg)
    print(agg.variables.time)
