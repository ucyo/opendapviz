import logging
import sys
from logging import DEBUG, StreamHandler

import numpy

from data.loader import Loader, Exclude, Include
from data.model import DatasetsIndex
from util import excel2time

logger = logging.getLogger("opendapViz")
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(DEBUG)

import click


@click.command()
@click.argument('url')
@click.argument('output-file')
@click.option('--catalog-folder', default="thredds/catalog/",
              help='Will be appended to the URL to point to the catalog folder.')
@click.option('--base-folder', default="", help="Start at this sub folder not the top hierachy")
@click.option('--dataset-include', default=None, help="Comma separated list of include filters for dataset names")
@click.option('--dataset-exclude', default=None)
@click.option('--catalog-include', default=None)
@click.option('--catalog-exclude', default=None)
@click.option('--local-cache-dir', default=".cache", help="Local cache folder (will be created)")
@click.option('--modify-timestamp', default="none", type=click.Choice(['none', 'excel']))
def load_catalog_data(url, base_folder, output_file, dataset_include, catalog_folder, catalog_include, catalog_exclude,
                      dataset_exclude,
                      local_cache_dir, modify_timestamp):
    """
    Recursively load data from the given server using ncml and opendap.
    """
    print(url, dataset_include, catalog_folder)

    loader = Loader(local_cache_dir, url, catalog_folder)

    if dataset_include is not None:
        for key in dataset_include.split(","):
            loader.add_filter(Include(key), "dataset")

    if dataset_exclude is not None:
        for key in dataset_exclude.split(","):
            loader.add_filter(Exclude(key), "dataset")

    if catalog_include is not None:
        for key in catalog_include.split(","):
            loader.add_filter(Include(key), "catalog")

    if catalog_exclude is not None:
        for key in catalog_exclude.split(","):
            loader.add_filter(Exclude(key), "catalog")

    # data_url = "http://eos.scc.kit.edu/"
    # data_url_noaa = "https://dods.ndbc.noaa.gov/"

    cat = loader.load_catalog_recursively(base_folder, "catalog.xml")

    format_kit_icon_timestamp = lambda tv: str(
        (excel2time(float(tv)) + numpy.timedelta64(500, 'ms')).astype("datetime64[s]"))
    format_none = lambda x: x

    format_func = format_none
    if modify_timestamp == "excel":
        format_func = format_kit_icon_timestamp

    index = DatasetsIndex(url, loader, {"time": format_func})

    count = len(loader.loaded_dataset_metas)
    for i, dsi in enumerate(loader.loaded_dataset_metas):
        logger.debug("Entry %s: %d of %d" % (dsi.id, i, count))
        index.add_dataset(dsi)
    index.save(output_file)


if __name__ == "__main__":
    load_catalog_data()
