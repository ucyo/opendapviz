import json
import re
from datetime import datetime
from functools import partial

import geoviews as gv
import geoviews.feature as gf
import holoviews as hv
import numpy as np
import xarray as xr
from bokeh.io import curdoc
from bokeh.layouts import layout, column, row
from bokeh.models import ColumnDataSource, TableColumn, DataTable, Button, Panel, Div, DatePicker, Tabs, HoverTool
from bokeh.models.widgets import Toggle, Slider, TextInput
from cartopy import crs as ccrs

hv.extension('bokeh')
from util import read_file, logger, DotDict


def value_changed(attr, old, new):
    print("Previous label: ", old)
    print("Updated label: ", new)


def _single_selection(source, on_selected):
    def _selected(attr, old, new):
        print(attr, old, new)
        if len(new) > 1:
            source.selected.indices = [new[-1]]
        elif len(new) == 1:
            on_selected(new[-1])

    return _selected


def toggle_handler(toggled):
    print(toggled)


status_bar = Div(text="Select a varaible to plot",
                 style={"color": "white", "position": "fixed", "top": 0, "left": 0, "right": 0,
                        "background": "blue", "line-height": "30px", "z-index": 1000, "padding-left": 16}, height=30)


def load_file(index_file_name):
    index = json.loads(read_file(index_file_name, "r"))

    def date_range_change(is_start, attr, old, new):
        print(is_start, new)
        start = startDate.value
        start = datetime(start.year, start.month, start.day, 0, 0, 0)
        end = endDate.value
        end = datetime(end.year, end.month, end.day, 23, 59, 59)
        print(type(start), end)
        filter_func = lambda ds: any(map(lambda dt: start <= dt and dt <= end, ds["_time_values"]))
        dsTable.filter_datasets(filter_func)

    def name_filter_changed(attr, old, new):
        regex = re.compile(new)
        filter_func = lambda ds: regex.search(ds["id"])
        dsTable.filter_datasets(filter_func)

    class DatasetsTable:

        def __init__(self, index):
            self.meta_data = DotDict(index["meta"])
            self.datasets = index["datasets"]
            self.meta_variables = self.meta_data.variables
            self._fill_variables_table()
            self._fill_datasets_table()

        def _fill_variables_table(self):
            self.vars_short_names = list(self.meta_variables.keys())

            vars_names = list(map(self.to_long_name, self.vars_short_names))
            vars_dims = list(
                map(lambda v: ", ".join(list(map(self.to_long_name, v["shape"]))), self.meta_variables.values()))
            self.vars_data = dict(names=vars_names, dims=vars_dims, )
            self.vars_source = ColumnDataSource(self.vars_data)
            vars_columns = [
                TableColumn(field="names", title="Variable name"),
                TableColumn(field="dims", title="Variable dimensions"),
            ]
            self.vars_table = DataTable(source=self.vars_source, columns=vars_columns, width=600, height=350,
                                        selectable=True)
            # vars_source.selected.on_change('indices', table_changed)

        def to_long_name(self, name, show_units=False):

            if name not in self.meta_variables:
                return name

            attr = self.meta_variables[name]["attributes"]

            if "long_name" in attr:
                name = attr["long_name"]["value"]
            elif "standard_name" in attr:
                name = attr["standard_name"]["value"]

            if show_units and "units" in attr:
                return "%s [%s]" % (name, attr["units"]["value"])
            return name

        def filter_datasets(self, filter_func):
            self.filtered_datasets = list(filter(filter_func, self.datasets))
            self.datasets_source.data.update(self._populate_datasets_table_data(self.filtered_datasets))

        def _fill_datasets_table(self):
            # preprocess the time values
            max_date = min_date = None

            for ds in self.datasets:
                ds["_time_values"] = list(map(lambda t: np.datetime64(t).astype(datetime), ds["data"]["time"]))
                mx = max(ds["_time_values"])
                mn = min(ds["_time_values"])
                if max_date is None or mx > max_date:
                    max_date = mx
                if min_date is None or mn < min_date:
                    min_date = mn

            self.filtered_datasets = self.datasets
            self.datasets_min_date = min_date
            self.datasets_max_date = max_date
            self.datasets_source = ColumnDataSource(self._populate_datasets_table_data(self.filtered_datasets))
            datasets_columns = [
                TableColumn(field="names", title="Name", width=600),
                TableColumn(field="dates", title="Date", width=600)

            ]
            self.datasets_table = DataTable(source=self.datasets_source, columns=datasets_columns, width=600,
                                            height=350,
                                            selectable=True)

            # datasets_source.selected.on_change('indices', _single_selection(datasets_source, lambda index: update_available_vars(
            #    filtered_datasets[index]["meta"])))

        def _populate_datasets_table_data(self, datasets):
            ds_names = []
            ds_dates = []
            for ds in datasets:
                ds_names.append(ds["id"])
                ds_dates.append(ds["data"]["time"][0])  # todo format date
            return {"names": ds_names, "dates": ds_dates}

        def get_plot_infos(self):
            vars_index = self.vars_source.selected.indices
            ds_index = self.datasets_source.selected.indices
            if not vars_index or not ds_index:
                log("Nothing selected!")
                return
            var_name = self.vars_short_names[vars_index[0]]
            var = self.meta_variables[var_name]
            ds = self.filtered_datasets[ds_index[0]]
            return (ds, var_name, var["shape"])

    def gen_plot():
        infos = dsTable.get_plot_infos()
        if infos is None:
            return
        ds, var_name, shape = infos
        ds_uri = ds["id"]
        timestamp = ds["data"]["time"][0]
        file_name = ds_uri.split("/")[-1]
        kdims = shape
        vdims = [var_name]

        lon_key, lat_key = None, None
        for key in dsTable.meta_variables:
            entry = dsTable.meta_variables[key]
            if "attributes" not in entry or "standard_name" not in entry["attributes"]:
                continue
            if entry["attributes"]["standard_name"]["value"] == "longitude":
                lon_key = key

            if entry["attributes"]["standard_name"]["value"] == "latitude":
                lat_key = key

        if lat_key not in kdims or lon_key not in kdims:
            log("'lat' and 'lon' are required dimensions!")
            return
        full_url = index["opendap_url"] + ds_uri
        log("Opening dataset: " + full_url)
        btn_plot_lonXlat.disabled = True
        try:
            print("Opening : " + full_url)
            dataset = xr.open_dataset(full_url)
            log("Dataset successfully opened. Loading data...")
            kdimsSingularValue = list(filter(lambda dim: dataset[dim].size == 1, kdims))
            kdimsMultipleValues = list(filter(lambda dim: dataset[dim].size > 1, kdims))
            indexers = {key: dataset[key].values[0] for key in kdimsSingularValue}
            print(indexers)
            dataset = dataset.sel(indexers=indexers)
            print(kdimsMultipleValues, kdimsSingularValue)

            xr_dataset = gv.Dataset(dataset[var_name], group=dsTable.to_long_name(var_name, True) + "  ",
                                    crs=ccrs.PlateCarree())
            image = xr_dataset.to(gv.Image, [lon_key, lat_key], dynamic=True)

            graph = image.options(colorbar=True, tools=['hover'],cmap="viridis", width=800, height=640, colorbar_position="right",
                                  toolbar="below") * gf.coastline()
            renderer = hv.renderer('bokeh')
            hover = HoverTool(tooltips=[
                ("(x,y)", "(@lon{%0.1f}, @lat{%0.1f})"),
                ('desc', '@' + var_name),
            ], formatters={
                'y': 'printf',  # use 'datetime' formatter for 'date' field
                'x': 'printf',  # use 'printf' formatter for 'adj close' field
                # use default 'numeral' formatter for other fields
            } )
            plot = renderer.get_plot(graph )

            if len(kdimsMultipleValues) > 2:

                # callback_policy="mouseup" for slider in plots

                print(plot)
                plot = renderer.get_widget(plot, "server")
                bokeh_layout = plot.init_layout()
                print(bokeh_layout)

                latFull = dsTable.meta_variables[lat_key]["attributes"]["standard_name"]["value"]
                lonFull = dsTable.meta_variables[lon_key]["attributes"]["standard_name"]["value"]

                bk_plot = bokeh_layout.children[0]
                #bk_plot.add_tools(hover)
                bk_slider = bokeh_layout.children[1].children[1]
                print(bk_slider.callback_policy)
                bk_slider.callback_policy = "mouseup"
                bk_plot.xaxis.axis_label = lonFull
                bk_plot.yaxis.axis_label = latFull
                print(lonFull, latFull)
                # bk_plot.xaxis[0].formatter = NumeralTickFormatter(format="0.0")

                # bk_plot.yaxis[0].formatter = NumeralTickFormatter(format="$0")
            else:
                bokeh_layout = plot.state

            tab = Panel(title=timestamp, child=bokeh_layout)
            plotTabs.tabs.append(tab)

            log("Data successfully loaded!")
        except Exception as e:
            log("Failed to open or process dataset: %s" % full_url, e)
        finally:
            btn_plot_lonXlat.disabled = False

    dsTable = DatasetsTable(index)
    btn_plot_lonXlat = Button(label="Plot variable over 'lon'x'lat' (this may take some time)")
    btn_plot_lonXlat.on_click(gen_plot)

    startDate = DatePicker(title="Start date", min_date=dsTable.datasets_min_date, max_date=dsTable.datasets_max_date,
                           value=dsTable.datasets_min_date)
    endDate = DatePicker(title="End date", min_date=dsTable.datasets_min_date, max_date=dsTable.datasets_max_date,
                         value=dsTable.datasets_max_date)
    startDate.on_change("value", partial(date_range_change, True))
    endDate.on_change("value", partial(date_range_change, False))

    plotTabs = Tabs(tabs=[], width=1000, height=640, )

    plotLayout = column(plotTabs, name="plotLayout")
    mainLayout = column(Div(height=50, style={"height": 50}), row(startDate, endDate), dsTable.datasets_table,
                        dsTable.vars_table, btn_plot_lonXlat,
                        plotLayout, status_bar, name='mainLayout')

    doc.remove_root(loadLayout)
    doc.add_root(mainLayout)


def log(msg, ex=None):
    status_bar.text = msg
    if ex is None:
        logger.debug(msg)
    else:
        logger.exception(msg, ex)


doc = curdoc()

log("Load an index file to get started.")
btnLoad = Button(label="Load")
btnLoad.on_click(lambda: load_file(txt_file.value))
txt_file = TextInput(value="index_201x.json", title="Specify index file to load")
loadLayout = column(Div(height=50, style={"height": "50px"}), txt_file, btnLoad, status_bar)

doc.add_root(loadLayout)  # [plot.init_layout()]
