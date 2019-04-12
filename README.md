# OpenDap data preprocessor and visualizer

**NOTE:** this code is meant to work with data from the Karlsruhe Institute of Technology. If you are not a member of the KIT you will likely run into issues.


## Installation

The code runs on python 3. Best used with conda spec file: `spec-file.txt` or install dependencies via `requirements.txt`.

## Run the bokeh app

```bash
bokeh serve --show viz_app.py
```

This should open a new browser tab. You need to be connected to the KIT VPN.

## Usage

1. Select a dateset to use
2. Select the variable to plot. Note: plotting requires lat and lon dimensions!
3. Click the Plot button
4. Use the slider to change layers where available 
5. Select a different variable and repeat


## In case of segfaults with shapely

There were some issues with segfaults in the libgeos library and shapely.
This seemed to fix that:

```bash
pip uninstall shapely; pip install --no-binary :all: shapely

```


# Data precosessing
Load data from the server http://eos.scc.kit.edu/ and save it to test_index.json. Using the specifed filters and paths:

```bash
python preprocess.py http://eos.scc.kit.edu/ test_index.json --catalog-folder=thredds/catalog/ --base-folder=polstracc0new/ --catalog-include=201603220 --dataset-include=grid_reg_DOM01_ML_00 --local-cache-dir=.cache_test --modify-timestamp=excel

```
