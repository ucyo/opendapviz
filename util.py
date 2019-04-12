import logging
from typing import Union
from pathlib import Path

logger = logging.getLogger("opendapViz")


def read_file(file_path: Union[str, Path], mode="rb"):
    file_path = Path(file_path) if isinstance(file_path, str) else file_path

    if not file_path.exists():
        return None

    with open(str(file_path), mode) as file:
        content = file.read()
    logger.debug("Read '%s' from local cache" % file_path)
    return content


def write_file(content, full_path: Union[str, Path], mode="wb"):
    full_path = Path(full_path) if isinstance(full_path, str) else full_path
    parent_dir = full_path.parent
    if not parent_dir.exists():
        try:
            parent_dir.mkdir(parents=True)
        except Exception:
            pass

    with open(str(full_path), mode) as file:
        file.write(content)
    logger.debug("File written to: %s", full_path)



class DotDict(dict):

    def __getitem__(self, item):
        return self.get(item)

    def __getattr__(self, item):
        return self.get(item)


"""Transform datetime of EXCEL-float to Python object."""

import pandas as pd
import decimal as dc


def _excel2timestamp(excel, *args, **kwargs) ->pd.Timestamp:
    try:
        if isinstance(excel, (float)):
            excel = str(dc.Decimal(excel))
        if '.' not in excel:
            day,perc = excel, '0'  # if it is an integer
        else:
            excel = excel + '0'
            day, perc = excel.split('.')
        return pd.Timestamp(day, *args,
                            **kwargs)+pd.Timedelta('1 day')*float('.'+perc)
    except:
        message = 'Expected <float> or <str>, got {}'.format(type(excel))
        raise TypeError(message)


def excel2time(excel, mode='numpy', *args, **kwargs):
    """Transform datetime of EXCEL-float to Python object."""
    timestamp = _excel2timestamp(excel, *args, **kwargs)
    if mode in ('python', 'datetime'):
        return timestamp.to_pydatetime()
    elif mode in ('numpy', 'np'):
        return timestamp.to_datetime64()
    elif mode in ('julian'):
        return timestamp.to_julian_date()
    else:
        message = 'Expected "numpy, julian, datetime", got "{}"'.format(mode)
        raise ValueError(message)