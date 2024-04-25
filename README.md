# groonga-index

## Pre-requisites

1. Install `groonga`, following appropriate instructions at https://groonga.org/docs/install.html
2. Have a working `python3` environment. See https://wiki.python.org/moin/BeginnersGuide/Download for help.
3. Install the requirements as specified in `requirements.txt`.
   Might be useful to have a virtual environment to manage this.
   A `Pipfile` is included for use with `pipenv` (https://pipenv.pypa.io/en/latest/installation.html)

## Using the loader

Create a blank database

```sh
python -m index_loader -n <db_path>
```

Load a parquet file (which must contain the columns `URL` and `WebText`).

```sh
python <db_path> <source_parquet_path>
```

The following options can be passed to limit or tune the load:

* `-r <n>` Specify number of row groups to process - defaults to all in source file.
* `-s <n>` Specity number of row groups per batch. Defaults to 1. Can increase memory usage.
