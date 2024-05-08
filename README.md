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

* `-g <n>` Specify number of row groups to process - defaults to all in source file.
* `-s <n>` Specify number of row groups per batch. Defaults to 1. Can increase memory usage.
* `-p <n>` Specify number of subprocesses to spawn. Defaults to 4. Can increase memory usage. This really only affects the initial data load, as indexing is single-threaded.

The `-i` flag allows you to inspect the source file prior to loading it.

### Accessing cloud storage

You can access files direct from the Azure Blob File Store by referencing as follows:

```sh
python -m index_loader -i abfs://commoncrawlextracts/2023-40-Global-full-zstd.parquet
```

This works for inspections and loads, although loads can be much slower.
`AZURE_STORAGE_ACCOUNT_NAME` and `AZURE_STORAGE_ACCOUNT_KEY` need to be set.

## Design notes

The command creates two Groonga objects:

* The `Site` table, which holds the `URL` and `WebText` from the Common Crawl extract.
* The `Words` table, which is the index of the `WebText`.

### `Site` table

The Site table is created with the flags `TABLE_HASH_KEY` and `KEY_LARGE`, per
[instructions to create large data store table](https://groonga.org/docs/reference/commands/table_create.html#create-large-data-store-table) on the Groonga docs site.

The effect of setting `KEY_LARGE` extends the `_key` field from a theoretical maximum of 4GB to 1TB.
Note that this is the storage size of the `_key`, not the number of entries.
The URL is loaded into the `_key`, so this can take some size.

To inspect this, run `groonga data/tdc.db object_inspect --output_pretty yes Site`.
The important result field is `max_total_size`.
Without setting the `KEY_LARGE` flag, the maximum size of the key field was reported as being 4,294,967,295.
Setting `KEY_LARGE` increased this to 1,099,511,627,775. It appears that the load job was failing as this space ran out.

The maxmimum number of records in a `TABLE_HASH_KEY` table is 536,870,912.

It may be more efficient to load the url into a separate url column and allow the `_key` to be auto-generated.

There is one column: `text`. This stores the text version of the Common Crawl.
It is created with the flags `COMPRESS_ZSTD`, and an implicit `COLUMN_SCALAR` (?).

It's possible to store a given column on a different location in the filesystem, by specifying the 
[`path` parameter](https://groonga.org/docs/reference/commands/column_create.html#path).
As stated, this could be useful in placing the Creative Commons text on a slower / cheaper disk, while keeping the index on fast disk.

#### Load results

Loading just the index (i.e. omitting the web text) (13492 parquet row groups) created 147,270,283 ids in 12 minutes and 47 seconds.

A column can only be 256GiB. Trying to load the whole source parquet file results in the load stopping at about 60%. This could be dealt with either by pre-processing the web text to make each web text smaller, or by [sharding](https://groonga.org/docs/reference/sharding.html#sharding). To load the sharded table, this script would need to change to accept a number of row groups and a start row group, or row groups per shard.

Thought: is it possible to "shard" across columns - i.e. first x characters in column a, next x characters in column b.

When indexing after the fact (offline indexing), the database index creates a temporary file,
which grows. This is then converted into 1GB size files, but during this process, you need to have
at least 2X the size of the index available.

```
INFO/MainProcess] source/2023-40-Global-full-zstd.parquet has 13492 row groups (processing 8000)
100%|█████████████████████████████████████| 8000/8000 [38:38<00:00,  3.45it/s]
[INFO/MainProcess] Loaded table in 2318.280167 seconds
[INFO/MainProcess] Creating index
[INFO/ForkPoolWorker-12] Set thread limit <GroongaResult status=0 start_time=1715098473.095662 elapsed=0.0005037784576416016>
[INFO/ForkPoolWorker-12] Create site lexicon: [0, 0]
[INFO/ForkPoolWorker-12] Set thread limit <GroongaResult status=0 start_time=1715127068.58626 elapsed=0.0003294944763183594>
[INFO/MainProcess] Created index in 28595.497092 seconds
[INFO/MainProcess] index status: {
  "site": {
    "records": 87382236
  },
  "lexicon": {
    "records": 73698208
  }
}
[INFO/MainProcess] process shutting down
```

This created a database with the following structure:

Object          | File prefix    | Size on disk (kb)
----------------|----------------|--------------
Site            | tdc.db.0000100 | 9,008,748
Site.text       | tdc.db.0000101 | 262,517,776
Words           | tdc.db.0000104 | 2,009,640
Words.site_text | tdc.db.0000105 | 178,341,320

Overall, the index is 431GiB.

## Searching

The database can be searched via the Groonga admin interface, visible on port 10041 after running the server:

```sh
groonga -s --protocol http data/tdc.db
```

Navigate to **Table &rarr; Site** on the left-hand menu, and expand the **Advanced search** panel.

![](./docs/groonga-search.png)
