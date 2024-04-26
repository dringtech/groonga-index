import json
import logging
import multiprocessing as mp
import subprocess
import time

from poyonga import Groonga
from tqdm import tqdm

from .source import SourceFile

logger = mp.log_to_stderr()
# Todo work out how to set formatter... %(asctime)s:%(funcName)s:%(levelname)s:%(message)s


def init(port_nums_queue):
    global port
    port = port_nums_queue.get()
    logger.info("Starting groonga database %s on port %d", _database, port)
    ret = subprocess.run(
        f"groonga -d --port {port} --protocol http {_database}".split(),
        capture_output=True)
    logger.info("Return code %s", ret.returncode)


def get_groonga():
    logger.debug("Running in %s against port %d",
                 mp.current_process().name, port)
    return Groonga(port=port)


def setup_tables():
    g = get_groonga()
    ret = g.call("table_remove", name="Words")
    logger.info('Removed Words lexicon - return code %s', ret.status)
    ret = g.call("table_remove", name="Site")
    logger.info('Removed Site table - return code %s', ret.status)
    ret = (
        g.call("table_create", name="Site",
               flags="TABLE_HASH_KEY", key_type="ShortText"),
        g.call("column_create", table="Site", name="text",
               type="Text", flags="COMPRESS_ZSTD")
    )
    logger.info(f'Create site table: { [r.status for r in ret] }')


def create_index():
    logLevel = logger.getEffectiveLevel()
    g = get_groonga()
    logger.setLevel(logging.INFO)
    ret = g.call('thread_limit', max=8)
    logger.info('Set thread limit %s', ret)
    ret = (
        g.call("table_create", name="Words", flags="TABLE_PAT_KEY", key_type="ShortText",
               default_tokenizer="TokenBigram", normalizer="NormalizerAuto"),
        g.call("column_create", table="Words", name="site_text",
               flags="COLUMN_INDEX|WITH_POSITION", type="Site", source="text")
    )
    logger.info(f'Create site lexicon: { [r.status for r in ret] }')
    ret = g.call('thread_limit', max=1)
    logger.info('Set thread limit %s', ret)
    logger.setLevel(logLevel)


def load_row_group(row_group):
    g = get_groonga()
    max_row = min(row_group + _row_group_size,
                  _row_group_count)
    logger.info("Loading %s row group %s to %s (%s)", _source,
                row_group, max_row, port)
    # TODO Externalise the source column names
    data = SourceFile(_source).read_row_groups(range(row_group, max_row), columns=[
        'URL', 'WebText']).rename_columns(['_key', 'text'])
    logger.debug("Read %s rows (columns %s)", data.num_rows, data.column_names)
    ret = g.call('load', table='Site', values=data.to_pylist())
    logger.debug("Completed load of %s: %s rows", row_group, ret.body)
    return {"rows": ret.body, "n_groups": max_row - row_group, "elapsed": ret.elapsed}


def report_status():
    g = get_groonga()
    site = g.call("object_inspect", name="Site")
    lexicon = g.call("object_inspect", name="Words")
    return {
        "site": {
            "records": site.body['n_records']
        },
        "lexicon": {
            "records": lexicon.body['n_records']
        }
    }


def stop_groonga(p):
    logger.info("Stopping groonga")
    groonga = Groonga(port=p)
    groonga.call("shutdown", mode="immediate")
    logger.info("Stopped")


def control(source, database, pool_size=16, row_group_limit=None, row_group_size=1):
    global _source, _database, _row_group_size, _row_group_count
    _source = source
    _database = database
    _row_group_size = row_group_size

    source_row_groups = SourceFile(_source).num_row_groups

    _row_group_count = row_group_limit or source_row_groups
    if _row_group_count > source_row_groups:
        logger.warning("Requested more row groups than are in the source file (%s > %s)",
                       _row_group_count, source_row_groups)
        _row_group_count = source_row_groups

    first_port = 10042
    port_nums = [x for x in range(first_port, first_port + pool_size)]
    config_queue = mp.Queue()
    for i in port_nums:
        config_queue.put(i)

    with mp.Pool(pool_size, init, (config_queue, )) as p:
        logger.setLevel(logging.INFO)

        # Create the tables (also removes existing table and lexicon)
        p.apply(setup_tables)

        # Process each row group in the dataset
        start = time.perf_counter()

        logger.info("%s has %d row groups (processing %s)", _source,
                    source_row_groups, _row_group_count)

        with tqdm(total=_row_group_count) as progress:
            for res in p.imap_unordered(load_row_group, range(
                    0, _row_group_count, _row_group_size)):
                progress.update(res['n_groups'])
        logger.info("Loaded table in %f seconds", time.perf_counter() - start)

        # Create the index
        logger.info("Creating index")
        start = time.perf_counter()
        p.apply(create_index)
        logger.info("Created index in %f seconds", time.perf_counter() - start)

        # Report the status of the index
        status = p.apply(report_status)
        logger.info('index status: %s', json.dumps(status, indent=2))

        # Stop all the Groonga servers
        for port in port_nums:
            p.apply(stop_groonga, (port,))
