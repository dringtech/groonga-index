import os
import sys
from getopt import getopt, GetoptError

from . import loader
from . import db
from . import source


def usage(rc=0):
    print('''
    index_loader

      usage:
        index_loader -i <source_parquet>
            Inspect the source parquet file

        index_loader -n <db_path>
            Create new blank database at specified path

        index_loader [-g <num_row_groups>] [-s <batch_size>] [-p <pool_size>] <db_path> <source_parquet>
            Load database with contents of source parquet
            
            -g Number of row groups to load (defaults to all)
            -s Row groups loaded per batch (defaults to 1)
            -p Size of pool to run (defaults to 4)
    ''', file=sys.stderr)
    exit(rc)


if __name__ == '__main__':
    try:
        opts, args = getopt(sys.argv[1:], "ing:p:s:")
    except GetoptError:
        usage(1)

    cmd_args = {}

    opts_dict = dict(opts)
    if '-i' in opts_dict:
        source_file = source.SourceFile(args[0])

        print( "*********************************************************************************************************\n"
              f"* File         * { source_file.path }\n"
              f"*   Row groups * { source_file.num_row_groups }\n"
              f"*   Columns    * { source_file.column_names }\n"
               "*********************************************************************************************************\n"
        )
        sys.exit(0)

    if '-n' in opts_dict:
        database = args[0]
        try:
            db.create_new_database(database)
        except:
            print(f'Failed to create new database { database }', file=sys.stderr)
            sys.exit(1)
        sys.exit(0)

    for o, a in opts:
        if o == '-g':
            cmd_args['row_group_limit'] = int(a)
        if o == '-p':
            cmd_args['pool_size'] = int(a)
        if o == '-s':
            cmd_args['row_group_size'] = int(a)

    try:
        cmd_args["database"], cmd_args['source'] = args
    except ValueError:
        usage(1)

    loader.control(**cmd_args)
