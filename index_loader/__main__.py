import os
import sys
from getopt import getopt, GetoptError

from . import loader
from . import db


def usage(rc=0):
    print('''
    index_loader

      usage:
        index_loader -n <db_path>
            Create new blank database at specified path

        index_loader [-r <num_row_groups>] [-s <batch_size>] <db_path> <source_parquet>
            Load database with contents of source parquet
            
            -r Specify maximum number of row groups to load (defaults to all)
            -s Specify number of row groups loaded per batch (defaults to 1)
    ''', file=sys.stderr)
    exit(rc)

if __name__ == '__main__':
    try:
      opts, args = getopt(sys.argv[1:], "ng:s:")
    except GetoptError:
      usage(1)

    cmd_args = {}
    for o, a in opts:
        if o == '-n':
            try:
                db.create_new_database(args[0])
            except:
                print('Failed to create new database', file=sys.stderr)
                sys.exit(1)
            sys.exit(0)
        if o == '-g':
            cmd_args['row_group_limit'] = int(a)
        if o == '-s':
            cmd_args['row_group_size'] = int(a)

    try:
        cmd_args["database"], cmd_args['source'] = args
    except ValueError:
        usage(1)

    loader.control(**cmd_args)
