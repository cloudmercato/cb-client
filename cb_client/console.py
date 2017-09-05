#!/usr/bin/env python
import sys
import argparse
from . import wringers


class ClientArgumentParser(argparse.ArgumentParser):
    def __init__(self, *args, **kwargs):
        super(ClientArgumentParser, self).__init__(*args, **kwargs)
        self.add_argument('-u', '--master-url', default=None,
                          help='set manually token')
        self.add_argument('-t', '--token', default='',
                          help='set manually token')
        self.add_argument('-v', '--verbose', type=int,
                          default=None,
                          help='set verbosity level, between 1 (debug) and 5 (error)')
        self.add_argument('-i', '--input', type=argparse.FileType('r'), default=sys.stdin,
                          help='takes result from file instead stdin')
        self.add_argument('-d', '--date', required=False,
                          help='Test date in ISO8601 format')
        self.add_argument('-s', '--storage-id', required=False,
                          help='Benched storage id')


parser = ClientArgumentParser()
subparser = parser.add_subparsers(help='Parsers for each bench mark test', dest='bench_name')

# FIO
FioArgumentParser = subparser.add_parser('fio', help='fio help')
FioArgumentParser.add_argument('-n', '--numjobs', help='FIO number of threads', type=int)
FioArgumentParser.add_argument('-r', '--ratio', help='FIO mixed ratio', default='100:0')
FioArgumentParser.add_argument('-vf', '--volume-flavor-id', type=int, required=False)
FioArgumentParser.add_argument('-vm', '--volume-manager-id', type=int, required=False)
FioArgumentParser.add_argument('-ns', '--network-storage-id', type=int, required=False)

# SysBench RAM parser
SysbenchCpuArgumentParser = subparser.add_parser('sysbench_cpu')
SysbenchRamArgumentParser = subparser.add_parser('sysbench_ram')

# Ab parser
AbArgumentParser = subparser.add_parser('ab', help='ab help')

# Bonnie
BonnieArgumentParser = subparser.add_parser('bonnie')

# SysBench parser
SysbenchMySqlArgumentParser = subparser.add_parser('sysbench_mysql', help='sysbench help')
SysbenchMySqlArgumentParser.add_argument('-vf', '--volume-flavor-id', help='id of Volume Flavor from Volume Flavor table', type=int, required=True)
SysbenchMySqlArgumentParser.add_argument('-om', '--oltp-test-mode', choices=['simple', 'complex', 'nontrx'], default='complex', help='Oltp Test Mode used', required=False)
SysbenchMySqlArgumentParser.add_argument('-ts', '--table-size', help='Table size used for test', type=int, required=True)
SysbenchMySqlArgumentParser.add_argument('-ro', '--read-only', help='Is read only on?', action='store_false', required=False)
SysbenchMySqlArgumentParser.add_argument('-ntm', '--oltp-nontrx-mode', choices=['select', 'update_key', 'update_nokey', 'insert', 'delete'], default='select', help='Oltp Non Tx Test Mode used', required=False)
SysbenchMySqlArgumentParser.add_argument('-dpm', '--db-ps-mode', choices=['disable', 'auto'], default='auto', help='DB Prepared Statements Used', required=False)
SysbenchMySqlArgumentParser.add_argument('-ors', '--oltp-range-size', help='Oltp range', type=int, default=100, required=False)
SysbenchMySqlArgumentParser.add_argument('-ops', '--oltp-point-selects', help='Number of point select queries in a single transaction', type=int, default=10, required=False)
SysbenchMySqlArgumentParser.add_argument('-osr', '--oltp-simple-ranges', help='Number of simple range queries in a single transaction', type=int, default=1 , required=False)
SysbenchMySqlArgumentParser.add_argument('-osumr', '--oltp-sum-ranges', help='Number of SUM range queries in a single transaction', type=int, default=1 , required=False)
SysbenchMySqlArgumentParser.add_argument('-odr', '--oltp-distinct-ranges', help='Number of DISTINCT range queries in a single transaction', type=int, default=1 , required=False)
SysbenchMySqlArgumentParser.add_argument('-oiu', '--oltp-index-updates', help='Number of index UPDATE queries in a single transaction', type=int, default=1 , required=False)
SysbenchMySqlArgumentParser.add_argument('-oniu', '--oltp-non-index-updates', help='Number of non index UPDATE queries in a single transaction', type=int, default=1 , required=False)
SysbenchMySqlArgumentParser.add_argument('-ocd', '--oltp-connect-delay', help='Time in microseconds to sleep after each connection to database', type=int, default=0 , required=False)
SysbenchMySqlArgumentParser.add_argument('-oudm', '--oltp-user-delay-max', help='Maximum time in microseconds to sleep after each request', type=int, default=0 , required=False)
SysbenchMySqlArgumentParser.add_argument('-oor', '--oltp-order-ranges', help='Number of ORDER range queries in a single transaction', type=int, default=1 , required=False)


def main():
    args = parser.parse_args()
    Wringer = wringers.WRINGERS.get(args.bench_name)
    wringer = Wringer(**vars(args))
    wringer.run()

if __name__ == '__main__':
    main()
