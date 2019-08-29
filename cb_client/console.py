#!/usr/bin/env python
import sys
import argparse
from cb_client import wringers
from cb_client import runners


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
        self.add_argument('-T', '--tag', required=False,
                          help='Tag to add to result')
        self.add_argument('-p', '--project', required=False)
        self.add_argument('-St', '--is-not-standalone', dest='is_standalone', action="store_false", required=False)
        self.add_argument('-se', '--set', dest='test_set', required=False)


parser = ClientArgumentParser()
subparser = parser.add_subparsers(help='Parsers for each bench mark test', dest='bench_name')

# Monitoring
MetricArgumentParser = subparser.add_parser('metric', help='help')
PrometheusArgumentParser = subparser.add_parser('prometheus', help='help')

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

SysbenchOltpArgumentParser = subparser.add_parser('sysbench_oltp')
SysbenchOltpArgumentParser.add_argument('-vf', '--volume-flavor', type=int, required=False)
SysbenchOltpArgumentParser.add_argument('-D', '--datastore-type', required=True)
SysbenchOltpArgumentParser.add_argument('-S', '--script', required=True)

# Ab parser
AbArgumentParser = subparser.add_parser('ab', help='ab help')
AbArgumentParser.add_argument('-n', '--num-thread', help='')
AbArgumentParser.add_argument('-k', '--keep-live', action='store_true', help='')
AbArgumentParser.add_argument('-S', '--secure', action='store_true', help='')
AbArgumentParser.add_argument('-de', '--destination-id', help='')
AbArgumentParser.add_argument('-De', '--destination-type', help='')

# wrk
WrkArgumentParser = subparser.add_parser('wrk', help='wrk help')
WrkArgumentParser.add_argument('-de', '--destination-id', help='')
WrkArgumentParser.add_argument('-De', '--destination-type', help='')

# Bonnie
BonnieArgumentParser = subparser.add_parser('bonnie')

# Geekbench
Geekbench3ArgumentParser = subparser.add_parser('geekbench3')

Geekbench4ArgumentParser = subparser.add_parser('geekbench4')
Geekbench4ArgumentParser.add_argument('-F', '--format', default='json')
Geekbench4ArgumentParser.add_argument('-M', '--mode', default='standard')

# SPEC CPU
SpecCpu2006ArgumentParser = subparser.add_parser('spec_cpu2006')
SpecCpu2017ArgumentParser = subparser.add_parser('spec_cpu2017')

# FinanceBench
FinanceBenchArgumentParser = subparser.add_parser('financebench')
FinanceBenchArgumentParser.add_argument('-app', '--app', type=str)
FinanceBenchArgumentParser.add_argument('-mode', '--mode', type=str)
FinanceBenchArgumentParser.add_argument('-compiler', '--compiler', type=str)
FinanceBenchArgumentParser.add_argument('-compiler-version', '--compiler-version', type=str)

# LAMMPS
LammpsArgumentParser = subparser.add_parser('lammps')
LammpsArgumentParser.add_argument('--test', type=str)
LammpsArgumentParser.add_argument('--num-process', type=int)

# V-Ray
VRayArgumentParser = subparser.add_parser('vray')
VRayArgumentParser.add_argument('--unit', type=str)

# VASP test
VaspTestArgumentParser = subparser.add_parser('vasptest')
VaspTestArgumentParser.add_argument('--num-process', type=str)
VaspTestArgumentParser.add_argument('--scenario', type=str)
VaspTestArgumentParser.add_argument('--executable', type=str)

# Network
TracepathArgumentParser = subparser.add_parser('tracepath', help='tracepath help')
TracepathArgumentParser.add_argument('-de', '--destination-id', help='')
TracepathArgumentParser.add_argument('-De', '--destination-type', help='')

IperfArgumentParser = subparser.add_parser('iperf', help='iperf help')
IperfArgumentParser.add_argument('-de', '--dest-instance_type', help='')


# Hibench
WordcountArgumentParser = subparser.add_parser('wordcount', help='wordcount help')
WordcountArgumentParser.add_argument('-si', '--size', help='Hibench test size')
WordcountArgumentParser.add_argument('-r', '--report-dir', help='Report base directory')
WordcountArgumentParser.add_argument('-ar', '--architecture', help='')

TeraSortArgumentParser = subparser.add_parser('terasort', help='terasort help')
TeraSortArgumentParser.add_argument('-si', '--size', help='Hibench test size')
TeraSortArgumentParser.add_argument('-r', '--report-dir', help='Report base directory')
TeraSortArgumentParser.add_argument('-ar', '--architecture', help='')

DfsioArgumentParser = subparser.add_parser('dfsio', help='dfsio help')
DfsioArgumentParser.add_argument('-si', '--size', help='Hibench test size')
DfsioArgumentParser.add_argument('-r', '--report-dir', help='Report base directory')
DfsioArgumentParser.add_argument('-ar', '--architecture', help='')

# Phoronix
PhoronixTestSuiteArgumentParser = subparser.add_parser('phoronix_test_suite', help='pts elp')
PhoronixTestSuiteArgumentParser.add_argument('-te', '--test', help='test')

# Python
PythonReadArgumentParser = subparser.add_parser('python_read', help='')
PythonReadArgumentParser.add_argument('-Si', '--size', required=True)
PythonReadArgumentParser.add_argument('-Ch', '--chunk-size', required=True)
PythonReadArgumentParser.add_argument('-It', '--iterations', required=True)
PythonReadArgumentParser.add_argument('-Fi', '--filename', required=True)

# CI task
CiTaskArgumentParser = subparser.add_parser('ci_task', help='')
CiTaskArgumentParser.add_argument('-Ser', '--service', required=True)
CiTaskArgumentParser.add_argument('-Tas', '--task', required=True)
CiTaskArgumentParser.add_argument('-Max', '--max-concurrency', required=True)
CiTaskArgumentParser.add_argument('-Con', '--concurrency', required=True)

# Kvazaar
KvazaarArgumentParser = subparser.add_parser('kvazaar', help='')
KvazaarArgumentParser.add_argument('--preset', required=True)
KvazaarArgumentParser.add_argument('--threads', required=True)
KvazaarArgumentParser.add_argument('--input-file', required=True)
KvazaarArgumentParser.add_argument('--real-time', required=True)
KvazaarArgumentParser.add_argument('--user-time', required=True)
KvazaarArgumentParser.add_argument('--sys-time', required=True)
KvazaarArgumentParser.add_argument('--output-size', required=True)

# Vdbench
VdbenchArgumentParser = subparser.add_parser('vdbench', help='')
VdbenchArgumentParser.add_argument('--vdbench-config', type=str, required=False)
VdbenchArgumentParser.add_argument('--fsd-depth', type=int, required=False)
VdbenchArgumentParser.add_argument('--fsd-width', type=int, required=False)
VdbenchArgumentParser.add_argument('--fsd-files', type=int, required=False)
VdbenchArgumentParser.add_argument('--fsd-size', type=str, required=False)
VdbenchArgumentParser.add_argument('--fsd-directio', action="store_true")
VdbenchArgumentParser.add_argument('--fwd-xfersize', type=str, required=False)
VdbenchArgumentParser.add_argument('--fwd-fileio', type=str, required=False)
VdbenchArgumentParser.add_argument('--fwd-rdpct', type=int, required=False)
VdbenchArgumentParser.add_argument('--rd-elapsed', type=int, required=False)
VdbenchArgumentParser.add_argument('--rd-threads', type=int, required=False)

VdbenchArgumentParser.add_argument('-vf', '--volume-flavor-id', type=int, required=False)
VdbenchArgumentParser.add_argument('-vm', '--volume-manager-id', type=int, required=False)
VdbenchArgumentParser.add_argument('-ns', '--network-storage-id', type=int, required=False)

RunnerArgumentParser = subparser.add_parser('run', help='help')
RunnerArgumentParser = RunnerArgumentParser.add_argument('test')

# Ffmpeg
FfmpegArgumentParser = subparser.add_parser('ffmpeg', help='')
FfmpegArgumentParser.add_argument('--output-format', required=True)
FfmpegArgumentParser.add_argument('--unit', default='cpu', required=False)
FfmpegArgumentParser.add_argument('--input_file', required=False)


def main():
    parsed_args = parser.parse_known_args()[0]
    if parsed_args.bench_name == 'run':
        del sys.argv[:2]
        runner = runners.RUNNERS[parsed_args.test]()
        runner.run()
    else:
        Wringer = wringers.WRINGERS.get(parsed_args.bench_name)
        wringer = Wringer(**vars(parsed_args))
        wringer.run()


if __name__ == '__main__':
    main()
