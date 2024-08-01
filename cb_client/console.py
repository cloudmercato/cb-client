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
FioArgumentParser.add_argument('-nf', '--nfs', type=int, required=False)
FioArgumentParser.add_argument('-nft', '--nfs-type', type=int, required=False)
FioArgumentParser.add_argument('-nfc', '--nfs-conf', type=int, required=False)

# SysBench CPU
SysbenchCpuArgumentParser = subparser.add_parser('sysbench_cpu')
# SysBench RAM
SysbenchRamArgumentParser = subparser.add_parser('sysbench_ram')
# SysBench OLTP
SysbenchOltpArgumentParser = subparser.add_parser('sysbench_oltp')
SysbenchOltpArgumentParser.add_argument('-vf', '--volume-flavor', type=int, required=False)
SysbenchOltpArgumentParser.add_argument('-D', '--datastore-type', required=True)
SysbenchOltpArgumentParser.add_argument('--volume-type', required=False)
SysbenchOltpArgumentParser.add_argument('--volume-size', required=False)
SysbenchOltpArgumentParser.add_argument('-S', '--script', required=True)
SysbenchOltpArgumentParser.add_argument('--tables', required=True)
SysbenchOltpArgumentParser.add_argument('--table-size', required=True)
SysbenchOltpArgumentParser.add_argument('--range-size', required=True)
SysbenchOltpArgumentParser.add_argument('--skip-trx', action='store_true')
SysbenchOltpArgumentParser.add_argument('--ssl', action='store_true')

# pgbench
PgbenchArgumentParser = subparser.add_parser('pgbench')
PgbenchArgumentParser.add_argument('-vf', '--volume-flavor', type=int, required=False)
PgbenchArgumentParser.add_argument('--datastore', required=False)
PgbenchArgumentParser.add_argument('-D', '--datastore-type', required=True)
PgbenchArgumentParser.add_argument('--volume-type', required=False)
PgbenchArgumentParser.add_argument('--volume-size', required=False)

PgbenchArgumentParser.add_argument('-S', '--script', required=True)
PgbenchArgumentParser.add_argument('--fillfactor', required=True)
PgbenchArgumentParser.add_argument('--rate', required=False)
PgbenchArgumentParser.add_argument('--foreignkey', action='store_true')
PgbenchArgumentParser.add_argument('--no-vacuum', action='store_true')

# ycsb
YcsbArgumentParser = subparser.add_parser('ycsb')
YcsbArgumentParser.add_argument('-vf', '--volume-flavor', type=int, required=False)
YcsbArgumentParser.add_argument('--datastore', required=False)
YcsbArgumentParser.add_argument('-D', '--datastore-type', required=True)
YcsbArgumentParser.add_argument('--volume-type', required=False)
YcsbArgumentParser.add_argument('--volume-size', required=False)

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

Geekbench5ArgumentParser = subparser.add_parser('geekbench5')
Geekbench5ArgumentParser.add_argument('-F', '--format', default='json')
Geekbench5ArgumentParser.add_argument('-M', '--mode', default='standard')

Geekbench6ArgumentParser = subparser.add_parser('geekbench6')
Geekbench6ArgumentParser.add_argument('-M', '--mode', default='standard')

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
TracepathArgumentParser.add_argument('--datacenter', required=False, help='')

IperfArgumentParser = subparser.add_parser('iperf', help='iperf help')
IperfArgumentParser.add_argument('-de', '--dest-instance_type', help='')
IperfArgumentParser.add_argument('--datacenter', required=False, help='')


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
FfmpegArgumentParser.add_argument('--output-scale')
FfmpegArgumentParser.add_argument('--unit', default='cpu', required=False)
FfmpegArgumentParser.add_argument('--threads')
FfmpegArgumentParser.add_argument('--input-file', required=False)
FfmpegArgumentParser.add_argument('--preset')

# Python FPB
PythonFpbArgumentParser = subparser.add_parser('fpb', help='')

# Redis Benchmark
RedisBenchmarkParser = subparser.add_parser('redis-benchmark', help='')
RedisBenchmarkParser.add_argument('--clients', type=int, required=True)
RedisBenchmarkParser.add_argument('--requests', type=int, required=True)
RedisBenchmarkParser.add_argument('--size', type=int, required=True)
RedisBenchmarkParser.add_argument('--keepalive', type=int, required=True)
RedisBenchmarkParser.add_argument('--keyspacelen', type=int, required=True)
RedisBenchmarkParser.add_argument('--numreq', type=int, required=True)
RedisBenchmarkParser.add_argument('--datastore-id', type=int, required=False)
RedisBenchmarkParser.add_argument('--datastore-type-id', type=int, required=True)

# tcptraceroute
TcptracerouteParser = subparser.add_parser('tcptraceroute', help='')
TcptracerouteParser.add_argument('--length', type=int, required=True)
TcptracerouteParser.add_argument('--target-instance-id', type=int, required=False)
TcptracerouteParser.add_argument('--target-instance-type', required=False)
TcptracerouteParser.add_argument('--target-type-id', type=int, required=True)
TcptracerouteParser.add_argument('--target-type-type', required=True)

# STREAM
StreamParser = subparser.add_parser('stream', help='')
StreamParser.add_argument('--compiler', required=True)

# CPU steal
CpuStealParser = subparser.add_parser('cpu_steal', help='')

# OpenSSL speed
OpensslSpeedParser = subparser.add_parser('openssl_speed', help='')
OpensslSpeedParser.add_argument('--block-size', required=True, type=int)
OpensslSpeedParser.add_argument('--num-thread', required=True, type=int)
OpensslSpeedParser.add_argument('--mode', required=True)
OpensslSpeedParser.add_argument('--use-evp', action='store_true')
OpensslSpeedParser.add_argument('--version', required=False)

# OS benchmark DL
OsBenchmarkDownloadParser = subparser.add_parser('os_benchmark_download', help='')
OsBenchmarkDownloadParser.add_argument('--dest-zone', required=True, type=int)
OsBenchmarkDownloadParser.add_argument('--object-storage', required=False, type=int)
OsBenchmarkDownloadParser.add_argument('--storage-class', required=True, type=int)
# OS benchmark UL
OsBenchmarkUploadParser = subparser.add_parser('os_benchmark_upload', help='')
OsBenchmarkUploadParser.add_argument('--dest-zone', required=True, type=int)
OsBenchmarkUploadParser.add_argument('--object-storage', required=False, type=int)
OsBenchmarkUploadParser.add_argument('--storage-class', required=True, type=int)
# OS benchmark multiDL
OsBenchmarkVideoStreamingParser = subparser.add_parser('os_benchmark_multi_download', help='')
OsBenchmarkVideoStreamingParser.add_argument('--dest-zone', required=True, type=int)
OsBenchmarkVideoStreamingParser.add_argument('--object-storage', required=False, type=int)
OsBenchmarkVideoStreamingParser.add_argument('--storage-class', required=True, type=int)
# OS benchmark VS
OsBenchmarkVideoStreamingParser = subparser.add_parser('os_benchmark_video_streaming', help='')
OsBenchmarkVideoStreamingParser.add_argument('--dest-zone', required=True, type=int)
OsBenchmarkVideoStreamingParser.add_argument('--object-storage', required=False, type=int)
OsBenchmarkVideoStreamingParser.add_argument('--storage-class', required=True, type=int)
# OS benchmark AB
OsBenchmarkAbParser = subparser.add_parser('os_benchmark_ab', help='')
OsBenchmarkAbParser.add_argument('--dest-zone', required=True, type=int)
OsBenchmarkAbParser.add_argument('--object-storage', required=False, type=int)
OsBenchmarkAbParser.add_argument('--storage-class', required=True, type=int)
# OS benchmark Curl
OsBenchmarkDownloadParser = subparser.add_parser('os_benchmark_curl', help='')
OsBenchmarkDownloadParser.add_argument('--dest-zone', required=True, type=int)
OsBenchmarkDownloadParser.add_argument('--object-storage', required=False, type=int)
OsBenchmarkDownloadParser.add_argument('--storage-class', required=True, type=int)

# AI Benchamrk
AiBenchmarkParser = subparser.add_parser('ai_benchmark', help='')
# Sudoku ML Benchamrk
SudokuMlBenchParser = subparser.add_parser('sudoku_ml_bench', help='')
SudokuMlBenchParser.add_argument('--unit', required=True)

# lmbench
## mhz
MhzParser = subparser.add_parser('mhz', help='')
## lat_mem_rd
LatMemRdParser = subparser.add_parser('lat_mem_rd', help='')
## tlb
TlbParser = subparser.add_parser('tlb', help='')
TlbParser.add_argument('--line-size', required=True, type=int)
TlbParser.add_argument('--length', required=True, type=int)
TlbParser.add_argument('--warmup', required=True, type=int)
TlbParser.add_argument('--repetitions', required=True, type=int)
TlbParser.add_argument('--cold-cache', action="store_true")
## cache
CacheParser = subparser.add_parser('cache', help='')
CacheParser.add_argument('--line-size', required=True, type=int)
CacheParser.add_argument('--length', required=True, type=int)
CacheParser.add_argument('--warmup', required=True, type=int)
CacheParser.add_argument('--repetitions', required=True, type=int)
CacheParser.add_argument('--cold-cache', action="store_true")
## lmbench stream
LmbenchStreamParser = subparser.add_parser('lmbench_stream', help='')
LmbenchStreamParser.add_argument('--length', required=True, type=int)
LmbenchStreamParser.add_argument('--parallelism', required=True, type=int)
LmbenchStreamParser.add_argument('--warmup', required=True, type=int)
LmbenchStreamParser.add_argument('--repetitions', required=True, type=int)
## lat_ops
LatOpsParser = subparser.add_parser('lat_ops', help='')
LatOpsParser.add_argument('--parallelism', required=True, type=int)
LatOpsParser.add_argument('--warmup', required=True, type=int)
LatOpsParser.add_argument('--repetitions', required=True, type=int)
## bw_mem
BwMemParser = subparser.add_parser('bw_mem', help='')
BwMemParser.add_argument('--parallelism', required=True, type=int)
BwMemParser.add_argument('--warmup', required=True, type=int)
BwMemParser.add_argument('--repetitions', required=True, type=int)
BwMemParser.add_argument('--operation', required=True, type=str)
## yolo-benchmark-predict
YoloBenchmarkPredict = subparser.add_parser('yolo_benchmark_predict', help='')

# CoreMark
CoreMarkParser = subparser.add_parser('coremark', help='')

# lm-sensors
LmSensorsParser = subparser.add_parser('lm_sensors', help='')
LmSensorsParser.add_argument('--cpu-usage', required=True, type=int)

# ipmi-sensors
ImpiSensorsParser = subparser.add_parser('ipmi_sensors', help='')
ImpiSensorsParser.add_argument('--user-cpu-usage', required=True, type=int)

# gotowaf
GoToWafParser = subparser.add_parser('gotowaf', help='')
GoToWafParser.add_argument('-de', '--destination-id', help='')
GoToWafParser.add_argument('-De', '--destination-type', help='')

# pybench
PythonBenchmarkParser = subparser.add_parser('python_benchmark', help='')
PythonBenchmarkParser.add_argument('-py', '--python-version-minor', help='')

# Blender bench
BlenderBenchmarkParser = subparser.add_parser('blender_benchmark', help='')

# Deepsparse bench
DeepsparseBenchmarkParser = subparser.add_parser('deepsparse_benchmark', help='')
DeepsparseBenchmarkParser.add_argument('-py', '--python-version', help='')
DeepsparseBenchmarkParser.add_argument('-sc', '--scenario', help='')
DeepsparseBenchmarkParser.add_argument('-wt', '--warmup-time', help='')
DeepsparseBenchmarkParser.add_argument('-tp', '--thread-pinning', help='')
DeepsparseBenchmarkParser.add_argument('-en', '--engine', help='')

# Ollama
OllamaParser = subparser.add_parser('ollama', help='')
OllamaParser.add_argument('--version', help='')
OllamaParser.add_argument('--unit', help='')
OllamaParser.add_argument('--query', help='')
OllamaParser.add_argument('--model', help='')

# Ollama Benchmark Speed
OllamaBenchmarkSpeedParser = subparser.add_parser('ollama_benchmark_speed', help='')
OllamaBenchmarkSpeedParser.add_argument('--unit', help='')

# Whisper
WhisperBenchmarkParser = subparser.add_parser('whisper_benchmark', help='')

# InvokeAi
InvokeAiBenchmarkParser = subparser.add_parser('invokeai_benchmark', help='')
InvokeAiBenchmarkParser.add_argument('--unit', help='')

# nvbandwidth 
NvbandwidthParser = subparser.add_parser('nvbandwidth', help='')
NvbandwidthParser.add_argument('--buffer')

# gpuburn 
GpuBurnParser = subparser.add_parser('gpu_burn', help='')

# npb 
NpbParser = subparser.add_parser('npb', help='')


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
