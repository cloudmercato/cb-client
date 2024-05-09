from __future__ import division

import os
import sys
import re
import csv
import json
import io
import math
from functools import reduce
from datetime import datetime
from collections import Counter
try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

import six

from . import exceptions
from . import client
from . import utils
from .settings import get_config

try:
    import behave
    BEHAVE_VERSION = behave.__version__
except ImportError:
    BEHAVE_VERSION = None


PYTHON_VERSION = '%d.%d.%d' % sys.version_info[:3]
TIME_SCALE = {
  's': 1,
  'ms': 10**-3,
  'us': 10**-6,
  'ns': 10**-9
}
TRACEPATH_RESULT_REG = re.compile(r'\s*Resume:\s*pmtu\s*(\d*)\s*hops\s*(\d*)\s*back\s*(\d*)')
TRACEPATH_TIME_REG = re.compile(r'.*\s([0-9\.]*)ms.*')

try:
    REG_FINANCEBENCH = re.compile(r"^Processing time on (G|C)PU ?\(?(\w*)?\)?: ([\d\.]*) \(ms\)\s*$")
except:
    REG_FINANCEBENCH = None
RE_FLOAT = re.compile("([0-9.]+)")
REG_LAMMPS_PERF = re.compile('Performance:\s*([\d\.]*)\s*([\d\w/]*),\s*([\d\.]*)\s*([\d\w/]*),\s*([\d\.]*)\s*([\d\w/]*)')
REG_LAMMPS_ROW = re.compile(r'^(\w*)\s*\|\s*([\d.]*)\s*\|\s*([\d.]*)\s*\|\s*([\d.]*)\s*\|\s*([\d.]*)\s*\|\s*([\d.]*)\s*$')
REG_VRAY = re.compile(r'Rendering took (\d*):(\d*) minutes.', re.M)
REG_VASP = re.compile(r'Took (\d*)m([0-9.]*)s')
REG_FFMPEG_OPTS = re.compile("Reading option '-([^']*)'.*argument '([^']*)'")
REG_FFMPEG_TIME = re.compile("bench: utime=([^s]*)s stime=([^s]*)s rtime=([^s]*)s")
REG_FFMPEG_BYTES = re.compile("([0-9]*) ?bytes")
REG_FFMPEG_STATS = re.compile("frame=\s*(\d*)\s*fps=\s*([0-9\.]*)\s*q=([0-9\.-]*)\s*Lsize=\s*(\d*)kB\s*time=[0-9:\.]*\s*bitrate=([\d\.]*)kbits/s\s*speed=\s*([\d\.]*)x")
GITLAB_METRICS = (
    'db_ping_latency_seconds',
    'filesystem_access_latency_seconds',
    'filesystem_write_latency_seconds',
    'filesystem_read_latency_seconds',
    'filesystem_circuitbreaker_latency_seconds',
    'redis_ping_latency_seconds',
)
FFMPEG_OPTS = ['preset', 'filter_threads', 'filter_complex_threads']
FFMPEG_OPTS_TRANS = {'i', lambda k, v: ('input_file', v.split('/')[-1])}

class InvalidInput(Exception):
    pass


def convert_second(value, src=None, dst='ms'):
    if isinstance(value, six.string_types):
        value = value.strip()
        if value.isalnum():
            value, src_ = re.match('(\d*)(\w*)', value).groups()
            src = src_ if src is None else src
    value = float(value)
    in_sec = value * TIME_SCALE.get(src, 1)
    in_dst_fmt = in_sec / TIME_SCALE[dst]
    return round(in_dst_fmt, 2)


def mean(values):
    return float(sum(values)) / max(len(values), 1)


def geo_mean(values):
    return reduce(lambda x, y: x*y, values) ** (1.0/len(values))


def weighted_mean(values):
    return sum([w/100*s for w, s in values])


class BaseWringer(object):
    def __init__(self, input_=None, master_url=None, token=None,
                 instance_id=None, flavor_id=None, image_id=None,
                 date=None, tag=None, project=None, is_standalone=None,
                 test_set=None, **kwargs):
        """
        :param input_: Output of benchmark command, default is stdin
        :type input_: file

        :param master_url: Base endpoint of master REST API, default is from
                           configuration
        :type master_url: str

        :param token: Authentication token, default is from configuration
        :type token: str

        :param flavor_id: Flavor ID of tested machine, default is from
                          configuration
        :type flavor_id: int

        :param image_id: Image ID of tested machine, default is from
                         configuration
        :type image_id: int

        :param date: Execution date in ISO8601, default is now
        :type date: str
        """
        self.input_ = input_ or sys.stdin

        self.config = get_config()
        self.master_url = master_url or self.config.get('master_url')
        self.token = token or self.config.get('token')
        self.instance_id = instance_id or self.config.get('id')
        self.flavor_id = flavor_id or self.config.get('flavor')
        self.image_id = image_id or self.config.get('image')
        self.provider_id = self.config.get('provider')
        self.datacenter_id = self.config.get('datacenter')
        self.instance_type_id = self.config.get('type')
        self.test_date = date or datetime.now().isoformat()
        self.project = project or self.config.get('project')
        if str(self.project).lower() in ('null', 'none'):
            self.project = None

        self.client = client.Client(self.master_url, self.token)

        self.tag = tag
        self.is_standalone = is_standalone
        self.test_set = test_set

    def _get_data(self):
        """
        Return benchmark result as dict. You must override this method.

        :returns: Benchmark result
        :rtype: dict
        """
        raise NotImplementedError("You must overide this method.")

    def _get_metadata(self):
        """
        Return benchmark configuration as dict, you can override this method
        to fit with your need.
        """
        data = {
            'test_date': self.test_date,
            'flavor': self.flavor_id,
            'image': self.image_id,
            'provider': self.provider_id,
            'datacenter': self.datacenter_id,
            'instance_type': self.instance_type_id,
            'instance': self.instance_id,
            'tag': self.tag,
            'project': self.project,
            'set': self.test_set,
        }
        if self.is_standalone is not None:
            data['is_standalone'] = self.is_standalone
        return data

    def run(self):
        """
        Public runner to parse and publish result.
        """
        try:
            response = self.client.post_result(
                bench_name=self.bench_name,
                data=self._get_data(),
                metadata=self._get_metadata())
            if response.status_code >= 300:
                raise exceptions.ServerError(response.content + str(self._get_data()))
            return response
        except KeyboardInterrupt:
            raise SystemExit(1)


PROCESS_STATES = {
    'A': 'all',
    'R': 'running',
    'D': 'uninterruptible sleep',
    'S': 'interruptible sleep',
    'T': 'stopped',
    'Z': 'zombie',
}
class MetricWringer(BaseWringer):
    bench_name = 'metric'

    def run(self):
        """
        Public runner to parse and publish result.
        """
        data = []
        process_count = Counter(A=0, D=0, R=0, S=0, T=0, Z=0)
        self.input_ = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8', errors='ignore')
        for line in self.input_:
            line = line.strip()
            # Pass empty
            if not line:
                continue
            # Set counter or upload
            if line == 'SEP' and process_count['A']:
                for name, value in process_count.items():
                    data.append({
                        'group': 'PRG',
                        'value': value,
                        'name': '%s processes' % PROCESS_STATES[name],
                        'date': datetime.fromtimestamp(int(line[1])).isoformat(),
                        'instance': self.instance_id,
                    })
                process_count = Counter(A=0, D=0, R=0, S=0, T=0, Z=0)
            line = line.split()
            # Skip not implemented
            if not hasattr(self, 'parse_%s' % line[0]):
                continue
            # Count process
            elif line[0] == 'PRG':
                process_count['A'] += 1
                process_count[line[8]] += 1
            group, timestamp = line[:3:2]
            parser = getattr(self, 'parse_%s' % group)
            data.extend(parser(line))
        try:
            url = self.client.make_url('/rest/metric/')
            for start in range(0, len(data), 250):
                response = self.client.post(
                    url=url,
                    json=data[start:start+250])
                if response.status_code >= 300:
                    error = exceptions.ServerError(response.content + str(data))
            return response
        except KeyboardInterrupt:
            raise SystemExit(1)

    def _get_cpu_usage(self, stime, utime, ntime, itime, wtime, Itime, Stime, steal):
        use_time = int(stime) + int(utime) + int(ntime) + int(Itime) + int(Stime)
        wait_time = int(itime) + int(wtime) + int(steal)
        total_time = use_time + wait_time
        percent = use_time / total_time * 100
        return percent

    def _get_cpu_steal_rate(self, stime, utime, ntime, itime, wtime, Itime, Stime, steal):
        total_time = int(stime) + int(utime) + int(ntime) + int(Itime) + int(Stime) + int(itime) + int(wtime) + int(steal)
        steal_time = int(steal)
        percent = steal_time / total_time * 100
        return percent

    def parse_CPU(self, line):
        """
        CPU zuluvm 1514345405 2017/12/27 03:30:05 3237908 100 2 4363430 10933017 0 628530138 485206 0 884945 0 0 4608 100
        """
        timestamp, _, _, _, total, cpu_count, stime, utime, ntime, itime, wtime, Itime, Stime, steal, guest, _, _, = line[2:-2]
        data = {
            'stime': stime,
            'utime': utime,
            'itime': itime,
            'steal': steal,
        }
        metrics = [{
            'group': 'CPU',
            'value': self._get_cpu_usage(stime, utime, ntime, itime, wtime, Itime, Stime, steal),
            'name': 'CPU usage',
            'date': datetime.fromtimestamp(int(timestamp)).isoformat(),
            'instance': self.instance_id,
        }]
        for name, value in data.items():
            metrics.append({
                'group': 'CPU',
                'value': value,
                'name': name,
                'date': datetime.fromtimestamp(int(timestamp)).isoformat(),
                'instance': self.instance_id,
            })
        return metrics

    def parse_cpu(self, line):
        """
        cpu teststeal 1581105787 2020/02/07 20:03:07 30 100 0 1127 1681 0 36 0 0 0 155 0 1995 100 21239022365 65534227891
        cpu victim 1517408534 2018/01/31 14:22:14 1022853 100 0 63709 515511 0 101620702 11852 0 2612 0 0 2304 100
        cpu victim 1517408534 2018/01/31 14:22:14 1022853 100 1 64801 515500 0 101566068 60189 0 3948 0 0 2304 100


        cpu zuluvm 1514345405 2017/12/27 03:30:05 3237908 100 0 4363430 10933017 0 628530138 485206 0 884945 0 0 4608 100
        """
        timestamp, _, _, _, total, cpu_index, stime, utime, ntime, itime, wtime, Itime, Stime, steal, guest, _, _ = line[2:-2]
        data = {
            'stime': stime,
            'utime': utime,
            'steal': steal,
        }
        metrics = [{
            'group': 'CPU',
            'value': self._get_cpu_usage(stime, utime, ntime, itime, wtime, Itime, Stime, steal),
            'name': 'CPU#%d usage' % int(cpu_index),
            'date': datetime.fromtimestamp(int(timestamp)).isoformat(),
            'instance': self.instance_id,
        }, {
            'group': 'CPU',
            'value': self._get_cpu_steal_rate(stime, utime, ntime, itime, wtime, Itime, Stime, steal),
            'name': 'CPU#%d steal rate' % int(cpu_index),
            'date': datetime.fromtimestamp(int(timestamp)).isoformat(),
            'instance': self.instance_id,
        }]
        for name, value in data.items():
            metrics.append({
                'group': 'CPU',
                'value': value,
                'name': 'CPU#%d %s' % (int(cpu_index), name),
                'date': datetime.fromtimestamp(int(timestamp)).isoformat(),
                'instance': self.instance_id,
            })
        return metrics

    def parse_CPL(self, line):
        """CPL zuluvm 1514345405 2017/12/27 03:30:05 3237908 2 0.08 0.03 0.00 1192304404 738790591"""
        timestamp, _, _, _, _, c1, c5, c15, context_switch, interrupts = line[2:]
        data = {
            'c1': (c1, 'Load 1'),
            'c5': (c5, 'Load 5'),
            'c15': (c15, 'Load 15'),
        }
        metrics = []
        for name, (value, name) in data.items():
            metrics.append({
                'group': 'CPL',
                'value': value,
                'name': name,
                'date': datetime.fromtimestamp(int(timestamp)).isoformat(),
                'instance': self.instance_id,
            })
        return metrics

    def parse_MEM(self, line):
        """MEM zuluvm 1514345405 2017/12/27 03:30:05 3237908 4096 231469 45087 18739 9304 14255 5"""
        timestamp, _, _, _, _, page_size, physical, free, cache, buffer_, slab = line[2:13]
        data = {
            'physical': (physical, 'Physical pages'),
            'free': (free, 'Free pages'),
            'cache': (cache, 'Cache pages'),
        }
        metrics = []
        for name, (value, name) in data.items():
            metrics.append({
                'group': 'MEM',
                'value': value,
                'name': name,
                'date': datetime.fromtimestamp(int(timestamp)).isoformat(),
                'instance': self.instance_id,
            })
        return metrics

    def parse_disk(self, line, disk_type):
        """
        DSK victim 1517445950 2018/02/01 00:45:50 1041531 sda 825240 300319 14518802 305838 147045480
        """
        timestamp, _, _, _, device, time, read, sread, write, swrite = line[2:]
        data = {
            'time': (time, 'I/O time'),
            'read': (read, 'read'),
            'sread': (sread, 'read sectors'),
            'write': (write, 'write'),
            'swrite': (swrite, 'write sectors'),
        }
        metrics = []
        for name, (value, vname) in data.items():
            name = '%s %s' % (vname, device)
            metrics.append({
                'group': disk_type,
                'value': value,
                'name': name,
                'date': datetime.fromtimestamp(int(timestamp)).isoformat(),
                'instance': self.instance_id,
            })
        return metrics

    def parse_NET(self, line):
        """
        NET victim 1517448991 2018/02/01 01:36:31 1044572 upper 2068124 1107308 12253 10852 2085705 1112233 2084476 0
        NET victim 1517448991 2018/02/01 01:36:31 1044572 enp0s5 2117101 2864807572 1118790 106303965 0 0
        """
        if line[6] == 'upper':
            timestamp, _, _, _, interface, tcp_rpackets, tcp_spackets, udp_rpackets, udp_spackets, ip_rpackets, ip_spackets, app_packets, forward_packets = line[2:]
            data = {
                'tcp_rpackets': (tcp_rpackets, 'TCP received packets'),
                'tcp_spackets': (tcp_spackets, 'TCP sent packets'),
                'udp_rpackets': (udp_rpackets, 'UDP received packets'),
                'udp_spackets': (udp_spackets, 'UDP sent packets'),
                'ip_rpackets': (ip_rpackets, 'IP received packets'),
                'ip_spackets': (ip_spackets, 'IP sent packets'),
            }
        else:
            timestamp, _, _, _, interface, rpackets, rbytes, spackets, sbytes, intspeed, duplex = line[2:]
            data = {
                'rpackets': (rpackets, 'received packets'),
                'rbytes': (rbytes, 'received bytes'),
                'spackets': (spackets, 'sent packets'),
                'sbytes': (sbytes, 'sent bytes'),
            }
        metrics = []
        for name, (value, vname) in data.items():
            name = '%s %s' % (vname, interface)
            metrics.append({
                'group': 'NET',
                'value': value,
                'name': name,
                'date': datetime.fromtimestamp(int(timestamp)).isoformat(),
                'instance': self.instance_id,
            })
        return metrics

    def parse_DSK(self, line):
        return self.parse_disk(line, 'DSK')

    def parse_MDD(self, line):
        return self.parse_disk(line, 'MDD')

    def parse_LVM(self, line):
        return self.parse_disk(line, 'LVM')


class PrometheusMetricWringer(BaseWringer):
    bench_name = 'metric'

    def run(self):
        """
        Public runner to parse and publish result.
        """
        data = []
        date_ = datetime.now().isoformat()
        for line in self.input_:
            line = line.strip()
            # Pass empty & comment
            if not line or line.startswith('#'):
                continue
            splitted_line = line.split()
            if splitted_line[0] not in GITLAB_METRICS or len(splitted_line) != 2:
                continue
            name, value = line.split()
            data.append({
                'group': 'APP',
                'value': value,
                'name': name,
                'date': date_,
                'instance': self.instance_id,
            })
        try:
            url = self.client.make_url('/rest/metric/')
            for start in range(0, len(data), 250):
                response = self.client.post(
                    url=url,
                    json=data[start:start+250])
                if response.status_code >= 300:
                    error = exceptions.ServerError(response.content + str(data))
            return response
        except KeyboardInterrupt:
            raise SystemExit(1)


class SysbenchCpuWringer(BaseWringer):
    bench_name = 'sysbench_cpu'

    def _get_data(self):
        # Parse input file
        WANTEDS = ('of threads', 'events per second', 'total time', 'total number of events', 'min', 'avg', 'max', 'numbers limit', 'time elapsed')
        bench_data = {}
        bench_data['version'] = self.input_.readline().split()[1]
        for line in self.input_:
            for _line in [line.strip() for w in WANTEDS if w in line]:
                key, value = _line.split(':')
                bench_data[key.strip()] = value.strip()
        if 'total time' in bench_data:
            exec_time = int(float(re.sub(r'[^0-9\.]', '', bench_data['total time'])))
        else:
            exec_time = int(float(re.sub(r'[^0-9\.]', '', bench_data['time elapsed'])))
            
        # Return a dict with API needed infos
        return {
            'exec_time': exec_time,
            'num_thread': bench_data['Number of threads'],
            'cpu_max_prime': bench_data['Prime numbers limit'],
            'event_number': bench_data['total number of events'],
            'event_speed': bench_data['events per second'],
            'latency_min': bench_data['min'],
            'latency_max': bench_data['max'],
            'latency_avg': bench_data['avg'],
        }


class SysbenchRamWringer(BaseWringer):
    bench_name = 'sysbench_ram'

    def _get_data(self):
        # Parse input file
        WANTEDS = ('threads:', 'total time', 'total number of events', 'min', 'avg', 'max', 'operation:', 'block size', 'time elapsed')
        bench_data = {}
        for line in self.input_:
            if 'total time' in bench_data:
                exec_time = int(float(re.sub(r'[^0-9\.]', '', bench_data['total time'])))
            if 'time elapsed' in bench_data:
                exec_time = int(float(re.sub(r'[^0-9\.]', '', bench_data['time elapsed'])))

            if 'Operations performed' in line or 'Total operations:' in line:
                operations, operation_speed = re.findall(r'(\d+).+\(\s*([0-9\.]+)', line)[0]
                bench_data['operations'] = operations
                bench_data['operation_speed'] = operation_speed
            elif 'B transferred (' in line:
                transfered, transfer_speed = re.findall(r'(\s*\d*\.\d*)', line)
                bench_data['transfered'] = transfered
                bench_data['transfer_speed'] = transfer_speed
            else:
                for _line in [line.strip() for w in WANTEDS if w in line]:
                    key, value = _line.split(':')
                    bench_data[key] = value.strip()
        # Return a dict with API needed infos
        return {
            'exec_time': exec_time,
            'num_thread': bench_data['Number of threads'],
            'readwrite': bench_data['operation'],
            'mode': 'seq',
            'block_size': bench_data['block size'],
            'event_number': bench_data['total number of events'],
            'operations': bench_data['operations'],
            'operation_speed': bench_data['operation_speed'],
            'transfered': bench_data['transfered'],
            'transfer_speed': bench_data['transfer_speed'],
        }


class BaseObjectStorageWringer(BaseWringer):
    """
    Abstract class for object storage benchmarks.
    """
    def __init__(self, storage_id, *args, **kwargs):
        super(BaseObjectStorageWringer, self).__init__(*args, **kwargs)
        self.storage_id = storage_id

    def _get_metadata(self):
        metadata = super(BaseObjectStorageWringer, self)._get_metadata()
        metadata['storage'] = self.storage_id
        return metadata


class AbWringer(BaseObjectStorageWringer):
    """
    Apache Bench Wringer
    """
    bench_name = 'ab'

    def __init__(self, destination_id, destination_type, num_thread,
                 keep_live, secure, *args, **kwargs):
        super(AbWringer, self).__init__(*args, **kwargs)
        self.destination_id = destination_id
        self.destination_type = destination_type
        self.num_thread = num_thread
        self.keep_live = keep_live
        self.secure = secure

    def _get_data(self):
        bench_data = {
            'num_error_response': 0,
        }
        for line in self.input_:
            if not line.strip():
                continue
            elif line.startswith('Document Path:'):
                bench_data['path'] = line.split(':')[1].strip()
            elif line.startswith('Document Length:'):
                bench_data['file_size'] = re.findall('\s*(\d+)\s*', line)[0]
            elif line.startswith('Concurrency Level:'):
                bench_data['num_thread'] = line.split(':')[1].strip()
            elif line.startswith('Time taken for tests:'):
                bench_data['exec_time'] = re.findall('\s*([\d.]+)\s*', line)[0]
            elif line.startswith('Complete requests:'):
                bench_data['num_request'] = line.split(':')[1].strip()
            elif line.startswith('Failed requests:'):
                bench_data['num_failed_request'] = line.split(':')[1].strip()
            elif line.startswith('Non-2xx responses:'):
                bench_data['num_error_response'] = line.split(':')[1].strip()
            elif line.startswith('Total transferred:'):
                bench_data['transfered'] = re.findall('\s*(\d+)\s*', line)[0]
            elif line.startswith('HTML transferred:'):
                bench_data['html_transfered'] = re.findall('\s*(\d+)\s*', line)[0]
            elif line.startswith('Requests per second:'):
                bench_data['request_rate'] = re.findall('\s*([\d.]+)\s*', line)[0]
            elif line.startswith('Time per request:'):
                if 'across all' in line:
                    bench_data['global_time_per_request'] = re.findall('\s*([\d.]+)\s*', line)[0]
                else:
                    bench_data['time_per_request'] = re.findall('\s*([\d.]+)\s*', line)[0]
            elif line.startswith('Transfer rate:'):
                bench_data['transfer_rate'] = re.findall('\s*([\d.]+)\s*', line)[0]
            elif line.strip().startswith('95%'):
                bench_data['percentage_95'] = line.split()[1]
        dest_key = 'dest_%s' % self.destination_type
        bench_data.update({
            dest_key: self.destination_id,
            'num_thread': self.num_thread,
            'keep_alive': self.keep_live,
            'secure': self.secure,
        })
        return bench_data


class FioWringer(BaseWringer):
    bench_name = 'fio'

    def __init__(self, *args, **kwargs):
        super(FioWringer, self).__init__(*args, **kwargs)
        self.num_thread = kwargs.get('numjobs')
        self.mixed_ratio = kwargs['ratio']
        self.volume_flavor_id = kwargs['volume_flavor_id'] or None
        self.volume_manager_id = kwargs['volume_manager_id'] or None
        self.network_storage_id = kwargs['network_storage_id'] or None
        self.nfs_id = kwargs['nfs'] or None
        self.nfs_type_id = kwargs['nfs_type'] or None
        self.nfs_conf_id = kwargs['nfs_conf'] or None

    def _get_readwrite(self, rw):
        trans = {
            'read': 'read',
            'write': 'write',
            'readwrite': 'readwrite',
            'rw': 'readwrite',
            'randread': 'read',
            'randwrite': 'write',
            'randreadwrite': 'readwrite',
            'randrw': 'readwrite',
        }
        return trans[rw]

    def _get_data(self, **kwargs):
        fio_data = json.load(self.input_)
        fio_result = {}

        fio_result['volume_flavor'] = self.volume_flavor_id
        fio_result['volume_manager'] = self.volume_manager_id
        fio_result['network_storage'] = self.network_storage_id
        fio_result['nfs'] = self.nfs_id
        fio_result['nfs_conf'] = self.nfs_conf_id
        fio_result['nfs_type'] = self.nfs_type_id

        fio_result['num_thread'] = self.num_thread or len(fio_data['jobs'])
        fio_result['mixed_ratio'] = self.mixed_ratio

        fio_result['block_size'] = fio_data['global options']['bs'] if 'bs' in fio_data['global options'] else fio_data['jobs'][0]['job options']['bs']
        fio_result['readwrite'] = self._get_readwrite(fio_data['global options']['rw'])
        fio_result['mode'] = 'rand' if 'rand' in fio_data['global options']['rw'] else 'seq'
        fio_result['runtime'] = int(fio_data['global options']['runtime'] if 'runtime' in fio_data['global options'] else int(fio_data['jobs'][0]['job options']['runtime'])/1000)
        fio_result['io_depth'] = fio_data['global options']['iodepth']

        fio_result['read_io'] = int(fio_data['jobs'][0]['read']['io_bytes']/1048576)
        fio_result['read_iops'] = int(fio_data['jobs'][0]['read']['iops'])
        fio_result['read_bandwidth'] = fio_data['jobs'][0]['read']['bw']
        fio_result['read_bandwidth_max'] = fio_data['jobs'][0]['read']['bw_max']

        fio_result['write_io'] = int(fio_data['jobs'][0]['write']['io_bytes']/1048576)
        fio_result['write_iops'] = int(fio_data['jobs'][0]['write']['iops'])
        fio_result['write_bandwidth'] = fio_data['jobs'][0]['write']['bw']
        fio_result['write_bandwidth_max'] = fio_data['jobs'][0]['write']['bw_max']
        # clat changed to clat_ns in FIO 3
        if 'clat' in fio_data['jobs'][0]['write']:
            lat_key = 'clat'
            lat_factor = 1
        else:
            lat_key = 'clat_ns'
            lat_factor = 1000
        fio_result['read_latency'] = float(fio_data['jobs'][0]['read'][lat_key]['mean']) / lat_factor
        fio_result['read_latency_stdev'] = float(fio_data['jobs'][0]['read'][lat_key]['stddev']) / lat_factor
        fio_result['write_latency'] = float(fio_data['jobs'][0]['write'][lat_key]['mean']) / lat_factor
        fio_result['write_latency_stdev'] = float(fio_data['jobs'][0]['write'][lat_key]['stddev']) / lat_factor

        return fio_result


class SysbenchOltpWringer(BaseWringer):
    bench_name = 'sysbench_oltp'

    def __init__(self, *args, **kwargs):
        super(SysbenchOltpWringer, self).__init__(*args, **kwargs)
        self.datastore_type = kwargs['datastore_type']
        self.volume_type = kwargs['volume_type']
        self.volume_size = kwargs['volume_size']
        self.script = kwargs['script']
        self.volume_flavor_id = kwargs['volume_flavor'] or None
        self.tables = kwargs['tables']
        self.table_size = kwargs['table_size']
        self.range_size = kwargs['range_size']
        self.skip_trx = kwargs['skip_trx']
        self.ssl = kwargs['ssl']

    def _get_data(self):
        sysbench_data = {
            'volume': self.volume_flavor_id,
            'datastore_type': self.datastore_type,
            'script': self.script,
            'tables': self.tables,
            'table_size': self.table_size,
            'range_size': self.range_size,
            'skip_trx': self.skip_trx,
            'ssl': self.ssl,
        }
        for line in self.input_:
            if 'Number of threads' in line:
                sysbench_data['num_thread'] = int(re.search('[\d]+', line).group())
            elif 'total time:' in line:
                sysbench_data['run_time'] = int(float(re.search('[\d]+[.]?[\d]*', line).group()))
            elif 'total number of events:' in line:
                sysbench_data['total_events'] = int(re.search('[\d]+', line).group())
            elif 'read:' in line:
                sysbench_data['read_queries'] = re.search('[\d]+', line).group()
            elif 'write:' in line:
                sysbench_data['write_queries'] = re.search('[\d]+', line).group()
            elif 'other:' in line:
                sysbench_data['other_queries'] = re.search('[\d]+', line).group()
            elif 'transactions:' in line:
                sysbench_data['transactions'] = re.search('(\d+)', line).group(1)
                sysbench_data['transaction_speed'] = re.search('(\d+\.\d*)', line).group()
            elif 'queries:' in line:
                sysbench_data['queries'] = re.search('(\d+)', line).group(1)
                sysbench_data['query_speed'] = re.search('(\d+\.\d*)', line).group()
            elif 'avg:' in line:
                sysbench_data['lat_avg'] = re.search('([\d\.]+)', line).group()
            elif 'min:' in line:
                sysbench_data['lat_min'] = re.search('([\d\.]+)', line).group()
            elif 'max:' in line:
                sysbench_data['lat_max'] = re.search('([\d\.]+)', line).group()
            elif '95th percentile:' in line:
                sysbench_data['lat_95p'] = line.split(':')[-1].strip()
            elif 'sum:' in line:
                sysbench_data['lat_sum'] = re.search('([\d\.]+)', line).group()
        return sysbench_data

    def _get_metadata(self):
        metadata = super(SysbenchOltpWringer, self)._get_metadata()
        # metadata['volume_flavor_id'] = self.volume_flavor_id
        return metadata


class PgbenchWringer(BaseWringer):
    bench_name = 'pgbench'

    def __init__(self, *args, **kwargs):
        super(PgbenchWringer, self).__init__(*args, **kwargs)
        self.datastore = kwargs['datastore']
        self.datastore_type = kwargs['datastore_type']
        self.volume_flavor_id = kwargs['volume_flavor'] or None
        self.volume_type = kwargs['volume_type']
        self.volume_size = kwargs['volume_size']

        self.script = kwargs['script']
        self.fillfactor = kwargs['fillfactor']
        self.rate = kwargs['rate']
        self.no_vacuum = kwargs['no_vacuum']
        self.foreignkey = kwargs['foreignkey']

    def _get_data(self):
        pg_data = {
            'volume': self.volume_flavor_id,
            'datastore': self.datastore,
            'datastore_type': self.datastore_type,
            'script': self.script,
            'fillfactor': self.fillfactor,
            'rate': self.rate,
            'no_vacuum': self.no_vacuum,
            'foreignkey': self.foreignkey,
        }
        for line in self.input_:
            if line.startswith('pgbench'):
                pg_data['version'] = line.strip()
            elif line.startswith('scaling factor:'):
                data = line.split(':')[1].strip()
                pg_data['scalefactor'] = data
            elif line.startswith('query mode'):
                data = line.split(':')[1].strip()
                pg_data['protocol'] = data
            elif line.startswith('number of clients:'):
                data = line.split(':')[1].strip()
                pg_data['client'] = data
            elif line.startswith('number of threads:'):
                data = line.split(':')[1].strip()
                pg_data['jobs'] = data
            elif line.startswith('duration:'):
                data = line.split(':')[1].split()[0]
                pg_data['time'] = data
            elif line.startswith('number of transactions actually processed:'):
                data = line.split(':')[1].strip()
                pg_data['transactions'] = data
            elif line.startswith('latency average = '):
                data = line.split('=')[1].split()[0]
                pg_data['lat_avg'] = data
            elif line.startswith('tps'):
                data = line.split('=')[1].split()[0]
                key = 'tps' if 'including' in line else 'tps_no_con'
                pg_data[key] = data
        return pg_data


class YcsbWringer(BaseWringer):
    bench_name = 'ycsb'

    def __init__(self, *args, **kwargs):
        super(YcsbWringer, self).__init__(*args, **kwargs)
        self.datastore = kwargs['datastore']
        self.datastore_type = kwargs['datastore_type']
        self.volume_flavor_id = kwargs['volume_flavor'] or None
        self.volume_type = kwargs['volume_type']
        self.volume_size = kwargs['volume_size']

    def _get_data(self):
        ycsb_data = {
            'volume': self.volume_flavor_id,
            'datastore': self.datastore,
            'datastore_type': self.datastore_type,
        }
        ops = ('read', 'cleanup', 'update', 'read_modify_write', 'insert', 'scan')
        for op in ops:
            ycsb_data.update({
                '%s_count' % op: 0,
                '%s_ok' % op: 0,
                '%s_error' % op: 0,
            })

        line1 = next(self.input_).strip()
        ycsb_data['threads'] = re.findall(r'-threads (\d*)', line1)[0]
        attrs = dict([
            opt.split('=', 1)
            for opt in re.findall('-p ([^ ]*)', line1)
        ])
        ycsb_data.update(attrs)

        key_to_id = {
            'RunTime(ms)': 'run_time',
            'Throughput(ops/sec)': 'ops',
            'Total Operations': 'count',
            'Average': 'avg',
            'Min': 'min',
            'Max': 'max',
            'p99.9': 'p99_9',
            'p99.99': 'p99_99',
            'Return=OK': 'ok',
            'Return=Error': 'error',
        }
        for line in self.input_:
            if line.startswith('version:'):
                ycsb_data['version'] = line.split(':')[1].strip()
                continue
            if not line.startswith('['):
                continue
            op, key, val = [i.strip() for i in line.split(',')]
            op = op[1:-1].lower()

            field_key = '%s_%s' % (op, key_to_id.get(key, key))
            ycsb_data[field_key] = val
        print(ycsb_data)
        return ycsb_data


class DdWringer(BaseWringer):
    bench_name = 'dd'

    def __init__(self, *args, **kwargs):
        super(DdWringer, self).__init__(*args, **kwargs)
        self.input_file = kwargs['if']
        self.output_file = kwargs['of']
        self.block_size = kwargs['bs']
        self.count = kwargs['count']
        self.volume_flavor_id = kwargs['volume_flavor_id']

    def _get_data(self, **kwargs):
        dd_data = self.input_.readlines()[-1].strip()
        dd_result = {
          ''
        }
        return dd_result


class VdbenchWringer(BaseWringer):
    bench_name = 'vdbench'

    def __init__(self, *args, **kwargs):
        super(VdbenchWringer, self).__init__(*args, **kwargs)
        for key, val in kwargs.items():
            setattr(self, key, val)
        if self.vdbench_config:
            self._parse_config(self.vdbench_config)

    def _parse_config(self, filename):
        with open(filename, 'r') as config_file:
            config = {
                line.strip().split('=')[0]: dict([
                    s.split('=')
                    for s in line.strip().split(',')
                ])
                for line in config_file
            }
        for prefix, setts in config.items():
            for key, value in setts.items():
                fieldname = "%s_%s" % (prefix, key)
                setattr(self, fieldname, value)
        self.rd_threads = config['rd'].get('threads', '8')
        self.fsd_directio = 'directio' in getattr(self, 'fsd_openflags', '')

    def _get_data(self, **kwargs):
        is_rd = False
        for line in self.input_.readlines():
            if 'Starting RD=' in line and not 'format_for_' in line:
                is_rd = True
                continue
            if is_rd and 'avg_2' in line:
                break
        date, i, ops, lat, cpu_total, cpu_sys, read_pct, read_iops, read_lat, write_iops, write_lat, read_bw, write_bw, bw, bs, mkdir_ops, mkdir_lat, rmdir_ops, rmdir_lat, create_ops, create_lat, open_ops, open_lat, close_ops, close_lat, delete_ops, delete_lat = line.split()
        vdbench_result = {
            'ops': ops,
            'lat': lat,
            'cpu_total': cpu_total,
            'cpu_sys': cpu_sys,
            'read_pct': read_pct,
            'read_iops': read_iops,
            'read_lat': read_lat,
            'write_iops': write_iops,
            'write_lat': write_lat,
            'read_bw': read_bw,
            'write_bw': write_bw,
            'bw': bw,
            'bs': bs,
            'mkdir_ops': mkdir_ops,
            'mkdir_lat': mkdir_lat,
            'rmdir_ops': rmdir_ops,
            'rmdir_lat': rmdir_lat,
            'create_ops': create_ops,
            'create_lat': create_lat,
            'open_ops': open_ops,
            'open_lat': open_lat,
            'close_ops': close_ops,
            'close_lat': close_lat,
            'delete_ops': delete_ops,
            'delete_lat': delete_lat,
        }
        if self.fwd_rdpct == 0:
            vdbench_result['operations'] = 'write'
        elif self.fwd_rdpct == 100:
            vdbench_result['operations'] = 'read'
        else:
            vdbench_result['operations'] = 'read,write'
        vdbench_result.update({
            'volume_flavor': self.volume_flavor_id,
            'width': self.fsd_width,
            'depth': self.fsd_depth,
            'files': self.fsd_files,
            'sizes': self.fsd_size,
            'directio': self.fsd_directio,
            'fileio': self.fwd_fileio,
            'xfersize': self.fwd_xfersize,
            'threads': self.rd_threads,
            'elapsed': self.rd_elapsed,
        })
        return vdbench_result


class MdtestWringer(BaseWringer):
    bench_name = 'mdtest'

    def __init__(self, *args, **kwargs):
        super(MdtestWringer, self).__init__(*args, **kwargs)
        self.depth = kwargs['depth']
        self.branching_factor = kwargs['branching_factor']
        self.bytes_ = kwargs['bytes']
        self.num_items = kwargs['num_items']

    def _get_data(self, **kwargs):
        vdbench_result = {
          'depth': self.depth,
          'branching_factor': self.branching_factor,
          'bytes': self.bytes_,
          'num_items': self.num_items
        }
        for line in self.input_.readlines():
            if 'Directory creation' in line:
                _max, _min, avg, stddev = line.split(':')[1].split()
                vdbench_result.update({
                  'dir_create_avg': avg,
                  'dir_create_stddev': stddev
                })
            elif 'Directory stat' in line:
                _max, _min, avg, stddev = line.split(':')[1].split()
                vdbench_result.update({
                  'dir_stat_avg': avg,
                  'dir_stat_stddev': stddev
                })
            elif 'Directory removal' in line:
                _max, _min, avg, stddev = line.split(':')[1].split()
                vdbench_result.update({
                  'dir_rm_avg': avg,
                  'dir_rm_stddev': stddev
                })
            elif 'File creation' in line:
                _max, _min, avg, stddev = line.split(':')[1].split()
                vdbench_result.update({
                  'file_create_avg': avg,
                  'file_create_stddev': stddev
                })
            elif 'File stat' in line:
                _max, _min, avg, stddev = line.split(':')[1].split()
                vdbench_result.update({
                  'file_stat_avg': avg,
                  'file_stat_stddev': stddev
                })
            elif 'File read' in line:
                _max, _min, avg, stddev = line.split(':')[1].split()
                vdbench_result.update({
                  'file_read_avg': avg,
                  'file_read_stddev': stddev
                })
            elif 'File removal' in line:
                _max, _min, avg, stddev = line.split(':')[1].split()
                vdbench_result.update({
                  'file_rm_avg': avg,
                  'file_rm_stddev': stddev
                })
            elif 'Tree creation' in line:
                _max, _min, avg, stddev = line.split(':')[1].split()
                vdbench_result.update({
                  'tree_create_avg': avg,
                  'tree_create_stddev': stddev
                })
            elif 'Tree removal' in line:
                _max, _min, avg, stddev = line.split(':')[1].split()
                vdbench_result.update({
                  'tree_rm_avg': avg,
                  'tree_rm_stddev': stddev
                })
        return vdbench_result


class BonnieWringer(BaseWringer):
    bench_name = 'bonnie'

    def __init__(self, *args, **kwargs):
        super(BonnieWringer, self).__init__(*args, **kwargs)
        self.bonnie_config = kwargs.get('bonnie_config', {})

    def _get_data(self, **kwargs):
        line =  self.input_.readlines()[-1]
        data = line.split(',')
        data = [(d if d and '+' not in d else None) for d in data]
        data[36:] = [convert_second(d) for d in data[36:]]
        bonnie_result = {
          'seq_write_bw_per_chr': data[7],
          'seq_write_bw_per_block': data[9],
          'seq_write_bw_per_rewrite': data[11],
          'seq_read_bw_per_chr': data[13],
          'seq_read_bw_per_block': data[15],

          'seq_write_lat_per_chr': data[36],
          'seq_write_lat_per_block': data[37],
          'seq_write_lat_per_rewrite': data[38],
          'seq_read_lat_per_chr': data[39],
          'seq_read_lat_per_block': data[40],

          'rand_seek_ops': data[17],
          'rand_seek_lat': data[41],

          'seq_create_ops': data[24],
          'seq_read_ops': data[26],
          'seq_delete_ops': data[28],
          'rand_create_ops': data[30],
          'rand_read_ops': data[32],
          'rand_delete_ops': data[34],

          'seq_create_lat': data[42],
          'seq_read_lat': data[43],
          'seq_delete_lat': data[44],
          'rand_create_lat': data[45],
          'rand_read_lat': data[46],
          'rand_delete_lat': data[47],
        }
        bonnie_result.update(self.bonnie_config)
        return bonnie_result

HIBENCH_ATTRS = (
  # Filesystem
  ('file_read', 'FILE: Number of bytes read=', 1024),
  ('file_read_operations', 'FILE: Number of read operations', 1),
  ('file_large_read_operations', 'FILE: Number of large read operations', 1),
  ('file_write', 'FILE: Number of bytes written', 1024),
  ('file_write_operations', 'FILE: Number of write operations', 1),
  ('hdfs_read', 'HDFS: Number of bytes read=', 1024),
  ('hdfs_read_operations', 'HDFS: Number of read operations', 1),
  ('hdfs_large_read_operations', 'HDFS: Number of large read operations', 1),
  ('hdfs_write', 'HDFS: Number of bytes written', 1024),
  ('hdfs_write_operations', 'HDFS: Number of write operations', 1),
  # Job
  ('killed_reduce', 'Killed reduce tasks', 1),
  ('map_tasks', 'Launched map tasks', 1),
  ('reduce_tasks', 'Launched reduce tasks', 1),
  ('rack_local_map_tasks', 'Rack-local map tasks', 1),
  ('map_time', 'time spent by all map tasks (ms)', 1000),
  ('map_time_in_slot', 'time spent by all maps in occupied slots', 1000),
  ('map_vcore_time', 'vcore-milliseconds taken by all map', 1000),
  ('map_mb_ms', 'megabyte-milliseconds taken by all map', 1024),
  ('reduce_time', 'time spent by all reduce tasks (ms)', 1000),
  ('reduce_time_in_slot', 'time spent by all reduces in occupied slots', 1000),
  ('reduce_vcore_time', 'vcore-milliseconds taken by all reduce tasks', 1000),
  ('reduce_mb_ms', 'megabyte-milliseconds taken by all reduce tasks', 1024),
  ('map_input_records', 'Map input records', 10**6),
  ('map_output_records', 'Map output records', 10**6),
  ('map_output_bytes', 'Map output bytes', 1024),
  ('map_output_mat_bytes', 'Map output materialized bytes', 1024),
  ('input_split_bytes', 'Input split bytes', 1024),
  ('combine_input_records', 'Combine input records', 10**6),
  ('combine_output_records', 'Combine output records', 10**6),
  ('reduce_input_groups', 'Reduce input groups', 1),
  ('reduce_shuffle_bytes', 'Reduce shuffle bytes', 1024),
  ('reduce_input_records', 'Reduce input records', 10**6),
  ('reduce_output_records', 'Reduce output records', 10**6),
  ('spilled_records', 'Spilled Records', 10**6),
  ('shuffled_maps', 'Shuffled Maps', 1),
  ('failed_shuffle', 'Failed Shuffles', 1),
  ('merged_map_outputs', 'Merged Map outputs', 1),
  ('gc_time', 'GC time elapsed', 1000),
  ('cpu_time', 'CPU time spent', 1000),
  ('physical_memory', 'Physical memory (bytes)', 2**20),
  ('virtual_memory', 'Virtual memory (bytes)', 2**20),
  ('total_commited_heap_usage', 'Total committed heap usage (bytes)', 2**20),
  # Suffle errors
  ('bad_id', 'BAD_ID', 1),
  ('connection', 'CONNECTION', 1),
  ('io_error', 'IO_ERROR', 1),
  ('wrong_length', 'WRONG_LENGTH', 1),
  ('wrong_map', 'WRONG_MAP', 1),
  ('wrong_reduce', 'WRONG_REDUCE', 1),
  # File format counter
  ('file_format_bytes_read', 'Bytes Read', 1024),
  ('file_format_bytes_write', 'Bytes Written', 1024),
)
class BaseHiBenchWringer(BaseWringer):
    def __init__(self, architecture, report_dir, size, *args, **kwargs):
        super(BaseHiBenchWringer, self).__init__(*args, **kwargs)
        self.report_dir = report_dir
        self.size = size
        self.architecture = architecture

    @property
    def report_filename(self):
        return os.path.join(self.report_dir, self.bench_name, self.framework, 'bench.log')

    @property
    def time_filename(self):
        return os.path.join(self.report_dir, self.bench_name, self.framework, 'time.txt')

    def _get_data(self):
        bench_data = {
          'framework': self.framework,
          'size': self.size,
          'arch': self.architecture,
        }
        report_file = open(self.report_filename)
        try:
            with open(self.time_filename) as fd:
                bench_data['exec_time'] = int(float(fd.read().strip()))
        except (IOError, ValueError):
            pass
        is_report = False
        for line in report_file:
            if not is_report:
               is_report = 'File System Counters' in line
               continue
            attr = [(i, j, k) for i, j, k in HIBENCH_ATTRS if j in line]
            if attr:
                _, value = line.split('=')
                bench_data[attr[0][0]] = int(value.strip()) / attr[0][2]
        return bench_data


class WordcountWringer(BaseHiBenchWringer):
    framework = 'hadoop'
    bench_name = 'wordcount'


class TeraSortWringer(BaseHiBenchWringer):
    framework = 'hadoop'
    bench_name = 'terasort'


DFSIO_ATTRS = (
  ('throughtput', 'Throughput mb/sec'),
  ('io_avg', 'Average IO rate mb/sec'),
  ('io_rate', 'IO rate std deviation'),
  ('exec_time', 'Test exec time sec'),
  ('aggr_throughtput', 'Average of Aggregated Throughput'),
  ('aggr_throughtput_stddev', 'Standard Deviation'),
)
class DfsioWringer(BaseWringer):
    framework = 'hadoop'
    bench_name = 'dfsio'

    def __init__(self, architecture, report_dir, size, *args, **kwargs):
        super(DfsioWringer, self).__init__(*args, **kwargs)
        self.report_dir = report_dir
        self.size = size
        self.architecture = architecture

    @property
    def report_filename(self):
        bench_name = 'dfsioe'
        filename = 'result_%s.txt' % self.mode
        return os.path.join(self.report_dir, bench_name, self.framework, filename)

    @property
    def time_filename(self):
        bench_name = 'dfsioe'
        return os.path.join(self.report_dir, bench_name, self.framework, 'time.txt')

    def run(self):
        self.mode = 'read'
        super(DfsioWringer, self).run()
        self.mode = 'write'
        super(DfsioWringer, self).run()

    def _get_data(self):
        bench_data = {
          'framework': self.framework,
          'size': self.size,
          'arch': self.architecture,
          'mode': self.mode,
        }
        report_file = open(self.report_filename)
        try:
            with open(self.time_filename) as fd:
                bench_data['exec_time'] = fd.read()
        except IOError:
            pass
        for line in report_file:
            attr = [(i, j) for i, j in DFSIO_ATTRS if j in line]
            if attr:
                _, value = line.split(':')
                bench_data[attr[0][0]] = float(value.strip().split()[0])
        return bench_data


class BaseNetworkWringer(BaseWringer):
    def __init__(self, destination_id, destination_type, datacenter=None, *args, **kwargs):
        super(BaseNetworkWringer, self).__init__(*args, **kwargs)
        self.destination_id = destination_id
        self.destination_type = destination_type
        self.datacenter = datacenter


class TracepathWringer(BaseNetworkWringer):
    bench_name = 'tracepath'

    def _get_data(self):
        for line in self.input_:
            match_time = TRACEPATH_TIME_REG.match(line)
            if match_time is not None:
                time_ = match_time.groups()[0]
                continue
            match = TRACEPATH_RESULT_REG.match(line)
            if match is not None:
                mtu, hops, back = match.groups()
                break
        else:
            raise exceptions.ParseError()
        dest_key = 'dest_%s' % self.destination_type
        data = {
            'time': time_,
            'mtu': mtu,
            'hops': hops,
            'back': back,
            dest_key: self.destination_id,
        }
        if self.datacenter:
            data['dest_datacenter'] = self.datacenter
        return data


class IperfWringer(BaseNetworkWringer):
    bench_name = 'iperf'

    def __init__(self, dest_instance_type, *args, **kwargs):
        kwargs['destination_id'] = dest_instance_type
        kwargs['destination_type'] = 'instance_type'
        super(IperfWringer, self).__init__(*args, **kwargs)
        self.dest_instance_type = dest_instance_type

    def _get_data(self):
        result = json.load(self.input_)
        mode = 'download' if result['start']['test_start']['reverse'] else 'upload'
        transport = result['start']['test_start']['protocol'].lower()
        data = {
          'time': result['start']['test_start']['duration'],
          'num_thread': result['start']['test_start']['num_streams'],
          'transport': transport,
          'mode': mode,
          'client_bytes': result['end']['sum_sent']['bytes'] / 2**20,
          'client_bandwidth': result['end']['sum_sent']['bits_per_second'] / 2**20,
          'server_bytes': result['end']['sum_received']['bytes'] / 2**20,
          'server_bandwidth': result['end']['sum_received']['bits_per_second'] / 2**20,
          'dest_instance_type': self.dest_instance_type,
        }
        if self.datacenter:
            data['dest_datacenter'] = self.datacenter
        return data


GEEKBENCH4_FIELDS = {
    # V3
    'Twofish': ('twofish', 2**20),
    'SHA1': ('sha1', 2**20),
    'SHA2': ('sha2', 2**20),
    'BZip2 Compress': ('bzip_compress', 2**20),
    'BZip2 Decompress': ('bzip_decompress', 2**20),
    'JPEG Compress': ('jpeg_compress', 10**6),
    'JPEG Decompress': ('jpeg_decompress', 10**6),
    'PNG Compress': ('png_compress', 10**6),
    'PNG Decompress': ('png_decompress', 10**6),
    'Sobel': ('sobel', 10**6),
    'BlackScholes': ('blackscholes', 10**6),
    'Mandelbrot': ('mandelbrot', 10**9),
    'Sharpen Filter': ('sharpen', 10**9),
    'Blur Filter': ('blur', 10**9),
    'SGEMM': ('sgemm', 10**9),
    'DGEMM': ('dgemm', 10**9),
    'SFFT': ('sfft', 10**9),
    'DFFT': ('dfft', 10**9),
    'N-Body': ('nbody', 10**6),
    'Stream Copy': ('stream_copy', 2**30),
    'Stream Scale': ('stream_scale', 2**30),
    'Stream Add': ('streama_add', 2**30),
    'Stream Triad': ('stream_triad', 2**30),
    'Ray Trace': ('ray', 10**6),
    # V4
    'AES': ('aes', 2**30),
    'LZMA': ('lzma', 2**20),
    'JPEG': ('jpeg', 10**6),
    'Canny': ('canny', 10**6),
    'Lua': ('lua', 2**20),
    'Dijkstra': ('dijkstra', 10**6),
    'SQLite': ('sqlite', 10**3),
    'HTML5 Parse': ('html5_parse', 2**20),
    'HTML5 DOM': ('html5_dom', 10**6),
    'Histogram Equalization': ('histogram', 10**6),
    'PDF Rendering': ('pdf', 10**6),
    'LLVM': ('llvm', 1),
    'Camera': ('camera', 1),
    'SGEMM': ('sgemm', 10**9),
    'N-Body Physics': ('nbody', 10**6),
    'Ray Tracing': ('ray', 10**3),
    'Rigid Body Physics': ('rigid', 1),
    'HDR': ('hdr', 10**6),
    'Gaussian Blur': ('gaussian', 10**6),
    'Speech Recognition': ('speech', 1),
    'Face Detection': ('face', 10**3),
    'Memory Copy': ('memory_copy', 2**30),
    'Memory Latency': ('memory_latency', 10**12),
    'Memory Bandwidth': ('memory_bandwidth', 2**30),
    # V4 OpenCL
    'RAW': ('raw', 2**30),
    'Depth of Field': ('depth_field', 2**30),
    'Particle Physics': ('particle_physics', 1),
}
class Geekbench4Wringer(BaseWringer):
    bench_name = 'geekbench4'

    def __init__(self, format, mode, *args, **kwargs):
        super(Geekbench4Wringer, self).__init__(*args, **kwargs)
        self.format = format
        self.mode = mode

    def parse_json(self):
        raw_results = json.load(self.input_)
        data = {
            'version': raw_results['version'],
            'runtime': raw_results['runtime'],
            'mode': self.mode,
        }
        for section in raw_results['sections']:
            if section['name'] == "Multi-Core":
                prefix = 'multi'
            else:
                prefix = 'single'
            for raw_result in section['workloads']:
                key, format_ = GEEKBENCH4_FIELDS[raw_result['name']]
                fieldname = '%s_%s' % (prefix, key)
                data[fieldname] = raw_result['workload_rate'] / format_
                if raw_result['name'] == 'Memory Latency':
                    data[fieldname] = data[fieldname] / raw_result['work']
                data[fieldname+'_score'] = raw_result['score']
        return data

    def parse_csv(self):
        data = {}
        raw_results = csv.reader(self.input_)
        next(raw_results)
        for line in raw_results:
            if not line:
                continue
            mode = 'single' if line[1] == "1" else 'multi'
            key, format_ = GEEKBENCH4_FIELDS[line[0]]
            fieldname = '%s_%s' % (mode, key)
            data[fieldname] = int(line[7]) / format_
            if line[0] == 'Memory Latency':
                data[fieldname] = data[fieldname] / int(line[7])
            data[fieldname+'_score'] = int(line[4])
        return data

    def _get_data(self):
        if self.format == 'csv':
            data = self.parse_csv()
        else:
            data = self.parse_json()
        if self.mode == 'standard':
            single_crypto_scores = [data['single_aes_score']]
            single_crypto_score = geo_mean(single_crypto_scores)
            multi_crypto_scores = [data['multi_aes_score']]
            multi_crypto_score = geo_mean(multi_crypto_scores)
            single_int_scores = [data['single_%s_score' % f] for f in [
                'lzma', 'jpeg', 'canny', 'lua', 'dijkstra', 'sqlite',
                'html5_parse', 'html5_dom', 'histogram', 'pdf',
                'llvm', 'camera'
            ]]
            single_int_score = geo_mean(single_int_scores)
            multi_int_scores = [data['multi_%s_score' % f] for f in [
                'lzma', 'jpeg', 'canny', 'lua', 'dijkstra', 'sqlite',
                'html5_parse', 'html5_dom', 'histogram', 'pdf',
                'llvm', 'camera'
            ]]
            multi_int_score = geo_mean(multi_int_scores)
            single_float_scores = [data['single_%s_score' % f] for f in [
                'sgemm', 'sfft', 'nbody', 'ray', 'rigid', 'hdr', 'gaussian',
                'speech', 'face'
            ]]
            single_float_score = geo_mean(single_float_scores)
            multi_float_scores = [data['multi_%s_score' % f] for f in [
                'sgemm', 'sfft', 'nbody', 'ray', 'rigid', 'hdr', 'gaussian',
                'speech', 'face'
            ]]
            multi_float_score = geo_mean(multi_float_scores)
            single_memory_scores = [data['single_%s_score' % f] for f in [
                'memory_copy', 'memory_latency', 'memory_bandwidth'
            ]]
            single_memory_score = geo_mean(single_memory_scores)
            multi_memory_scores = [data['multi_%s_score' % f] for f in [
                'memory_copy', 'memory_latency', 'memory_bandwidth'
            ]]
            multi_memory_score = geo_mean(multi_memory_scores)
            single_score = weighted_mean((
                (5, single_crypto_score),
                (45, single_int_score),
                (30, single_float_score),
                (20, single_memory_score),
            ))
            multi_score = weighted_mean((
                (5, multi_crypto_score),
                (45, multi_int_score),
                (30, multi_float_score),
                (20, multi_memory_score),
            ))
            data.update(single_crypto_score=single_crypto_score,
                        multi_crypto_score=multi_crypto_score,
                        single_int_score=single_int_score,
                        multi_int_score=multi_int_score,
                        single_float_score=single_float_score,
                        multi_float_score=multi_float_score,
                        single_memory_score=single_memory_score,
                        multi_memory_score=multi_memory_score,
                        single_score=single_score,
                        multi_score=multi_score)
        return data


GEEKBENCH5_FIELDS = GEEKBENCH4_FIELDS.copy()
GEEKBENCH5_FIELDS.update({
    'AES-XTS': ('aes_xts', 2**30),
    'Text Compression': ('text_compression', 2**20),
    'Image Compression': ('image_compression', 2**20),
    'Navigation': ('navigation', 1),
    'HTML5': ('html5', 2**20),
    'SQLite': ('sqlite', 2**10),
    'PDF Rendering': ('pdf_rendering', 2**20),
    'N-Body Physics': ('nbody_physics', 2**20),
    'Text Rendering': ('text_rendering', 2**20),
    'Clang': ('clang', 2**20),
    'Camera': ('camera', 1),
    'Gaussian Blur': ('gaussian_blur', 2**20),
    'Face Detection': ('face_detection', 1),
    'Horizon Detection': ('horizon_detection', 2**20),
    'Image Inpainting': ('image_inpainting', 2**20),
    'HDR': ('hdr', 2**20),
    'Ray Tracing': ('ray_tracing', 2**10),
    'Structure from Motion': ('structure_from_motion', 2**10),
    'Machine Learning': ('machine_learning', 1),
    'Stereo Matching': ('stereo_matching', 10**6),
    'Depth of Field': ('depth_field', 10**9),
    'Feature Matching': ('feature_matching', 10**9),
})
class Geekbench5Wringer(BaseWringer):
    bench_name = 'geekbench5'

    def __init__(self, format, mode, *args, **kwargs):
        super(Geekbench5Wringer, self).__init__(*args, **kwargs)
        self.format = format
        self.mode = mode
        self._fields = GEEKBENCH5_FIELDS.copy()

    def parse_json(self):
        raw_results = json.load(self.input_)
        data = {
            'version': raw_results['version'],
            'runtime': raw_results['runtime'],
            'mode': self.mode,
        }
        for section in raw_results['sections']:
            if section['name'] == "Multi-Core":
                prefix = 'multi'
            else:
                prefix = 'single'
            for raw_result in section['workloads']:
                key, format_ = self._fields[raw_result['name']]
                fieldname = '%s_%s' % (prefix, key)
                data[fieldname] = raw_result['workload_rate'] / format_
                data[fieldname+'_score'] = raw_result['score']
        return data

    def parse_csv(self):
        data = {}
        raw_results = csv.reader(self.input_)
        next(raw_results)
        for line in raw_results:
            if not line:
                continue
            mode = 'single' if line[1] == "1" else 'multi'
            key, format_ = self._fields[line[0]]
            fieldname = '%s_%s' % (mode, key)
            data[fieldname] = int(line[7]) / format_
            data[fieldname+'_score'] = int(line[4])
        return data

    def _get_data(self):
        if self.format == 'csv':
            data = self.parse_csv()
        else:
            data = self.parse_json()
        if self.mode == 'standard':
            single_crypto_scores = [data['single_aes_xts_score']]
            single_crypto_score = geo_mean(single_crypto_scores)
            multi_crypto_scores = [data['multi_aes_xts_score']]
            multi_crypto_score = geo_mean(multi_crypto_scores)
            single_int_scores = [data['single_%s_score' % f] for f in [
                'text_compression', 'image_compression', 'navigation',
                'sqlite', 'html5', 'pdf_rendering', 'text_rendering',
                'clang', 'camera',
            ]]
            single_int_score = geo_mean(single_int_scores)
            multi_int_scores = [data['multi_%s_score' % f] for f in [
                'text_compression', 'image_compression', 'navigation',
                'sqlite', 'html5', 'pdf_rendering', 'text_rendering',
                'clang', 'camera',
            ]]
            multi_int_score = geo_mean(multi_int_scores)
            single_float_scores = [data['single_%s_score' % f] for f in [
                'text_compression', 'nbody_physics', 'rigid',
                'gaussian_blur', 'face_detection', 'horizon_detection',
                'image_inpainting', 'hdr', 'ray_tracing', 'structure_from_motion',
                'speech', 'machine_learning',
            ]]
            single_float_score = geo_mean(single_float_scores)
            multi_float_scores = [data['multi_%s_score' % f] for f in [
                'text_compression', 'nbody_physics', 'rigid',
                'gaussian_blur', 'face_detection', 'horizon_detection',
                'image_inpainting', 'hdr', 'ray_tracing', 'structure_from_motion',
                'speech', 'machine_learning',
            ]]
            multi_float_score = geo_mean(multi_float_scores)
            single_score = weighted_mean((
                (5, single_crypto_score),
                (65, single_int_score),
                (30, single_float_score),
            ))
            multi_score = weighted_mean((
                (5, multi_crypto_score),
                (65, multi_int_score),
                (30, multi_float_score),
            ))
            data.update(
                single_crypto_score=single_crypto_score,
                multi_crypto_score=multi_crypto_score,
                single_int_score=single_int_score,
                multi_int_score=multi_int_score,
                single_float_score=single_float_score,
                multi_float_score=multi_float_score,
                single_score=single_score,
                multi_score=multi_score
            )
        return data


GEEKBENCH6_FIELDS = {
    # Int
    'file_compression': 2**20,  # in MB/sec
    'navigation': 1,  # in routes/sec
    'html5_browser': 1,  # in pages/sec
    'pdf_renderer': 2**20,  # in Mpixels/sec
    'photo_library': 2**10,  # in Klines/sec
    'clang': 2**0,  # in Klines/sec
    'text_processing': 1,  # in pages/sec
    'asset_compression': 2**20,  # in MB/sec
    # Float
    'object_detection': 1,  # in images/sec
    'background_blur': 1,  # in images/sec
    'horizon_detection': 2**20,  # in Mpixels/sec
    'object_remover': 2**20,  # in Mpixels/sec
    'hdr': 2**20,  # in Mpixels/sec
    'photo_filter': 1,  # in images/sec
    'ray_tracer': 2**20,  # in Mpixels/sec
    'structure_from_motion': 2**10,  # in Kpixels/sec
}
GB6_INTS = (
    'file_compression',
    'navigation',
    'html5_browser',
    'pdf_renderer',
    'photo_library',
    'clang',
    'text_processing',
    'asset_compression',
)
GB6_FLOATS = (
    'object_detection',
    'background_blur',
    'horizon_detection',
    'object_remover',
    'hdr',
    'photo_filter',
    'ray_tracer',
    'structure_from_motion',
)

class Geekbench6Wringer(BaseWringer):
    bench_name = 'geekbench6'

    def __init__(self, mode, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mode = mode
        self._fields = GEEKBENCH6_FIELDS.copy()

    def parse_json(self):
        raw_results = json.load(self.input_)
        data = {
            'version': raw_results['version'],
            'runtime': raw_results['runtime'],
            'mode': self.mode,
            'iterations': raw_results['options']['iterations'],
            'cpu_workers': raw_results['options']['cpu_workers'],
            'memory_workers': raw_results['options']['memory_workers'],
            'workload_gap': raw_results['options']['workload_gap'],
            'single_score': raw_results['score'],
            'multi_score': raw_results['multicore_score'],
        }
        for section in raw_results['sections']:
            if section['name'] == "Multi-Core":
                prefix = 'multi'
            else:
                prefix = 'single'
            data.update({
                "%s_score" % prefix: section['score']
            })
            for raw_result in section['workloads']:
                key = raw_result['name'].lower().replace(' ', '_')
                format_ = self._fields[key]
                fieldname = '%s_%s' % (prefix, key)
                data[fieldname] = raw_result['rate'] / format_
                data[fieldname+'_score'] = raw_result['score']
        return data

    def _get_data(self):
        data = self.parse_json()
        # Make section scores
        single_int_scores = [data['single_%s_score' % f] for f in GB6_INTS]
        single_int_score = geo_mean(single_int_scores)
        multi_int_scores = [data['multi_%s_score' % f] for f in GB6_INTS]
        multi_int_score = geo_mean(multi_int_scores)
        single_float_scores = [data['single_%s_score' % f] for f in GB6_FLOATS]
        single_float_score = geo_mean(single_float_scores)
        multi_float_scores = [data['multi_%s_score' % f] for f in GB6_FLOATS]
        multi_float_score = geo_mean(multi_float_scores)
        # Make global scores
        single_score = weighted_mean((
            (65, single_int_score),
            (35, single_float_score),
        ))
        multi_score = weighted_mean((
            (65, multi_int_score),
            (35, multi_float_score),
        ))
        data.update(
            single_int_score=single_int_score,
            multi_int_score=multi_int_score,
            single_float_score=single_float_score,
            multi_float_score=multi_float_score,
            single_score=single_score,
            multi_score=multi_score
        )
        return data


GEEKBENCH3_SECTIONS = {
  'Integer': 'integer',
  'Floating Point': 'float',
  'Memory': 'memory',
}
class Geekbench3Wringer(BaseWringer):
    bench_name = 'geekbench3'

    def _get_data(self):
        raw_results = json.load(self.input_)
        data = {
            'single_score': raw_results['score'],
            'multi_score': raw_results['multicore_score'],
            'runtime': raw_results['runtime'],
            'version': raw_results['version'],
        }
        for section in raw_results['sections']:
            section_key = GEEKBENCH3_SECTIONS[section['name']]
            section_single_score_key = 'single_%s_score' % section_key
            section_multi_score_key = 'multi_%s_score' % section_key
            data.update({
              section_single_score_key: section['score'],
              section_multi_score_key: section['multicore_score'],
            })
            for workload in section['workloads']:
                key, format_ = GEEKBENCH4_FIELDS[workload['name']]
                single_result, multi_result = workload['results']
                single_fieldname = 'single_%s' % key
                multi_fieldname = 'multi_%s' % key
                data.update({
                  single_fieldname: single_result['workload_rate'] / format_,
                  single_fieldname+'_score': single_result['score'],
                  multi_fieldname: multi_result['workload_rate'] / format_,
                  multi_fieldname+'_score': multi_result['score'],
                })
        return data


class SpecCpu2006Wringer(BaseWringer):
    bench_name = 'spec_cpu2006'

    def _guess_c_compiler(self, text):
        if 'intel' in text.lower():
            version = re.match('.*Version ([\d.]*).*', text).groups()[0]
            return ('icc', version)
        if 'gcc' in text.lower():
            version = re.match('.*(\d\.\d\.\d).*', text).groups()[0]
            return ('gcc', version)
        if 'microsoft' in text.lower():
            version = re.match('.*Version ([\d.]*).*', text).groups()[0]
            return ('microsoft', version)
        return ('', '')

    def _guess_fortran_compiler(self, text):
        if 'intel' in text.lower():
            version = re.match('.*Version ([\d.]*).*', text).groups()[0]
            return ('icc', version)
        if 'gcc' in text.lower():
            version = re.match('.*(\d\.\d\.\d).*', text).groups()[0]
            return ('gcc', version)
        if 'microsoft' in text.lower():
            version = re.match('.*Version ([\d.]*).*', text).groups()[0]
            return ('microsoft', version)

    def run(self):
        """
        Public runner to parse and publish result.
        """
        raw_results = csv.reader(self.input_)
        data = []
        config_data = {}
        section = None
        error = None
        data = []
        for line in raw_results:
            if not line:
                continue
            # Stop at "Submit Notes"
            if section == 'Submit Notes':
                break
            # Set current section
            if len(line) == 1 and line[0][0] not in '#- ':
                section = line[0]
                continue
            # Skip sections
            if section in ('Selected Results Table',):
                continue
            # Parse result
            if len(line) == 12 and line[0] != 'Benchmark':
                if not line[2]:
                    continue
                result = self._get_data(line)
                data.append(result)
            # Get compiler
            if section == 'SOFTWARE':
                if line[0] == 'Compiler':
                    comp, version = self._guess_c_compiler(line[1])
                    config_data.update(c_compiler=comp, c_compiler_version=version)
                if 'fortran' in line[1].lower():
                    comp_version = self._guess_fortran_compiler(line[1])
                    if comp_version is None:
                        comp, version = config_data['c_compiler'], config_data['c_compiler_version']
                    else:
                        comp, version = comp_version
                    config_data.update(fortran_compiler=comp, fortran_compiler_version=version)
        if not config_data.get('fortran_compiler'):
            config_data['fortran_compiler'], config_data['fortran_compiler_version'] = config_data['c_compiler'], config_data['c_compiler_version']
        # Send result
        error = None
        for result in data:
            result.update(config_data)
            # Set mode
            try:
                response = self.client.post_result(
                    bench_name=self.bench_name,
                    data=result,
                    metadata=self._get_metadata())
                if response.status_code >= 300:
                    error = exceptions.ServerError(response.content + str(data))
            except KeyboardInterrupt:
                raise SystemExit(1)
            except Exception as err:
                error = err
        if error is not None:
            raise error

    def _get_data(self, line):
        copies = int(line[1])
        mode = 'rate' if copies > 1 else 'speed'
        data = {
            'test': line[0],
            'mode': mode,
            'copies': copies,
            'base_run_time': line[2],
            'base_rate': line[3],
        }
        return data


class SpecCpu2017Wringer(BaseWringer):
    bench_name = 'spec_cpu2017'

    def _guess_c_compiler(self, text):
        if 'intel' in text.lower():
            version = re.match('.*Version ([\d.]*).*', text).groups()[0]
            return ('icc', version)
        if 'gcc' in text.lower():
            version = re.match('.*Version ([\d.]*).*', text).groups()[0]
            return ('gcc', version)
        if 'microsoft' in text.lower():
            version = re.match('.*Version ([\d.]*).*', text).groups()[0]
            return ('microsoft', version)
        return ('', '')

    def _guess_fortran_compiler(self, text):
        if 'intel' in text.lower():
            version = re.match('.*Version ([\d.]*).*', text).groups()[0]
            return ('icc', version)
        if 'gcc' in text.lower():
            version = re.match('.*Version ([\d.]*).*', text).groups()[0]
            return ('gcc', version)
        if 'microsoft' in text.lower():
            version = re.match('.*Version ([\d.]*).*', text).groups()[0]
            return ('microsoft', version)

    def run(self):
        """
        Public runner to parse and publish result.
        """
        raw_results = csv.reader(self.input_)
        data = []
        config_data = {}
        section = None
        error = None
        data = []
        for line in raw_results:
            if not line:
                continue
            # Stop at "Submit Notes"
            if section == 'Submit Notes':
                break
            # Set mode
            if line[0].startswith('SPEC CPU2017 '):
                mode = 'rate' if line[0].endswith(' Rate Result') else 'speed'
            # Set current section
            if len(line) == 1 and line[0][0] not in '#- ':
                section = line[0]
                continue
            # Skip sections
            if section in ('Selected Results Table',):
                continue
            # Parse result
            if len(line) == 12 and line[0] != 'Benchmark':
                if not line[2]:
                    continue
                result = self._get_data(line, mode)
                data.append(result)
            # Get compiler
            if section == 'SOFTWARE':
                if line[0] == 'Compiler':
                    comp, version = self._guess_c_compiler(line[1])
                    config_data.update(c_compiler=comp, c_compiler_version=version)
                if 'fortran' in line[1].lower():
                    comp_version = self._guess_fortran_compiler(line[1])
                    if comp_version is None:
                        comp, version = config_data['c_compiler'], config_data['c_compiler_version']
                    else:
                        comp, version = comp_version
                    config_data.update(fortran_compiler=comp, fortran_compiler_version=version)
        if not config_data.get('fortran_compiler'):
            config_data['fortran_compiler'], config_data['fortran_compiler_version'] = config_data['c_compiler'], config_data['c_compiler_version']
        # Send result
        error = None
        for result in data:
            result.update(config_data)
            try:
                response = self.client.post_result(
                    bench_name=self.bench_name,
                    data=result,
                    metadata=self._get_metadata())
                if response.status_code >= 300:
                    error = exceptions.ServerError(response.content + str(data))
            except KeyboardInterrupt:
                raise SystemExit(1)
            except Exception as err:
                error = err
        if error is not None:
            raise error

    def _get_data(self, line, mode):
        if mode == 'rate':
            return {
                'test': line[0],
                'mode': mode,
                'copies': line[1],
                'base_run_time': line[2],
                'base_rate': line[3],
            }
        else:
            return {
                'test': line[0],
                'mode': mode,
                'copies': line[1],
                'base_run_time': line[2],
                'base_ratio': line[3],
            }


class FinanceBenchWringer(BaseWringer):
    bench_name = 'financebench'

    def __init__(self, *args, **kwargs):
        super(FinanceBenchWringer, self).__init__(*args, **kwargs)
        self.app = kwargs.get('app')
        self.mode = kwargs.get('mode')
        self.compiler = kwargs.get('compiler')
        self.compiler_version = kwargs.get('compiler_version')

    def _get_data(self):
        data = {
            'mode': self.mode,
            'app': self.app,
            'compiler': self.compiler,
            'compiler_version': self.compiler_version,
        }
        for line in self.input_:
            match = REG_FINANCEBENCH.match(line)
            if match is None:
                continue
            pu_type, mode, p_time = match.groups()
            if self.mode == 'CPU' and pu_type == 'C':
                data['time'] = float(p_time)
                break
            elif self.mode != 'CPU' and pu_type != 'C':
                data['time'] = float(p_time)
                break
        return data


LAMMPS_SECTIONS = {
    'Pair': 'pair',
    'Neigh': 'neigh',
    'Comm': 'comm',
    'Output': 'output',
    'Modify': 'modify',
    'Other': 'other',
    'Kspace': 'kspace',
    'Reduce': 'reduce',
}
class LammpsWringer(BaseWringer):
    bench_name = 'lammps'

    def __init__(self, test, num_process, *args, **kwargs):
        super(LammpsWringer, self).__init__(*args, **kwargs)
        self.test = test
        self.num_process = num_process

    def _parse_row(self, row, data):
        match = REG_LAMMPS_ROW.match(row)
        if match is None:
            return data
        section, min_, avg, max_, varavg, total = match.groups()
        if section in LAMMPS_SECTIONS:
            key_prefix = LAMMPS_SECTIONS[section]
            data.update({
                '%s_min' % key_prefix: min_,
                '%s_avg' % key_prefix: avg,
                '%s_max' % key_prefix: max_,
                '%s_varavg' % key_prefix: varavg,
                '%s_total' % key_prefix: total,
            })
        return data

    def _get_data(self):
        data = {
            'test': self.test,
            'num_process': self.num_process,
        }
        parsing_table = False
        for line in self.input_:
            if not line.strip():
                continue
            if line.startswith('-----'):
                parsing_table = True
            if parsing_table:
                data = self._parse_row(line, data)
        return data


class VRayWringer(BaseWringer):
    bench_name = 'vray'

    def __init__(self, *args, **kwargs):
        super(VRayWringer, self).__init__(*args, **kwargs)
        self.unit = kwargs.get('unit')

    def _get_data(self):
        vray_output = self.input_.read()
        search = REG_VRAY.search(vray_output)
        minutes, seconds = [int(i) for i in search.groups()]
        time_ = minutes * 60 + seconds
        return {
          'time': time_,
          'unit': self.unit,
        }


class VaspTestWringer(BaseWringer):
    bench_name = 'vasptest'

    def __init__(self, *args, **kwargs):
        super(VaspTestWringer, self).__init__(*args, **kwargs)
        self.num_process = kwargs.get('num_process')
        self.scenario = kwargs.get('scenario')
        self.executable = kwargs.get('executable')

    def _get_data(self):
        for line in self.input_:
            match = REG_VASP.match(line)
            if match is not None:
                break
        else:
            raise InvalidInput()
        minutes, seconds = match.groups()
        time_ = int(minutes) * 60 + float(seconds)
        return {
          'time': time_,
          'num_process': self.num_process,
          'scenario': self.scenario,
          'executable': self.executable,
          'python_version': PYTHON_VERSION,
          'behave_version': BEHAVE_VERSION,
        }


class PhoronixTestSuiteWringer(BaseWringer):
    bench_name = 'phoronix_test_suite'

    def __init__(self, *args, **kwargs):
        super(PhoronixTestSuiteWringer, self).__init__(*args, **kwargs)
        self.test = kwargs['test']
        self.bench_name = '%s_%s' % (self.bench_name,
                                     self.test.replace('pts/', ''))

    def _parse_phpbench(self):
        data = {}
        for line in self.input_:
            if 'Average:' in line:
                data['average'] = line.split(':')[-1].split()[0].strip()
            if 'Deviation:' in line:
                data['deviation'] = line.split(':')[-1].strip()\
                    .replace('%', '')
        return data

    def _get_data(self):
        func = getattr(self, '_parse_%s' % self.test.replace('pts/', ''))
        data = func()
        return data


class PythonReadWringer(BaseWringer):
    bench_name = 'python_read'

    def __init__(self, *args, **kwargs):
        super(PythonReadWringer, self).__init__(*args, **kwargs)
        self.unit = kwargs.get('unit')
        self.size = kwargs.get('size')
        self.chunk_size = kwargs.get('chunk_size')
        self.iterations = kwargs.get('iterations')
        self.filename = kwargs.get('filename')

    def _get_data(self):
        time_output = self.input_.read()
        return {
            'time': float(time_output),
            'unit': self.unit,
            'size': self.size,
            'chunk_size': self.chunk_size,
            'iterations': self.iterations,
            'filename': self.filename,
        }


class CiTaskWringer(BaseWringer):
    bench_name = 'ci_task'

    def __init__(self, *args, **kwargs):
        super(CiTaskWringer, self).__init__(*args, **kwargs)
        self.service = kwargs.get('service')
        self.task = kwargs.get('task')
        self.concurrency = kwargs.get('concurrency')
        self.max_concurrency = kwargs.get('max_concurrency')

    def _get_data(self):
        durations = {
            'Duration': 'duration',
            'Pending duration': 'pending_duration',
            'Task duration': 'task_duration',
            'Status': 'status',
        }
        data = {
            'service': self.service,
            'task': self.task,
            'concurrency': self.concurrency,
            'max_concurrency': self.max_concurrency,
        }
        for line in self.input_:
            if line.split(':')[0] in durations:
                key, value = line.split(':')
                data[durations[key]] = value.strip()
        return data


class WrkWringer(BaseObjectStorageWringer):
    """
    wrk Wringer
    """
    bench_name = 'wrk'

    def __init__(self, destination_id, destination_type, *args, **kwargs):
        super(WrkWringer, self).__init__(*args, **kwargs)
        self.destination_id = destination_id
        self.destination_type = destination_type

    def _get_data(self):
        input_data = self.input_.read().replace('-nan', '0')
        input_data = json.loads(input_data)
        data = input_data.copy()
        dest_key = 'dest_%s' % self.destination_type
        url = urlparse(data['url'])
        data.update({
            dest_key: self.destination_id,
            'with_ssl': data['url'].startswith('https://'),
            'path': url.path,
            'throughput': data['bytes_per_sec'] / 1024,
            'transfered': data['bytes'] / 1024,
            'requests_within': data.pop('requests_within_stdev'),
            'latency_within': data.pop('latency_within_stdev'),
        })
        return data


class KvazaarWringer(BaseWringer):
    bench_name = 'kvazaar'

    def __init__(self, input_file, threads, preset, real_time,
                 user_time, sys_time, output_size, *args, **kwargs):
        super(KvazaarWringer, self).__init__(*args, **kwargs)
        self.input_file = input_file
        self.threads = threads
        self.preset = preset
        self.real_time = real_time
        self.user_time = user_time
        self.sys_time = sys_time
        self.output_size = output_size

    def _get_data(self):
        data = {
            'input_file': self.input_file,
            'threads': self.threads,
            'preset': self.preset,
            'real_time': self.real_time,
            'user_time': self.user_time,
            'sys_time': self.sys_time,
            'output_size': self.output_size,
        }
        for line in self.input_:
            if 'FPS' not in line:
                continue
            _, value = line.split(':')
            data['fps'] = value.strip()
        return data


class FfmpegWringer(BaseWringer):
    bench_name = 'ffmpeg'

    def __init__(self, output_format, output_scale, unit, preset, input_file, threads, *args, **kwargs):
        super(FfmpegWringer, self).__init__(*args, **kwargs)
        self.output_format = output_format
        self.output_scale = output_scale
        self.unit = unit
        self.input_file = input_file
        self.preset = preset
        self.threads = threads

    def _get_data(self):
        data = {
            'output_format': self.output_format,
            'output_scale': self.output_scale,
            'unit': self.unit,
            'preset': self.preset,
            'input_file': self.input_file,
            'threads': self.threads,
        }
        for line in self.input_:
            if 'ffmpeg version' in line:
                data['version'] = line.split()[2]
            elif 'built with' in line:
                data['compiler'] = ' '.join(line.split()[2:4])
            elif line.startswith('Reading option'):
                if ' output url.' in line:
                    continue
                opt, value = REG_FFMPEG_OPTS.search(line).groups()
                if opt in FFMPEG_OPTS:
                    if opt in FFMPEG_OPTS_TRANS:
                        opt, value = FFMPEG_OPTS_TRANS[opt](opt, value)
                    data[opt] = value
                continue
            elif line.startswith('bench: maxrss'):
                data['maxrss'] = line.split('=')[-1].strip()[:-2]
            elif line.startswith('bench: utime'):
                utime, stime, rtime = REG_FFMPEG_TIME.search(line).groups()
                data.update({
                    'real_time': rtime,
                    'user_time': utime,
                    'sys_time': stime
                })
            elif 'Output stream #0:0 (video)' in line:
                bytes_ = REG_FFMPEG_BYTES.search(line).groups()[0]
                data['output_size'] = bytes_
            elif line.startswith('frame=') and 'Lsize' in line:
                _, fps, q, lsize, bitrate, speed = REG_FFMPEG_STATS.search(line).groups()
                data.update({
                    'frame_rate': fps,
                    'bit_rate': bitrate,
                    'output_quality': q,
                    'speed': speed,
                })
        return data


class PythonFpbWringer(BaseWringer):
    bench_name = 'fpb'

    def __init__(self, *args, **kwargs):
        super(PythonFpbWringer, self).__init__(*args, **kwargs)

    def _get_data(self):
        data = {'threads': 1}
        for line in self.input_:
            if not line.strip():
                continue
            key, value = line.split(':', 1)
            key, value = key.strip(), value.strip()
            data[key] = value
        return data


class TcptracerouteWringer(BaseWringer):
    bench_name = 'tcptraceroute'

    def __init__(self, length, target_type_id, target_type_type, target_instance_id=None, target_instance_type=None,
                 *args, **kwargs):
        super(TcptracerouteWringer, self).__init__(*args, **kwargs)
        self.length = length
        self.target_instance_id = target_instance_id
        self.target_instance_type = target_instance_type
        self.target_type_id = target_type_id
        self.target_type_type = target_type_type

    def _get_data(self):
        data = {
            'length': self.length,
            'target_type_id': self.target_type_id,
            'target_type_type': self.target_type_type,
            'target_instance_id': self.target_instance_id,
            'target_instance_type': self.target_instance_type,
        }
        for line in self.input_:
            if not line.strip():
                continue
            data['time'] = line.strip().split()[-2]
        return data


class RedisBenchmarkWringer(BaseWringer):
    bench_name = 'redis_benchmark'

    def __init__(self, clients, requests, size, keepalive, keyspacelen, numreq,
                 datastore_id, datastore_type_id,
                 *args, **kwargs):
        super(RedisBenchmarkWringer, self).__init__(*args, **kwargs)
        self.clients = clients
        self.requests = requests
        self.size = size
        self.keepalive = keepalive
        self.keyspacelen = keyspacelen
        self.numreq = numreq
        self.datastore_id = datastore_id
        self.datastore_type_id = datastore_type_id

    def run(self):
        raw_results = csv.reader(self.input_)
        data = []
        config_data = {
            'datastore': self.datastore_id,
            'datastore_type': self.datastore_type_id,
            'clients': self.clients,
            'requests': self.requests,
            'size': self.size,
            'keepalive': self.keepalive,
            'keyspacelen': self.keyspacelen,
            'numreq': self.numreq,
        }
        for line in raw_results:
            if not line:
                continue
            try:
                key = line[0].split()[0]
                data.append({
                    'test': key,
                    'rate': line[1],
                })
            except:
                print("Cannot parse %s" % line)
        # Send result
        error = None
        for result in data:
            result.update(config_data)
            # Set mode
            try:
                response = self.client.post_result(
                    bench_name=self.bench_name,
                    data=result,
                    metadata=self._get_metadata())
                if response.status_code >= 300:
                    error = exceptions.ServerError(response.content + str(data))
            except KeyboardInterrupt:
                raise SystemExit(1)
            except Exception as err:
                error = err
        if error is not None:
            raise error

    def _get_data(self, line):
        copies = int(line[1])
        mode = 'rate' if copies > 1 else 'speed'
        data = {
            'test': line[0],
            'mode': mode,
            'copies': copies,
            'base_run_time': line[2],
            'base_rate': line[3],
        }
        return data


STREAM_FUNCTIONS = ('Copy', 'Scale', 'Add', 'Triad')
class StreamWringer(BaseWringer):
    bench_name = 'stream'

    def __init__(self, compiler, *args, **kwargs):
        super(StreamWringer, self).__init__(*args, **kwargs)
        self.compiler = compiler

    def run(self):
        data = []
        for line in self.input_:
            if not line.strip():
                continue
            if line.split(':')[0] in STREAM_FUNCTIONS:
                function, rate, avg, min_, max_ = line.split()
                data.append({
                    'compiler': self.compiler,
                    'function': function.lower()[:-1],
                    'rate': rate,
                    'time_avg': avg,
                    'time_min': min_,
                    'time_max': max_,
                })
        for result in data:
            try:
                response = self.client.post_result(
                    bench_name=self.bench_name,
                    data=result,
                    metadata=self._get_metadata())
                if response.status_code >= 300:
                    error = exceptions.ServerError(response.content + str(data))
            except KeyboardInterrupt:
                raise SystemExit(1)
            except Exception as err:
                error = err


class CpuStealWringer(BaseWringer):
    bench_name = 'cpu_steal'

    def _get_cpu_usage(self, stime, utime, ntime, itime, wtime, Itime, Stime, steal):
        use_time = int(stime) + int(utime) + int(ntime) + int(Itime) + int(Stime)
        wait_time = int(itime) + int(wtime) + int(steal)
        total_time = use_time + wait_time
        percent = use_time / total_time * 100
        return percent

    def _get_cpu_steal_rate(self, stime, utime, ntime, itime, wtime, Itime, Stime, steal):
        total_time = int(stime) + int(utime) + int(ntime) + int(Itime) + int(Stime) + int(itime) + int(wtime) + int(steal)
        steal_time = int(steal)
        percent = steal_time / total_time * 100
        return percent

    def run(self):
        data = []
        if hasattr(self.input_, 'buffer'):
            self.input_ = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8', errors='ignore')
        for raw_line in self.input_:
            raw_line = raw_line.strip()
            if not raw_line.startswith('cpu'):
                continue
            line = raw_line.split()[2:]
            line = line[:17]
            timestamp, _, _, _, total, cpu_index, stime, utime, ntime, itime, wtime, Itime, Stime, steal, guest, _, _ = line
            data.append({
                'cpu_id': cpu_index,
                'value': self._get_cpu_steal_rate(stime, utime, ntime, itime, wtime, Itime, Stime, steal),
            })
        for result in data:
            try:
                response = self.client.post_result(
                    bench_name=self.bench_name,
                    data=result,
                    metadata=self._get_metadata())
                if response.status_code >= 300:
                    error = exceptions.ServerError(response.content + str(data))
            except KeyboardInterrupt:
                raise SystemExit(1)
            except Exception as err:
                error = err
 

class OpensslSpeedWringer(BaseWringer):
    bench_name = 'openssl_speed'

    def __init__(self, block_size, num_thread, mode, use_evp, version=None,
                 *args, **kwargs):
        super(OpensslSpeedWringer, self).__init__(*args, **kwargs)
        self.block_size = block_size
        self.num_thread = num_thread
        self.mode = mode
        self.use_evp = use_evp
        self.version = version

    def _parse_lte_2017(self):
        data = []
        runtime = 3
        algo = None
        for raw_line in self.input_:
            line = raw_line.strip()
            if 'Got: +H' in line and algo is None:
                algo = line.split(':')[3]

            # +F:16:aes-128 cbc:172952373.33:194222549.33:199455744.00:446262613.33:446401194.67
            if not line.startswith('+F:') or 'from ' in line:
                continue

            _, _, _, b16, b64, b256, b1024, b8192 = line.split(':')
            line_values = line.split(':')[3:]
            if len(line_values) == 5:
                b16, b64, b256, b1024, b8192 = line_values
            elif len(line_values) == 6:
                b16, b64, b256, b1024, b8192, b16384 = line_values

            byte_rates = (
                (16, b16),
                (64, b64),
                (256, b256),
                (1024, b1024),
                (8192, b8192),
            )
            
            if len(line_values) == 6:
                byte_rates += ((16394, b16384), )

            for block_size, byte_rate in byte_rates:
                byte_rate = float(byte_rate)
                handled = byte_rate * runtime

                block_rate = byte_rate / block_size
                handled_blocks = handled / block_size
                data.append({
                    'runtime': runtime,
                    'byte_rate': round(byte_rate/1024, 2),
                    'byte_handled': round(handled/1024, 2),
                    'block_rate': block_rate,
                    'block_handled': int(handled_blocks),
                    'version': self.version,
                    'block_size': block_size,
                    'num_thread': self.num_thread,
                    'use_evp': self.use_evp,
                    'mode': self.mode,
                    'algo': algo,
                })
        return data

    def _parse_gt_2017(self):
        data = []
        for raw_line in self.input_:
            line = raw_line.strip()
            # +F:3:md5:313616442.66
            if not line.startswith('+F:') or 'from ' in line:
                continue

            _, runtime, algo, rate = line.split(':')
            byte_rate = float(rate)
            runtime = float(runtime)
            handled = byte_rate * runtime

            block_rate = byte_rate / self.block_size
            handled_blocks = handled / self.block_size
            data.append({
                'runtime': runtime,
                'byte_rate': round(byte_rate/1024, 2),
                'byte_handled': round(handled/1024, 2),
                'block_rate': block_rate,
                'block_handled': int(handled_blocks),
                'version': self.version,
                'block_size': self.block_size,
                'num_thread': self.num_thread,
                'use_evp': self.use_evp,
                'mode': self.mode,
                'algo': algo,
            })
        return data

    def run(self):
        version_year = self.version.split()[-1]
        version_year = int(version_year) if version_year.isdigit() else 0
        if version_year < 2017:
            data = self._parse_lte_2017()
        else:
            data = self._parse_gt_2017()

        for result in data:
            try:
                response = self.client.post_result(
                    bench_name=self.bench_name,
                    data=result,
                    metadata=self._get_metadata()
                )
                if response.status_code >= 300:
                    error = exceptions.ServerError(response.content + str(data))
            except KeyboardInterrupt:
                raise SystemExit(1)
 

class BaseOsBenchmarkWringer(BaseWringer):
    def __init__(self, dest_zone, object_storage, storage_class,
                 *args, **kwargs):
        super(BaseOsBenchmarkWringer, self).__init__(*args, **kwargs)
        self.storage_class = storage_class
        self.dest_zone = dest_zone
        self.object_storage = object_storage

    def _get_data(self):
        # Parse input file
        bench_data = {}
        for line in self.input_:
            key, value = line.split()
            bench_data[key.strip()] = value.strip()
        # Return a dict with API needed infos
        bench_data.update({
            'dest_zone': self.dest_zone,
            'object_storage': self.object_storage,
            'storage_class': self.storage_class,
        })
        return bench_data
 

class OsBenchmarkDownloadWringer(BaseOsBenchmarkWringer):
    bench_name = 'os_benchmark_download'


class OsBenchmarkUploadWringer(BaseOsBenchmarkWringer):
    bench_name = 'os_benchmark_upload'
 

class OsBenchmarkMultiDownloadWringer(BaseOsBenchmarkWringer):
    bench_name = 'os_benchmark_multi_download'
 

class OsBenchmarkVideoStreamingWringer(BaseOsBenchmarkWringer):
    bench_name = 'os_benchmark_video_streaming'
 

class OsBenchmarkAbWringer(BaseOsBenchmarkWringer):
    bench_name = 'os_benchmark_ab'
 

class OsBenchmarkCurlWringer(BaseOsBenchmarkWringer):
    bench_name = 'os_benchmark_curl'


class AiBenchmarkWringer(BaseWringer):
    bench_name = 'ai_benchmark'

    def run(self):
        results = []
        raw_data = json.loads(self.input_.read())
        base_data = {
            'cpu_cores': raw_data['test_info']['cpu_cores'],
            'intra_thread': raw_data['test_info']['intra_threads'],
            'inter_thread': raw_data['test_info']['inter_threads'],
            'tf_version': raw_data['test_info']['tf_version'],
            'py_version': raw_data['test_info']['py_version'].split()[0],
            'version': raw_data['test_info']['version'],
            'precision': raw_data['test_info']['precision'],
            'unit': 'cpu' if raw_data['test_info']['is_cpu_build'] else 'gpu',
        }
        for test in raw_data['test_results']:
            data = base_data.copy()
            if str(raw_data['test_results'][test]['mean']) == 'nan':
                continue
            data['test'] = test
            data.update(raw_data['test_results'][test])
            results.append(data)
        if raw_data['test_info']['full_suite']:
            for test in ('ai_score', 'inference_score', 'training_score'):
                mean = raw_data[test]
                data = base_data.copy()
                data.update(test=test, mean=mean, std=0)
                results.append(data)

        for result in results:
            try:
                response = self.client.post_result(
                    bench_name=self.bench_name,
                    data=result,
                    metadata=self._get_metadata()
                )
                if response.status_code >= 300:
                    error = exceptions.ServerError(response.content + str(data))
            except KeyboardInterrupt:
                raise SystemExit(1)


class MhzWringer(BaseWringer):
    bench_name = 'mhz'

    def run(self):
        results = []
        for result in self.input_.readlines():
            data = {'rate': float(result)}
            try:
                response = self.client.post_result(
                    bench_name=self.bench_name,
                    data=data,
                    metadata=self._get_metadata()
                )
                if response.status_code >= 300:
                    error = exceptions.ServerError(response.content + str(data))
            except KeyboardInterrupt:
                raise SystemExit(1)


class TlbWringer(BaseWringer):
    bench_name = 'tlb'

    def run(self):
        results = []
        for result in self.input_.readlines():
            split_res = result.strip().split()
            data = {
                'pages': int(split_res[1]),
                'latency': float(split_res[3]),
            }
            try:
                response = self.client.post_result(
                    bench_name=self.bench_name,
                    data=data,
                    metadata=self._get_metadata()
                )
                if response.status_code >= 300:
                    error = exceptions.ServerError(response.content + str(data))
            except KeyboardInterrupt:
                raise SystemExit(1)


class BwMemWringer(BaseWringer):
    bench_name = 'bw_mem'

    def __init__(self, parallelism, warmup, repetitions, operation, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.parallelism = parallelism
        self.warmup = warmup
        self.repetitions = repetitions
        self.operation = operation

    def run(self):
        results = []
        for result in self.input_.readlines():
            size, lat = result.strip().split()
            data = {
                'size': int(float(size)),
                'latency': float(lat),
                'parallelism': self.parallelism,
                'warmup': self.warmup,
                'repetitions': self.repetitions,
                'operation': self.operation,
            }
            try:
                response = self.client.post_result(
                    bench_name=self.bench_name,
                    data=data,
                    metadata=self._get_metadata()
                )
                if response.status_code >= 300:
                    error = exceptions.ServerError(response.content + str(data))
            except KeyboardInterrupt:
                raise SystemExit(1)


class SudokuMlBenchWringer(BaseWringer):
    bench_name = 'sudoku_ml_bench'

    def __init__(self, unit, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.unit = unit

    def _get_data(self):
        def parse_range(name):
            r = data.get(name)
            if not r:
                return {}
            if isinstance(r, str):
                r = [int(i) for i in r.split(',')]
            if isinstance(r, int):
                r = [r-1, r]
            return {'%s_min' % name: r[0], '%s_max' % name: r[1]}
        data = json.loads(self.input_.read())
        if not data.get('precision_policy'):
            data['precision_policy'] = 'default'
        if math.isnan(data.get('train_start_loss') or 0):
            data['train_start_loss'] = None
        if math.isnan(data.get('train_end_loss') or 0):
            data['train_end_loss'] = None
        data.update(parse_range('train_removed'))
        data.update(parse_range('infer_removed'))
        data['unit'] = self.unit
        data['model'] = data['model_path']
        return data


class CoreMarkWringer(BaseWringer):
    bench_name = 'coremark'

    def _get_data(self):
        # Parse input file
        WANTEDS = {
            'CoreMark Size': 'size',
            'Total ticks': 'total_ticks',
            'Total time (secs)': 'total_time',
            'Iterations': 'iterations',
            'Iterations/Sec': 'rate',
            'Compiler version': 'compiler_version',
            'Compiler flags': 'compiler_flag',
        }
        bench_data = {}
        for line in self.input_:
            if ':' not in line:
                continue
            key, value = line.split(':', 1)
            key = key.strip()
            value = value.strip()
            if 'Parallel' in key:
                mode = key.split()[1]
                bench_data.update({
                    'concurrency': value,
                    'concurrency_mode': mode,
                })
            elif key in WANTEDS:
                bench_data[WANTEDS[key]] = value

        if 'concurrency_mode' not in bench_data:
            concurrency = bench_data['compiler_flag'].split('-DMULTITHREAD=')[-1].split()[0]
            bench_data.update({
                'concurrency_mode': 'fork',
                'concurrency': concurrency,
            })
        return bench_data


class LmSensorsWringer(BaseWringer):
    bench_name = 'lm_sensors'

    def __init__(self, cpu_usage=None, *args, **kwargs):
        self.cpu_usage = cpu_usage
        super(LmSensorsWringer, self).__init__(*args, **kwargs)

    def _get_data(self):
        lm_data = json.load(self.input_)
        data = {
            'cpu_usage': self.cpu_usage,
            'raw': lm_data,
        }

        pm_keys = [k for k in lm_data.keys() if k.startswith('power_meter-acpi')]
        watt_values = [lm_data[k]['power1']['power1_average'] for k in pm_keys]
        data['watt_count'] = len(watt_values)
        data['watt_sum'] = sum(watt_values)
        data['watt'] = data['watt_sum'] / data['watt_count']
        data['watt_std'] = utils.stddev(watt_values)

        cpu_temp_keys = [k for k in lm_data.keys() if k.startswith('coretemp-isa-00')]
        cpu_temp_values = [
            [v for Ck, v in lm_data[k][ck].items() if Ck.endswith('_input')][0]
            for k in cpu_temp_keys
            for ck in lm_data[k]
            if isinstance(lm_data[k][ck], dict)
        ]
        data['cpu_temp_count'] = len(cpu_temp_values)
        data['cpu_temp_avg'] = sum(cpu_temp_values) / data['cpu_temp_count']
        data['cpu_temp_std'] = utils.stddev(cpu_temp_values)

        return data


class IpmiSensorsWringer(BaseWringer):
    bench_name = 'ipmi_sensors'

    FIELDS = {
    }

    def __init__(self, user_cpu_usage=None, *args, **kwargs):
        self.user_cpu_usage = user_cpu_usage
        super(IpmiSensorsWringer, self).__init__(*args, **kwargs)

    def _get_data(self):
        lines = self.input_.readlines()
        ipmi_data = [
            [v.strip() for v in l.split('|')]
            for l in lines
        ]
        ipmi_data = {
            l[0].replace(' ', '_').lower(): l[1]
            for l in ipmi_data
            if l[1] not in ('na', '0x0')
        }

        ipmi_data.update({
            'user_cpu_usage': self.user_cpu_usage,
            'raw': '\n'.join(lines),
        })
        if 'cpu_temp' in ipmi_data:
            ipmi_data['cpu_temp_avg'] = ipmi_data['cpu_temp']
            ipmi_data['cpu_temp_std'] = 0.0
            
        if 'cpu1_mem_temp' in ipmi_data:
            cpu_temps = [
                float(v) for k, v in ipmi_data.items()
                if k.startswith('cpu') and k.endswith('_mem_temp')
            ]
            ipmi_data['cpu_temp_avg'] = (sum(cpu_temps) / len(cpu_temps))
            ipmi_data['cpu_temp_std'] = utils.stddev(cpu_temps)
            

        if 'temp' in ipmi_data:
            ipmi_data['cpu_temp_avg'] = ipmi_data['temp']
            ipmi_data['cpu_temp_std'] = 0.0

        if 'fan1_speed' in ipmi_data:
            fan_rpms = [
                float(v) for k, v in ipmi_data.items()
                if k.startswith('fan') and k.endswith('_speed')
            ]
            ipmi_data['fan_number'] = len(fan_rpms)
            ipmi_data['fan_rpm_avg'] = (sum(fan_rpms) / ipmi_data['fan_number'])
            ipmi_data['fan_rpm_std'] = utils.stddev(fan_rpms)

        if 'fan1' in ipmi_data:
            fan_rpms = [
                float(v) for k, v in ipmi_data.items()
                if k.startswith('fan')
            ]
            ipmi_data['fan_number'] = len(fan_rpms)
            ipmi_data['fan_rpm_avg'] = (sum(fan_rpms) / ipmi_data['fan_number'])
            ipmi_data['fan_rpm_std'] = utils.stddev(fan_rpms)

        if 'power' in ipmi_data:
            ipmi_data['power_consumption'] = ipmi_data['power']

        if 'pwr_consumption' in ipmi_data:
            ipmi_data['power_consumption'] = ipmi_data['pwr_consumption']
            
        if 'power_consumption' not in ipmi_data:
            ipmi_data['power_consumption'] = -1

        return ipmi_data


class GoToWafWringer(BaseNetworkWringer):
    bench_name = 'gotowaf'

    def run(self):
        error = None
        config_data = {
            'destination_id': self.destination_id,
            'destination_type': self.destination_type,
        }
        for line in self.input_:
            if 'Summary' in line:
                break
            if not line.strip():
                continue

            result = self._get_data(line)
            if not result:
                continue

            result.update(config_data)
            # Set mode
            try:
                response = self.client.post_result(
                    bench_name=self.bench_name,
                    data=result,
                    metadata=self._get_metadata()
                )
                if response.status_code >= 300:
                    error = exceptions.ServerError(response.content + str(result))
            except KeyboardInterrupt:
                raise SystemExit(1)
            except Exception as err:
                error = err
        if error is not None:
            raise error

    def _get_data(self, line):
        data = {}
        if line.startswith('| '):
            if 'TEST SET' in line or 'TYPE' in line or 'DATE' in line or 'GENERIC' in line:
                return
            ts, tc, perc, bloc, bypa, unre, sent, fail = [
                i.strip() for i in line.split('|')
            ][1:-1]
            data = {
                'test_set': ts,
                'test_case': tc,
                'percentage': perc,
                'blocked': bloc,
                'bypassed': bypa,
                'unresolved': unre,
                'sent': sent,
                'failed': fail,
            }
        return data


class PythonBenchmarkWringer(BaseWringer):
    bench_name = 'python_benchmark'

    def __init__(self, python_version_minor=None, *args, **kwargs):
        super(PythonBenchmarkWringer, self).__init__(*args, **kwargs)
        self.python_version_minor = python_version_minor

    def run(self):
        error = None
        result = json.load(self.input_)
        for bench in result['benchmarks']:
            if bench['metadata']['name'] in ('2to3',):
                continue
            values = [
                v
                for r in bench['runs']
                if 'values' in r
                for v in r['values']
            ]
            warmups = [
                v[1]
                for r in bench['runs']
                if 'values' in r
                for v in r['warmups']
            ]
            durations = [
                r['metadata']['duration']
                for r in bench['runs']
                if 'values' in r
            ]
            load_avgs = [
                r['metadata']['load_avg_1min']
                for r in bench['runs']
                if 'values' in r
                if 'load_avg_1min' in r['metadata']
            ]
            if load_avgs:
                load_avg_1min_avg = sum(load_avgs) / len(load_avgs)
                load_avg_1min_std = utils.stddev(load_avgs)
            else:
                load_avg_1min_avg = load_avg_1min_std = None
            python_version = bench['metadata'].get('python_version')
            if self.python_version_minor:
                python_version_minor = self.python_version_minor
            elif python_version:
                python_version_minor = re.sub('(3\.\d*).*', r'\1', python_version)
            else:
                python_version_minor = None
            data = {
                # System
                'aslr': result['metadata']['aslr'],
                'cpu_config': result['metadata']['cpu_config'],
                'cpu_count': result['metadata']['cpu_count'],
                'cpu_freq': result['metadata']['cpu_freq'],
                'cpu_model_name': result['metadata']['cpu_model_name'],
                'perf_version': result['metadata']['perf_version'],
                'performance_version': result['metadata']['performance_version'],
                'runnable_threads': result['metadata'].get('runnable_threads'),
                # Bench
                'python_cflags': bench['metadata'].get('python_cflags'),
                'python_compiler': bench['metadata'].get('python_compiler'),
                'python_implementation': bench['metadata'].get('python_implementation'),
                'python_version': python_version,
                'python_version_minor': python_version_minor,
                'name': bench['metadata']['name'],
                # Values
                'duration_avg': sum(durations) / len(durations),
                'duration_std': utils.stddev(durations),
                'load_avg_1min_avg': load_avg_1min_avg,
                'load_avg_1min_std': load_avg_1min_std,
                'value_avg': sum(values) / len(values),
                'value_std': utils.stddev(values),
                'warmup_avg': sum(warmups) / len(warmups),
                'warmup_std': utils.stddev(warmups),
            }

            try:
                response = self.client.post_result(
                    bench_name=self.bench_name,
                    data=data,
                    metadata=self._get_metadata()
                )
                if response.status_code >= 300:
                    error = exceptions.ServerError(response.content + str(result))
            except KeyboardInterrupt:
                raise SystemExit(1)
            except Exception as err:
                error = err
        if error is not None:
            raise error


class BlenderBenchmarkWringer(BaseWringer):
    bench_name = 'blender_benchmark'

    def __init__(self, *args, **kwargs):
        super(BlenderBenchmarkWringer, self).__init__(*args, **kwargs)

    def run(self):
        error = None
        result = json.load(self.input_)
        for bench in result:
            data = {
                'version': bench['benchmark_launcher']['label'],
                'blender_version': bench['blender_version']['version'],
                'scene': bench['scene']['label'],
                'bitness': bench['system_info']['bitness'],
                'machine': bench['system_info']['machine'],
                'system': bench['system_info']['system'],
                'num_cpu_sockets': bench['system_info']['num_cpu_sockets'],
                'num_cpu_cores': bench['system_info']['num_cpu_cores'],
                'num_cpu_threads': bench['system_info']['num_cpu_threads'],
                'device_type': bench['device_info']['device_type'],
                'device_peak_memory': bench['stats']['device_peak_memory'],
                'number_of_samples': bench['stats']['number_of_samples'],
                'time_for_samples': bench['stats']['time_for_samples'],
                'samples_per_minute': bench['stats']['samples_per_minute'],
                'total_render_time': bench['stats']['total_render_time'],
                'render_time_no_sync': bench['stats']['render_time_no_sync'],
                'time_limit': bench['stats']['time_limit'],
            }

            try:
                response = self.client.post_result(
                    bench_name=self.bench_name,
                    data=data,
                    metadata=self._get_metadata()
                )
                if response.status_code >= 300:
                    error = exceptions.ServerError(response.content + str(result))
            except KeyboardInterrupt:
                raise SystemExit(1)
            except Exception as err:
                error = err
        if error is not None:
            raise error


class DeepsparseBenchmarkWringer(BaseWringer):
    bench_name = 'deepsparse_benchmark'

    def __init__(self, python_version=None, scenario=None, warmup_time=None, thread_pinning=None, engine=None, *args, **kwargs):
        self.python_version = python_version
        self.scenario = scenario
        self.warmup_time = warmup_time
        self.thread_pinning = thread_pinning
        self.engine = engine
        super(DeepsparseBenchmarkWringer, self).__init__(*args, **kwargs)

    def run(self):
        error = None
        result = json.load(self.input_)
        data = {
            'version': result['version'],
            'python_version': self.python_version,
            'model_path': result['orig_model_path'],
            'batch_size': result['batch_size'],
            'input_shapes': result['input_shapes'],
            'num_cores': result['num_cores'],
            'scenario': self.scenario or result['scenario'],
            'scheduler': result['scheduler'],
            'time': result['seconds_to_run'],
            'warmup_time': self.warmup_time,
            'num_streams': result['num_streams'],
            'thread_pinning': self.thread_pinning,
            'engine': self.engine,
            'items_per_sec': result['benchmark_result']['items_per_sec'],
            'seconds_ran': result['benchmark_result']['seconds_ran'],
            'iterations': result['benchmark_result']['iterations'],
            'median': result['benchmark_result']['median'],
            'mean': result['benchmark_result']['mean'],
            'std': result['benchmark_result']['std'],
            'perc25_0': result['benchmark_result']['25.0%'],
            'perc75_0': result['benchmark_result']['75.0%'],
            'perc90_0': result['benchmark_result']['90.0%'],
            'perc95_0': result['benchmark_result']['95.0%'],
            'perc99_0': result['benchmark_result']['99.0%'],
            'perc99_9': result['benchmark_result']['99.9%'],
        }

        try:
            response = self.client.post_result(
                bench_name=self.bench_name,
                data=data,
                metadata=self._get_metadata()
            )
            if response.status_code >= 300:
                error = exceptions.ServerError(response.content + str(result))
        except KeyboardInterrupt:
            raise SystemExit(1)
        except Exception as err:
            error = err
            print(err)


class OllamaWringer(BaseWringer):
    bench_name = 'ollama'

    def __init__(self, query, model, unit, version=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.query = query
        self.model = model
        self.unit = unit
        self.version = version

    def _get_data(self):
        re_key = re.compile("h?([a-z_]+)$")

        data = {
            "query": self.query,
            "model": self.model,
            "version": self.version,
            'unit': self.unit,
        }
        for line in self.input_:
            if not line or ':' not in line:
                continue
            key, value = line.split(':')
            key = re_key.search(key.strip().lower().replace(' ', '_')).groups()[-1]
            value = RE_FLOAT.search(value).group(0)
            data[key] = value
        return data


class WhisperBenchmarkWringer(BaseWringer):
    bench_name = 'whisper_benchmark'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _get_data(self):
        data = {}
        for line in self.input_:
            if ':' not in line:
                continue
            key, value = [i.strip() for i in line.split(':')]
            data[key] = value
        return data


class InvokeAiBenchmarkWringer(BaseWringer):
    bench_name = 'invokeai_benchmark'

    def __init__(self, unit, *args, **kwargs):
        self.unit = unit
        super().__init__(*args, **kwargs)

    def _get_data(self):
        data = {}
        for line in self.input_:
            if ':' not in line:
                continue
            key, value = [i.strip() for i in line.split(':', 1)]
            data[key] = value
        data['unit'] = self.unit
        return data


class NvbandwidthWringer(BaseWringer):
    bench_name = 'nvbandwidth'

    def __init__(self, buffer, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.buffer = buffer

    def _get_data(self):
        data = {
            'buffer': self.buffer,
        }
        for line in self.input_:
            if not line.strip():
                continue
            if line.split()[0] in ('SUM', 'MIN', 'MAX', 'AVG'):
                key, tc, value = line.lower().split()
                data.update({
                    key: value,
                    'testcase': tc,
                })
                continue

            if ':' not in line:
                continue
            key, value = [i.strip() for i in line.split(':', 1)]
            key = key.replace(' ', '_').lower()
            data[key] = value

        data.update({
            'version': data['nvbandwidth_version'],
            'test_samples': len([k for k in data if k.startswith('sample_')]),
        })
        return data


class GpuBurnWringer(BaseWringer):
    bench_name = 'gpu_burn'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _get_data(self):
        data = {
            'stats': []
        }
        conf_re = re.compile(r"Initialized device (\d+) with (\d+) MB of memory \((\d+) MB available, using (\d+) MB of it\), using (\w+)(, using Tensor Cores)?")
        val_re = re.compile(r"(\d+\.\d+)%.*?proc'd: (\d+) \((\d+) Gflop/s\).*?errors: (\d+).*?temps: (\d+) C", re.DOTALL)
        for line in self.input_:
            if not line.strip():
                continue

            if line.startswith('Burning for'):
                data['time'] = int(line.split()[2])
                continue

            conf_match = conf_re.match(line)
            if conf_match:
                _, init_mem, avai_mem, used_mem, dtype, tc = conf_match.groups()
                init_mem, avai_mem, used_mem = int(init_mem), int(avai_mem), int(used_mem)
                perc = int(round(used_mem / avai_mem * 100, 0))
                data.update({
                    'init_mem': init_mem,
                    'avai_mem': avai_mem,
                    'used_mem': used_mem,
                    'perc_mem': perc,
                    'dtype': dtype,
                    'tensor_core': bool(tc),
                })
                continue

            val_match = val_re.search(line)
            if val_match:
                _, _, gflops, errors, temp = val_match.groups()
                data['stats'].append((int(gflops), int(errors), int(temp)))

        value_names = ('gflop', 'errors', 'temp')
        for i, value_name in enumerate(value_names):
            values = [j[i] for j in data['stats']]
            avg = sum(values) / len(values)
            data.update({
                f"{value_name}_max": max(values),
                f"{value_name}_min": min(values),
                f"{value_name}_avg": avg,
                f"{value_name}_stddev": utils.stddev(values),
            })

        return data


WRINGERS = {
    'sysbench_cpu': SysbenchCpuWringer,
    'sysbench_ram': SysbenchRamWringer,
    'ab': AbWringer,
    'wrk': WrkWringer,
    'fio': FioWringer,
    'sysbench_oltp': SysbenchOltpWringer,
    'vdbench': VdbenchWringer,
    'mdtest': MdtestWringer,
    'bonnie': BonnieWringer,
    'wordcount': WordcountWringer,
    'terasort': TeraSortWringer,
    'dfsio': DfsioWringer,
    'tracepath': TracepathWringer,
    'iperf': IperfWringer,
    'geekbench6': Geekbench6Wringer,
    'geekbench5': Geekbench5Wringer,
    'geekbench4': Geekbench4Wringer,
    'geekbench3': Geekbench3Wringer,
    'spec_cpu2006': SpecCpu2006Wringer,
    'spec_cpu2017': SpecCpu2017Wringer,
    'financebench': FinanceBenchWringer,
    'lammps': LammpsWringer,
    'vray': VRayWringer,
    'vasptest': VaspTestWringer,
    'phoronix_test_suite': PhoronixTestSuiteWringer,
    'python_read': PythonReadWringer,
    'ci_task': CiTaskWringer,
    'kvazaar': KvazaarWringer,
    'ffmpeg': FfmpegWringer,
    'fpb': PythonFpbWringer,
    'metric': MetricWringer,
    'prometheus': PrometheusMetricWringer,
    'redis-benchmark': RedisBenchmarkWringer,
    'tcptraceroute': TcptracerouteWringer,
    'stream': StreamWringer,
    'cpu_steal': CpuStealWringer,
    'openssl_speed': OpensslSpeedWringer,
    'os_benchmark_download': OsBenchmarkDownloadWringer,
    'os_benchmark_upload': OsBenchmarkUploadWringer,
    'os_benchmark_multi_download': OsBenchmarkMultiDownloadWringer,
    'os_benchmark_video_streaming': OsBenchmarkVideoStreamingWringer,
    'os_benchmark_ab': OsBenchmarkAbWringer,
    'os_benchmark_curl': OsBenchmarkCurlWringer,
    'ai_benchmark': AiBenchmarkWringer,
    'sudoku_ml_bench': SudokuMlBenchWringer,
    'mhz': MhzWringer,
    'tlb': TlbWringer,
    'bw_mem': BwMemWringer,
    'pgbench': PgbenchWringer,
    'ycsb': YcsbWringer,
    'coremark': CoreMarkWringer,
    'lm_sensors': LmSensorsWringer,
    'ipmi_sensors': IpmiSensorsWringer,
    'gotowaf': GoToWafWringer,
    'python_benchmark': PythonBenchmarkWringer,
    'blender_benchmark': BlenderBenchmarkWringer,
    'deepsparse_benchmark': DeepsparseBenchmarkWringer,
    'ollama': OllamaWringer,
    'whisper_benchmark': WhisperBenchmarkWringer,
    'invokeai_benchmark': InvokeAiBenchmarkWringer,
    'nvbandwidth': NvbandwidthWringer,
    'gpu_burn': GpuBurnWringer,
}


def get_wringer(name):
    if name in WRINGERS:
        return WRINGERS[name]
    msg = "Wringer '%s' not found in %s" % (name, list(WRINGERS))
    raise exceptions.WringerNotFound(msg)
