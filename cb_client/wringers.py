import os
import sys
import re
import json
from datetime import datetime

import six

from . import exceptions
from . import client
from .settings import get_config

TIME_SCALE = {
  's': 1,
  'ms': 10**-3,
  'us': 10**-6,
  'ns': 10**-9
}

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


class BaseWringer(object):
    def __init__(self, input_=None, master_url=None, token=None,
                 flavor_id=None, image_id=None, date=None, **kwargs):
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
        self.flavor_id = flavor_id or self.config.get('flavor')
        self.image_id = image_id or self.config.get('image')
        self.provider_id = self.config.get('provider')
        self.datacenter_id = self.config.get('datacenter')
        self.instance_type_id = self.config.get('type')
        self.test_date = date or datetime.now().isoformat()

        self.client = client.Client(self.master_url, self.token)

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
        return {
            'test_date': self.test_date,
            'flavor': self.flavor_id,
            'image': self.image_id,
            'provider': self.provider_id,
            'datacenter': self.datacenter_id,
            'instance_type': self.instance_type_id,
        }

    def run(self):
        """
        Public runner to parse and publish result.
        """
        try:
            response = self.client.post_result(
                bench_name=self.bench_name,
                data=self._get_data(),
                metadata=self._get_metadata())
            # self.logger.info('Response: %s' % response.content)
            if response.status_code >= 300:
                raise exceptions.ServerError(response.content + str(self._get_data()))
            return response
        except KeyboardInterrupt:
            raise SystemExit(1)


class SysbenchCpuWringer(BaseWringer):
    bench_name = 'sysbench_cpu'

    def _get_data(self):
        # Parse input file
        WANTEDS = ('of threads', 'events per second', 'total time', 'total number of events', 'min', 'avg', 'max', 'numbers limit')
        bench_data = {}
        bench_data['version'] = self.input_.readline().split()[1]
        for line in self.input_:
            for _line in [line.strip() for w in WANTEDS if w in line]:
                key, value = _line.split(':')
                bench_data[key.strip()] = value.strip()
        # Return a dict with API needed infos
        return {
            'exec_time': int(float(re.sub(r'[^0-9\.]', '', bench_data['total time']))),
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
        WANTEDS = ('threads:', 'total time', 'total number of events', 'min', 'avg', 'max', 'operation:', 'block size')
        bench_data = {}
        for line in self.input_:
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
            'exec_time': int(float(re.sub(r'[^0-9\.]', '', bench_data['total time']))),
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
    Apache benchmark Wringer
    """
    bench_name = 'ab'

    def _get_data(self):
        bench_data = {
            'num_error_response': 0,
        }
        for line in self.input_:
            if not line.strip():
                continue
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

        fio_result['num_thread'] = self.num_thread or len(fio_data['jobs'])
        fio_result['mixed_ratio'] = self.mixed_ratio
        fio_result['volume_flavor'] = self.volume_flavor_id
        fio_result['volume_manager'] = self.volume_manager_id
        fio_result['network_storage'] = self.network_storage_id
	
        fio_result['block_size'] = fio_data['global options']['bs'] if 'bs' in fio_data['global options'] else fio_data['jobs'][0]['job options']['bs']
        fio_result['readwrite'] = self._get_readwrite(fio_data['global options']['rw'])
        fio_result['mode'] = 'rand' if 'rand' in fio_data['global options']['rw'] else 'seq'
        fio_result['runtime'] = int(fio_data['global options']['runtime'] if 'runtime' in fio_data['global options'] else fio_data['jobs'][0]['job options']['runtime'])/1000
        fio_result['io_depth'] = fio_data['global options']['iodepth']

        fio_result['read_io'] = int(fio_data['jobs'][0]['read']['io_bytes'])/1048576
        fio_result['read_iops'] = int(fio_data['jobs'][0]['read']['iops'])
        fio_result['read_bandwidth'] = fio_data['jobs'][0]['read']['bw']
        fio_result['read_bandwidth_max'] = fio_data['jobs'][0]['read']['bw_max']
        fio_result['read_latency'] = float(fio_data['jobs'][0]['read']['clat']['mean'])
        fio_result['read_latency_stdev'] = float(fio_data['jobs'][0]['read']['clat']['stddev'])

        fio_result['write_io'] = int(fio_data['jobs'][0]['write']['io_bytes'])/1048576
        fio_result['write_iops'] = int(fio_data['jobs'][0]['write']['iops'])
        fio_result['write_bandwidth'] = fio_data['jobs'][0]['write']['bw']
        fio_result['write_bandwidth_max'] = fio_data['jobs'][0]['write']['bw_max']
        fio_result['write_latency'] = float(fio_data['jobs'][0]['write']['clat']['mean'])
        fio_result['write_latency_stdev'] = float(fio_data['jobs'][0]['write']['clat']['stddev'])

        return fio_result


class SysbenchMysqlWringer(BaseWringer):
    bench_name = 'sysbench_mysql'

    def __init__(self, *args, **kwargs):
        super(SysbenchMysqlWringer, self).__init__(*args, **kwargs)
        self.table_size = kwargs['table_size']
        self.oltp_test_mode = kwargs['oltp_test_mode']
        self.volume_flavor_id = kwargs['volume_flavor_id']
        self.db_ps_mode = kwargs['db_ps_mode']
        self.oltp_distinct_ranges = kwargs['oltp_distinct_ranges']
        self.oltp_user_delay_max = kwargs['oltp_user_delay_max']
        self.oltp_index_updates = kwargs['oltp_index_updates']
        self.oltp_sum_ranges = kwargs['oltp_sum_ranges']
        self.read_only = kwargs['read_only']
        self.oltp_non_index_updates = kwargs['oltp_non_index_updates']
        self.oltp_range_size = kwargs['oltp_range_size']
        self.oltp_connect_delay = kwargs['oltp_connect_delay']
        self.oltp_nontrx_mode = kwargs['oltp_nontrx_mode']
        self.oltp_point_selects = kwargs['oltp_point_selects']
        self.oltp_simple_ranges = kwargs['oltp_simple_ranges']
        self.oltp_order_ranges = kwargs['oltp_order_ranges']

    def _get_data(self):
        sysbench_data = {}

        sysbench_data['table_size'] = self.table_size
        sysbench_data['mode'] = self.oltp_test_mode
        sysbench_data['volume'] = self.volume_flavor_id
        sysbench_data['nontrx_mode'] = self.oltp_nontrx_mode
        sysbench_data['read_only'] = self.read_only
        sysbench_data['range_size'] = self.oltp_range_size
        sysbench_data['point_select'] = self.oltp_point_selects
        sysbench_data['simple_ranges'] = self.oltp_simple_ranges
        sysbench_data['sum_ranges'] = self.oltp_sum_ranges
        sysbench_data['order_ranges'] = self.oltp_order_ranges
        sysbench_data['distinct_ranges'] = self.oltp_distinct_ranges
        sysbench_data['index_ranges'] = self.oltp_index_updates
        sysbench_data['non_index_ranges'] = self.oltp_non_index_updates
        sysbench_data['connect_delay'] = self.oltp_connect_delay
        sysbench_data['user_delay'] = self.oltp_user_delay_max
        sysbench_data['ps_mode'] = self.db_ps_mode

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
            elif 'avg:' in line:
                sysbench_data['avg_response_time'] = re.search('(\d+\.\d*)', line).group()          #time in ms
        
        sysbench_data['rw_speed'] = (float(sysbench_data['read_queries']) + float(sysbench_data['write_queries']))/float(sysbench_data['run_time'])

        return sysbench_data

    def _get_metadata(self):
        metadata = super(SysbenchMysqlWringer, self)._get_metadata()
        metadata['volume_flavor_id'] = self.volume_flavor_id
        return metadata


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
        self.vdbench_config = kwargs['vdbench_config']

    def _get_data(self, **kwargs):
        is_rd = False
        for line in self.input_.readlines():
            if 'Starting RD' in line:
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
        vdbench_result.update(self.vdbench_config)
        vdbench_result['operations'] = ','.join([fwd['operation'] for fwd in self.vdbench_config['fwds']])
        if vdbench_result.get('rdpct') is not None:
            if int(vdbench_result['rdpct']) == 0:
                vdbench_result['operations'] == 'write'
            elif int(vdbench_result['rdpct']) == 100:
                vdbench_result['operations'] == 'read'
            else:
                vdbench_result['operations'] == 'read,write'
        vdbench_result['width'] = self.vdbench_config['fsd']['width']
        vdbench_result['depth'] = self.vdbench_config['fsd']['depth']
        vdbench_result['files'] = self.vdbench_config['fsd']['files']
        vdbench_result['threads'] = sum([int(fwd['threads']) for fwd in self.vdbench_config['fwds']])
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
  ('map_time', 'time spent by all map tasks (ms)', 1),
  ('map_time_in_slot', 'time spent by all maps in occupied slots', 1),
  ('map_vcore_time', 'vcore-milliseconds taken by all map', 1),
  ('map_mb_ms', 'megabyte-milliseconds taken by all map', 1024),
  ('reduce_time', 'time spent by all reduce tasks (ms)', 1),
  ('reduce_time_in_slot', 'time spent by all reduces in occupied slots', 1),
  ('reduce_vcore_time', 'vcore-milliseconds taken by all reduce tasks', 1),
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

    def _get_data(self):
        bench_data = {
          'framework': self.framework,
          'size': self.size,
          'arch': self.architecture,
        }
        report_file = open(self.report_filename)
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

WRINGERS = {
    'sysbench_cpu': SysbenchCpuWringer,
    'sysbench_ram': SysbenchRamWringer,
    'ab': AbWringer,
    'fio': FioWringer,
    'sysbench_mysql': SysbenchMysqlWringer,
    'vdbench': VdbenchWringer,
    'mdtest': MdtestWringer,
    'bonnie': BonnieWringer,
    'wordcount': WordcountWringer,
    'terasort': TeraSortWringer,
}

def get_wringer(name):
    if name in WRINGERS:
        return WRINGERS[name]
    msg = "Wringer '%s' not found in %s" % (name, list(WRINGERS))
    raise exceptions.WringerNotFound(msg)
