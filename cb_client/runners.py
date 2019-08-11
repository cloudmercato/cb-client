import os
import io
import argparse
import logging
import tempfile
import subprocess

from cb_client import wringers

logger = logging.getLogger('cb_client')


class BaseRunner:
    wringer_class = None

    def parse_args(self):
        raise NotImplementedError()

    def get_cmd(self, args):
        raise NotImplementedError()

    def get_wringer_kwargs(self, cli_args, input_):
        return {
            'input_': input_,
        }

    def get_wringer(self, cli_args, input_):
        if self.wringer_class is None:
            raise Exception("Configure a wringer_class")
        kwargs = self.get_wringer_kwargs(cli_args, input_)
        return self.wringer_class(**kwargs)

    def setup(self):
        pass

    def tear_down(self):
        pass

    def run_cmd(self, cmd):
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        return stdout, stderr

    def run(self):
        args = self.parse_args()
        cmd = self.get_cmd(args)
        self.setup()
        out, err = self.run_cmd(cmd)
        logger.debug(out)
        logger.info(err)
        wringer_input = io.StringIO(out.decode())
        wringer = self.get_wringer(args, wringer_input)
        wringer.run()
        self.tear_down()


class SysbenchCpuRunner(BaseRunner):
    wringer_class = wringers.SysbenchCpuWringer

    def parse_args(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--threads', default=1, type=int)
        parser.add_argument('--time', default=60, type=int)
        parser.add_argument('--cpu-max-prime', default=64000, type=int)
        return parser.parse_args()

    def get_cmd(self, args):
        cmd = [
            'sysbench',
            '--threads=%d' % args.threads,
            '--time=%d' % args.time,
            'cpu',
            '--cpu-max-prime=%d' % args.cpu_max_prime,
            'run'
        ]
        return cmd


class SysbenchRamRunner(BaseRunner):
    wringer_class = wringers.SysbenchRamWringer

    def parse_args(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--threads', default=1, type=int)
        parser.add_argument('--time', default=60, type=int)
        parser.add_argument('--memory-block-size', default='1K', type=str)
        parser.add_argument('--memory-total-size', default='100G', type=str)
        parser.add_argument('--memory-scope', default='global', type=str)
        parser.add_argument('--memory-hugetlb', default='off', type=str)
        parser.add_argument('--memory-oper', default='write', type=str,
                            choices=['write', 'read'])
        parser.add_argument('--memory-access-mode', default='seq', type=str,
                            choices=['seq', 'rnd'])
        return parser.parse_args()

    def get_cmd(self, args):
        cmd = [
            'sysbench',
            '--threads=%d' % args.threads,
            '--time=%d' % args.time,
            'memory',
            '--memory-block-size=%s' % args.memory_block_size,
            '--memory-total-size=%s' % args.memory_total_size,
            '--memory-scope=%s' % args.memory_scope,
            '--memory-hugetlb=%s' % args.memory_hugetlb,
            '--memory-oper=%s' % args.memory_oper,
            '--memory-access-mode=%s' % args.memory_access_mode,
            'run'
        ]
        return cmd


class Geekbench4Runner(BaseRunner):
    wringer_class = wringers.Geekbench4Wringer

    def parse_args(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--compute', default=None, choices=['cuda', 'opencl'])
        return parser.parse_args()

    def get_cmd(self, args):
        cmd = [
            'geekbench4',
            '--no-upload',
            '--export-json',
            self.result_file,
        ]
        if args.compute:
            cmd += ['--compute', args.compute]
        return cmd

    def get_wringer_kwargs(self, args, input_):
        kwargs = super(Geekbench4Runner, self).get_wringer_kwargs(
            args, input_)
        mode = args.compute or 'standard'
        kwargs.update({
            'format': 'json',
            'input_': open(self.result_file),
            'mode': mode
        })
        return kwargs

    @property
    def result_file(self):
        if not hasattr(self, '_result_file'):
            self._result_file = tempfile.mktemp()
        return self._result_file

    def setup(self):
        cmd = [
            'geekbench4',
            '--unlock',
            os.environ['GB4_EMAIL'],
            os.environ['GB4_KEY'],
        ]
        self.run_cmd(cmd)

    def tear_down(self):
        try:
            os.remove(self.result_file)
        except FileNotFoundError:
            pass
        del self._result_file


class FioRunner(BaseRunner):
    wringer_class = wringers.FioWringer

    def parse_args(self):
        parser = argparse.ArgumentParser()
        # FIO
        parser.add_argument('--numjobs', default=1, type=int)
        parser.add_argument('--bs', default='4k')
        parser.add_argument('--mode', default='rand', choices=['seq', 'rand'])
        parser.add_argument('--readwrite', default='read', choices=['read', 'write'])
        parser.add_argument('--ioengine', default='libaio')
        parser.add_argument('--iodepth', default='32')
        parser.add_argument('--direct', default='1', choices=['0', '1'])
        parser.add_argument('--invalidate', default='1', choices=['0', '1'])
        parser.add_argument('--end_fsync', default='1', choices=['0', '1'])
        parser.add_argument('--runtime', default=60, type=int)
        parser.add_argument('--size', default=None)
        parser.add_argument('-r', '--ratio', help='FIO mixed ratio', default='100:0')
        parser.add_argument('--filename')
        # CB
        parser.add_argument('-vf', '--volume-flavor-id', type=int, required=False)
        parser.add_argument('-vm', '--volume-manager-id', type=int, required=False)
        parser.add_argument('-ns', '--network-storage-id', type=int, required=False)
        return parser.parse_args()

    def get_cmd(self, args):
        mode = '' if args.mode == 'seq' else args.mode
        rration, wration = args.ratio.split(':')
        ratio = wration if 'write' in args.readwrite else rration
        cmd = [
            'fio',
            '--numjobs=%s' % args.numjobs,
            '--bs=%s' % args.bs,
            '--rw=%s%s' % (mode, args.readwrite),
            '--ioengine=%s' % args.ioengine,
            '--iodepth=%s' % args.iodepth,
            '--direct=%s' % args.direct,
            '--invalidate=%s' % args.invalidate,
            '--end_fsync=%s' % args.end_fsync,
            '--rwmixread=%s' % ratio,
            '--time_based',
            '--runtime=%s' % args.runtime,
            '--timeout=%s' % args.runtime,
            '--group_reporting',
            '--output-format=json',
            '--filename=%s' % args.filename,
            '--name=fio'
        ]
        if args.size:
            cmd.insert(-1, '--size=%s' % args.size)
        return cmd

    def get_wringer_kwargs(self, args, input_):
        kwargs = super(FioRunner, self).get_wringer_kwargs(
            args, input_)
        kwargs.update({
            'numjobs': args.numjobs,
            'ratio': args.ratio,
            'volume_flavor_id': args.volume_flavor_id,
            'volume_manager_id': args.volume_manager_id,
            'network_storage_id': args.network_storage_id,
        })
        return kwargs

    def tear_down(self):
        try:
            os.remove('/fio')
        except FileNotFoundError:
            pass


RUNNERS = {
    'sysbench_cpu': SysbenchCpuRunner,
    'sysbench_ram': SysbenchRamRunner,
    'geekbench4': Geekbench4Runner,
    'fio': FioRunner,
}


def main():
    import sys
    runner = RUNNERS[sys.argv.pop(1)]()
    runner.run()


if __name__ == '__main__':
    main()
