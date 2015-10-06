from subprocess import *
import psutil
import time
import threading
import traceback
import weasel.etc.config as config
import os
import resource
from math import sqrt
from weasel.utils.estimation import EWMA


class NodeMonitor(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)
        self.running = True
        self.max_buffer_len = 20
        self.max_data_buffer_len = int(config.WAITTIME / config.MONITOR_PERIOD)
        self.period_data = []  # data that was polled is stored here...
        self.data_lock = threading.Lock()
        self.previous_time = time.time()
        self.previous_network_stats = psutil.net_io_counters(pernic=True)
        psutil.cpu_percent()
        self.tasks_to_monitor = []
        self.tasks_lock = threading.Lock()
        self.task_data = {}
        self.past_disk_stats = psutil.disk_io_counters()
        self.past_cpu_stats = psutil.cpu_times()
        self.capacity = self.get_capacity()
        self.estimated_utilization = {'cpu': EWMA(int(config.WAITTIME /
                                        config.MONITOR_PERIOD), 0.8), 
                                      'memory': EWMA(int(config.WAITTIME /
                                        config.MONITOR_PERIOD), 0.85), 
                                      'network': EWMA(int(config.WAITTIME /
                                        config.MONITOR_PERIOD), 0.8)}
        self.profile = False
        # monitor all management processes
        self.ch_ttime = 0
        self.ch_systime = 0
        self.myproc = psutil.Process()
        self.myproc_stats = self.myproc.cpu_times()
        self.tasks_to_monitor.append(self.myproc.pid)
        self.task_data[self.myproc.pid] = []
        if config.MEMFS_PROCESS != -1:
            self.memfs_proc1 = psutil.Process(config.MEMFS_PROCESS)
            self.memfs_proc1.cpu_affinity([0, 1, 2])
            self.memfs_proc1_stats = self.memfs_proc1.cpu_times()
            try:
                self.task_data[config.MEMFS_PROCESS] = []
                self.tasks_to_monitor.append(config.MEMFS_PROCESS)
            except:
                self.memfs_proc1 = None
        else:
            self.memfs_proc1 = None
        if config.MEMFS2_PROCESS != -1:
            self.memfs_proc2 = []
            self.memfs_proc2_stats = []
            proc = psutil.Popen(
                "ps -e | grep memcachefs",
                shell=True,
                stdout=PIPE,
                stderr=PIPE)
            out, err = proc.communicate()
            for line in out.split('\n'):
                tokens = line.split(' ')
                if len(tokens) < 2:
                    continue
                if tokens[0] == '':
                    continue
                memfs_process = psutil.Process(int(tokens[0]))
                self.memfs_proc2.append(memfs_process)
                try:
                    self.memfs_proc2_stats.append(memfs_process.cpu_times())
                    self.task_data[tokens[0]] = []
                    self.tasks_to_monitor.append(tokens[0])
                except:
                    self.memfs_proc2 = []
                    self.memfs_proc2_stats = []
            self.max_buffer_len = self.max_buffer_len + \
                1 + len(self.memfs_proc2)
        else:
            self.memfs_proc2 = None
            self.memfs_proc2_stats = None

    def add_task_to_monitor(self, task):
        ''' keep a maximum number of tasks to read their resource utilization '''
        self.tasks_lock.acquire()
        try:
            self.tasks_to_monitor.append(task)
            self.task_data[task] = []
            if len(self.tasks_to_monitor) > self.max_buffer_len:
                if config.MONITOR_MEMFS:
                    if self.memfs_proc1 is not None and self.memfs_proc2 is not None:
                        task_name = self.tasks_to_monitor.pop(
                            1 + len(self.memfs_proc2))
                    else:
                        task_name = self.tasks_to_monitor.pop(1)
                else:
                    task_name = self.tasks_to_monitor.pop(0)
                del self.task_data[task_name]
        except:
            traceback.print_exc()
        self.tasks_lock.release()

    def run(self):
        while self.running:
            self.data_lock.acquire()
            self.tasks_lock.acquire()
            self.get_host_statistics()
            if len(self.period_data) > self.max_data_buffer_len:
                self.period_data.pop(0)
            for task in self.task_data:
                if len(self.task_data[task]) > self.max_data_buffer_len:
                    self.task_data[task].pop(0)
            self.tasks_lock.release()
            self.data_lock.release()
            time.sleep(config.MONITOR_PERIOD)

    ''' get the capacity of the host '''

    def get_capacity(self):
        capacity = {}
        capacity['network'] = config.BANDWITH
        proc = psutil.Popen("cat /proc/cpuinfo | grep processor | wc -l",
                            shell=True, stdout=PIPE, stderr=PIPE)
        proc.wait()
        out, err = proc.communicate()
        capacity['cores'] = int(out)
        capacity['cpu'] = 100
        # replace with free
        proc = psutil.Popen("top -bn1 | grep Mem:", shell=True,
                            stdout=PIPE, stderr=PIPE)
        proc.wait()
        out, err = proc.communicate()
        tokens = out.split(':')[1].split()
        memory = tokens[0].split('k')[0]
        capacity['memory'] = float(memory)
        ''' return the total capacity '''
        return capacity

    def get_host_statistics(self):
        current_data = {'cpu': 0, 'memory': 0}
        # poll the local node for cpu and memory information...
        current_cpu_stats = psutil.cpu_percent()
        current_disk_stats = psutil.disk_io_counters()
        current_time = time.time()
        current_network_stats = psutil.net_io_counters(pernic=True)
        final_network_stats = {}
        (cpu_ttime, cpu_systime, ch_ttime, ch_systime, rtime) = os.times()
        cttime = ch_ttime - self.ch_ttime
        csystime = ch_systime - self.ch_systime
        self.ch_ttime = ch_ttime
        self.ch_systime = ch_systime
        current_memory_stats = psutil.virtual_memory()
        for interface in current_network_stats:
            final_network_stats[interface] = []
            if current_time - self.previous_time > 0.1:
                final_network_stats[interface].append(
                    (current_network_stats[interface].bytes_sent / 1024 -
                        self.previous_network_stats[interface].bytes_sent / 1024) /
                    (current_time - self.previous_time))
                final_network_stats[interface].append(
                    (current_network_stats[interface].bytes_recv / 1024 -
                     self.previous_network_stats[interface].bytes_recv / 1024)
                    / (current_time - self.previous_time))
            else:
                final_network_stats[interface].append(0)
                final_network_stats[interface].append(0)
        self.previous_network_stats = current_network_stats
        current_data['memory'] = 100 * \
            current_memory_stats.used / current_memory_stats.total
        current_data['network'] = 100 * (final_network_stats['ib0'][
                                         0] + final_network_stats['ib0'][1]) / self.capacity['network']
        current_data['cpu'] = current_cpu_stats
        # self.estimated_utilization['cpu'].get_next_value(current_cpu_stats)
        self.estimated_utilization['memory'].get_next_value(
            100 *
            current_memory_stats.used /
            current_memory_stats.total)
        self.estimated_utilization['network'].get_next_value(
            100 * (final_network_stats['ib0'][0] + final_network_stats['ib0'][1]) / config.BANDWITH)
        current_cpu_stats = psutil.cpu_times()
        diff_tuple = [
            float(
                current_cpu_stats.user -
                self.past_cpu_stats.user),
            float(
                current_cpu_stats.system -
                self.past_cpu_stats.system),
            float(
                current_cpu_stats.idle -
                self.past_cpu_stats.idle),
            float(
                current_cpu_stats.iowait -
                self.past_cpu_stats.iowait),
        ]
        self.past_cpu_stats = current_cpu_stats
        self.previous_time = current_time
        self.past_disk_stats = current_disk_stats
        current_data['cpu'] = diff_tuple
        self.estimated_utilization['cpu'].get_next_value(diff_tuple[0])
        # get stats for management processes
        other_proc_data = self.get_management_proc_stats(current_data['cpu'])
        current_data['cpu'][0] = current_data['cpu'][0] - other_proc_data[0]
        current_data['cpu'][1] = current_data['cpu'][1] - other_proc_data[1]
        self.period_data.append(current_data)

    def get_management_proc_stats(self, current_cpu):
        sum = current_cpu[0] + current_cpu[1] + current_cpu[2] + current_cpu[3]
        cmyproc_stats = self.myproc.cpu_times()
        other_proc_data = [
            float(
                cmyproc_stats.user -
                self.myproc_stats.user),
            float(
                cmyproc_stats.system -
                self.myproc_stats.system)]
        myproc_diff_tuple = [
            100 *
            float(
                cmyproc_stats.user -
                self.myproc_stats.user) /
            sum,
            100 *
            float(
                cmyproc_stats.system -
                self.myproc_stats.system) /
            sum]
        self.myproc_stats = cmyproc_stats
        self.task_data[self.myproc.pid].append(
            [myproc_diff_tuple, self.myproc.memory_percent()])
        if config.MONITOR_MEMFS:
            try:
                if self.memfs_proc1 is not None:
                    cmyproc_stats = self.memfs_proc1.cpu_times()
                    other_proc_data[0] = other_proc_data[
                        0] + float(cmyproc_stats.user - self.memfs_proc1_stats.user)
                    other_proc_data[1] = other_proc_data[
                        1] + float(cmyproc_stats.system - self.memfs_proc1_stats.system)
                    myproc_diff_tuple = [
                        100 *
                        float(
                            cmyproc_stats.user -
                            self.memfs_proc1_stats.user) /
                        sum,
                        100 *
                        float(
                            cmyproc_stats.system -
                            self.memfs_proc1_stats.system) /
                        sum]
                    self.task_data[config.MEMFS_PROCESS].append(
                        [myproc_diff_tuple, self.memfs_proc1.memory_percent()])
                    self.memfs_proc1_stats = cmyproc_stats
                if self.memfs_proc2 is not None:
                    i = 0
                    stats_tmp = []
                    for proc in self.memfs_proc2:
                        cmyproc_stats = proc.cpu_times()
                        other_proc_data[0] = other_proc_data[
                            0] + float(cmyproc_stats.user - self.memfs_proc2_stats[i].user)
                        other_proc_data[1] = other_proc_data[
                            1] + float(cmyproc_stats.system - self.memfs_proc2_stats[i].system)
                        myproc_diff_tuple = [
                            100 *
                            float(
                                cmyproc_stats.user -
                                self.memfs_proc2_stats[i].user) /
                            sum,
                            100 *
                            float(
                                cmyproc_stats.system -
                                self.memfs_proc2_stats[i].system) /
                            sum]
                        self.task_data[str(proc.pid)].append(
                            [myproc_diff_tuple, proc.memory_percent()])
                        stats_tmp.append(cmyproc_stats)
                        i = i + 1
                    self.memfs_proc2_stats = stats_tmp
            except:
                pass
        return other_proc_data

    def getKey(self, item):
        return item[1]

    def get_task_data(self):
        self.tasks_lock.acquire()
        temp_task_data = self.task_data.copy()
        self.tasks_lock.release()
        return temp_task_data

    def get_estimated_utilization(self):
        self.tasks_lock.acquire()
        data = {
            'cpu': self.estimated_utilization['cpu'].get_estimated_value(),
            'memory': self.estimated_utilization['memory'].get_estimated_value(),
            'network': self.estimated_utilization['network'].get_estimated_value()}
        self.tasks_lock.release()
        return data

    def get_average_utilization(self):
        avg = {'cpu': 0.0, 'memory': 0.0, 'network': 0.0}
        max = {'cpu': -1, 'memory': -1, 'network': -1}
        std = {'cpu': 0.0, 'memory': 0.0, 'network': 0.0}
        last_utilization = self.get_data()
        for data in last_utilization:
            avg['cpu'] = avg['cpu'] + data['cpu']
            avg['memory'] = avg['memory'] + data['memory']
            avg['network'] = avg['network'] + data['network']
            if data['cpu'] > max['cpu']:
                max['cpu'] = data['cpu']
            if data['memory'] > max['memory']:
                max['memory'] = data['memory']
            if data['network'] > max['network']:
                max['network'] = data['network']

        n = len(last_utilization)
        if n > 1:
            for r in avg:
                avg[r] = avg[r] / n
        if n > 2:
            for r in std:
                for data in last_utilization:
                    std[r] = std[r] + (data[r] - avg[r])**2
            for r in std:
                std[r] = sqrt(std[r] / float(n - 1))

        return (avg, max, std)

    def getCPUKey(self, item):
        return item['cpu'][0]

    def getMEMKey(self, item):
        return item['memory']

    def getBWKey(self, item):
        return item['network']

    def gethistokey(self, item):
        avg = 0.0
        for i in item:
            avg = avg + i
        if len(item) > 1:
            avg = avg / len(item)
        return (len(item), avg)

    def get_median_utilization(self):
        last_utilization = self.get_data()
        median = {'cpu': 0.0, 'cpu_io': 0.0, 'cpu_idle': 0.0, 'cpu_sys': 0.0,
                  'memory': 0.0, 'network': 0.0, 'disk': 0.0}
        sorted_by_cpu = sorted(last_utilization, key=self.getCPUKey)
        sorted_by_memory = sorted(last_utilization, key=self.getMEMKey)
        sorted_by_network = sorted(last_utilization, key=self.getBWKey)
        list_len = len(last_utilization)
        if list_len == 0:
            return median
        if list_len <= 2:
            median = {'cpu': sorted_by_cpu[-1]['cpu'][0],
                      'cpu_io': sorted_by_cpu[-1]['cpu'][3],
                      'cpu_idle': sorted_by_cpu[-1]['cpu'][2],
                      'cpu_sys': sorted_by_cpu[-1]['cpu'][1],
                      'memory': sorted_by_memory[-1]['memory'],
                      'network': sorted_by_network[-1]['network']}
            sum = median['cpu'] + median['cpu_io'] + \
                median['cpu_sys'] + median['cpu_idle']
            median['cpu'] = 100 * median['cpu'] / sum
            return median
        middle = int(list_len / 2)
        if list_len % 2 == 0:
            median['cpu_io'] = (
                sorted_by_cpu[middle - 1]['cpu'][3] + sorted_by_cpu[middle + 1]['cpu'][3]) / 2
            median['cpu_idle'] = (
                sorted_by_cpu[middle - 1]['cpu'][2] + sorted_by_cpu[middle + 1]['cpu'][2]) / 2
            median['cpu'] = (
                sorted_by_cpu[middle - 1]['cpu'][0] + sorted_by_cpu[middle + 1]['cpu'][0]) / 2
            median['cpu_sys'] = (
                sorted_by_cpu[middle - 1]['cpu'][1] + sorted_by_cpu[middle + 1]['cpu'][1]) / 2
            median['memory'] = (sorted_by_memory[
                                middle - 1]['memory'] + sorted_by_memory[middle + 1]['memory']) / 2
            median['network'] = (sorted_by_network[
                                 middle - 1]['network'] + sorted_by_network[middle + 1]['network']) / 2
        else:
            median['cpu'] = sorted_by_cpu[middle + 1]['cpu'][0]
            median['cpu_io'] = sorted_by_cpu[middle + 1]['cpu'][3]
            median['cpu_sys'] = sorted_by_cpu[middle + 1]['cpu'][1]
            median['cpu_idle'] = sorted_by_cpu[middle + 1]['cpu'][2]
            median['memory'] = sorted_by_memory[middle + 1]['memory']
            median['network'] = sorted_by_network[middle + 1]['network']
        sum = median['cpu'] + median['cpu_io'] + \
            median['cpu_sys'] + median['cpu_idle']
        median['cpu'] = 100 * float(median['cpu']) / float(sum)
        return median

    def get_utilization_by_histogram(self):
        last_utilization = self.get_data()
        histogram_util = {
            'cpu': 0.0,
            'cpu_idle': 0.0,
            'cpu_sys': 0.0,
            'cpu_io': 0.0,
            'memory': 0.0,
            'network': 0.0}
        histo_array_cpu = []
        histo_array_mem = []
        histo_array_bw = []
        if len(last_utilization) < 1:
            return histogram_util
        if len(last_utilization) == 1:
            data = last_utilization[0]
            sum = data['cpu'][0] + data['cpu'][1] + \
                data['cpu'][2] + data['cpu'][3]
            return {
                'cpu': 100 *
                data['cpu'][0] /
                sum,
                'cpu_idle': 100 *
                data['cpu'][2] /
                sum,
                'cpu_sys': 100 *
                data['cpu'][1] /
                sum,
                'cpu_io': 0,
                'memory': data['memory'],
                'network': data['network']}
        for i in range(21):
            histo_array_cpu.append([])
            histo_array_mem.append([])
            histo_array_bw.append([])
        for data in last_utilization:
            sum = data['cpu'][0] + data['cpu'][1] + \
                data['cpu'][2] + data['cpu'][3]
            real_cpu = 100 * float(data['cpu'][0]) / sum
            if real_cpu > 0.01:
                histo_array_cpu[int(real_cpu / 5)].append(real_cpu)
            if data['memory'] > 0.01:
                histo_array_mem[int(data['memory'] / 5)].append(data['memory'])
            if data['network'] > 0.01:
                histo_array_bw[
                    int(data['network'] / 5)].append(data['network'])
        sorted_numbers = {
            'cpu_idle': [0], 'cpu_sys': [0], 'cpu_io': [0], 'cpu': sorted(
                histo_array_cpu, key=self.gethistokey, reverse=True)[0], 'memory': sorted(
                histo_array_mem, key=self.gethistokey, reverse=True)[0], 'network': sorted(
                histo_array_bw, key=self.gethistokey, reverse=True)[0]}
        for r in histogram_util:
            avg = 0.0
            for i in sorted_numbers[r]:
                avg = avg + i
            if len(sorted_numbers[r]) > 0:
                avg = avg / len(sorted_numbers[r])
            histogram_util[r] = avg
        return histogram_util

    def get_data(self):
        self.data_lock.acquire()
        data = self.period_data[:]
        self.data_lock.release()
        return data

    def shutdown(self):
        self.running = False
