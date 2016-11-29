from datetime import datetime
import functools
import os
import re
import socket
import sys

from metrology.instruments import *  # noqa
from metrology.reporter.base import Reporter


class StatsDReporter(Reporter):
    """
    A statsd reporter that sends metrics to statsd daemon ::

      reporter = StatsDReporter('statsd.local', 8125)
      reporter.start()

    :param host: hostname of statsd daemon
    :param port: port of daemon
    :param interval: time between each reports
    :param prefix: metrics name prefix
    """
    def __init__(self, host, port, conn_type='udp', **options):
        self.host = host
        self.port = port
        self.conn_type = conn_type

        self.prefix = options.get('prefix')
        self.batch_size = options.get('batch_size', 100)
        self.batch_buffer = ''
        if self.batch_size <= 0:
            self.batch_size = 1
        self._socket = None
        super(StatsDReporter, self).__init__(**options)
        self.batch_count = 0
        if conn_type == 'tcp':
            self._send = self._send_tcp
        else:
            self._send = self._send_udp

        self.translate_dict = {'meter': 'm', 'gauge': 'g',
                               'timer': 'ms', 'counter': 'c',
                               'histogram': 'h'}

    @property
    def socket(self):
        if not self._socket:
            if self.conn_type == 'tcp':
                self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self._socket.connect((self.host, self.port))
            else:
                self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        return self._socket

    def write(self):
        for name, metric in self.registry:

            send = functools.partial(self.send_metric,
                                     name=name, metric=metric)

            if isinstance(metric, Meter):
                send(mtype='meter',
                     keys=['count', 'one_minute_rate', 'five_minute_rate',
                           'fifteen_minute_rate', 'mean_rate'])
            if isinstance(metric, Gauge):
                send(mtype='gauge', keys=['value'])
            if isinstance(metric, UtilizationTimer):
                send(mtype='timer',
                     keys=['count', 'one_minute_rate', 'five_minute_rate',
                          'fifteen_minute_rate', 'mean_rate', 'min', 'max',
                          'mean', 'stddev', 'one_minute_utilization',
                          'five_minute_utilization',
                          'fifteen_minute_utilization', 'mean_utilization'],
                     snapshot_keys=['median', 'percentile_95th',
                                    'percentile_99th', 'percentile_999th'])
            if isinstance(metric, Timer):
                send(mtype='timer',
                     keys=['count', 'total_time', 'one_minute_rate',
                          'five_minute_rate', 'fifteen_minute_rate',
                          'mean_rate', 'min', 'max', 'mean', 'stddev'],
                     snapshot_keys=['median', 'percentile_95th',
                                    'percentile_99th', 'percentile_999th'])
            if isinstance(metric, Counter):
                send(mtype='counter', keys=['count'])
            if isinstance(metric, Histogram):
                send(mtype='histogram',
                     keys=['count', 'min', 'max', 'mean', 'stddev'],
                     snapshot_keys=['median', 'percentile_95th',
                                    'percentile_99th', 'percentile_999th'])

        self._send()

    def send_metric(self, name, mtype, metric, keys, snapshot_keys=[]):
        base_name = re.sub(r"\s+", "_", name)
        if self.prefix:
            base_name = "{0}.{1}".format(self.prefix, base_name)

        for name in keys:
            value = getattr(metric, name)
            self._buffered_send_metric(base_name, name, value, datetime.now())

        if hasattr(metric, 'snapshot'):
            for name in snapshot_keys:
                value = getattr(metric.snapshot, name)
                self._buffered_send_metric(base_name, name, value,
                                           datetime.now())

    def _buffered_send_metric(self, base_name, name, value, time):
        self.batch_count += 1
        self.batch_buffer += "{0}.{1} {2} {3}{4}".format(base_name, name,
                                                         value, time,
                                                         os.linesep)
        # Check if we reach batch size and send
        if self.batch_count >= self.batch_size:
            self._send()

    def _send_tcp(self):
        if len(self.batch_buffer):
            if sys.version_info[0] > 2:
                self.socket.sendall(bytes(self.batch_buffer, 'ascii'))
            else:
                self.socket.sendall(self.batch_buffer)

            self.batch_count = 0
            self.batch_buffer = ''

    def _send_udp(self):
        if len(self.batch_buffer):
            if sys.version_info[0] > 2:
                self.socket.sendto(self.batch_buffer, (self.host, self.port))
            else:
                self.socket.sendto(self.batch_buffer, (self.host, self.port))

            self.batch_count = 0
            self.batch_buffer = ''
