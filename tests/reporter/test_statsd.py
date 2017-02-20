try:
    from StringIO import StringIO
    from mock import patch
except ImportError:
    from io import StringIO  # noqa
    from unittest.mock import patch  # noqa
import unittest
from unittest import TestCase

from metrology import Metrology
from metrology.reporter.statsd import StatsDReporter


class StatsDReporterTest(TestCase):
    def tearDown(self):
        Metrology.stop()

    @patch.object(StatsDReporter, 'socket')
    def test_send_nobatch(self, mock):
        self.reporter = StatsDReporter('localhost', 3333,
                                       batch_size=1, conn_type='tcp')

        Metrology.meter('meter').mark()
        Metrology.counter('counter').increment()
        Metrology.timer('timer').update(5)
        Metrology.utilization_timer('utimer').update(5)
        Metrology.histogram('histogram').update(5)
        self.reporter.write()
        self.assertTrue(mock.sendall.called)
        self.assertEqual(37, len(mock.sendall.call_args_list))
        self.reporter.stop()

    @patch.object(StatsDReporter, 'socket')
    def test_send_batch(self, mock):
        self.reporter = StatsDReporter('localhost', 3333,
                                       batch_size=2, conn_type='tcp')

        Metrology.meter('meter').mark()
        Metrology.counter('counter').increment()
        Metrology.timer('timer').update(5)
        Metrology.utilization_timer('utimer').update(5)
        Metrology.histogram('histogram').update(5)
        self.reporter.write()
        self.assertTrue(mock.sendall.called)
        self.assertEqual(19, len(mock.sendall.call_args_list))
        self.reporter.stop()

    @patch.object(StatsDReporter, 'socket')
    def test_udp_send_nobatch(self, mock):
        self.reporter = StatsDReporter('localhost', 3333,
                                       batch_size=1, conn_type='udp')

        Metrology.meter('meter').mark()
        Metrology.counter('counter').increment()
        Metrology.timer('timer').update(5)
        Metrology.utilization_timer('utimer').update(5)
        Metrology.histogram('histogram').update(5)
        self.reporter.write()
        self.assertTrue(mock.sendto.called)
        self.assertEqual(37, len(mock.sendto.call_args_list))
        self.reporter.stop()

    @patch.object(StatsDReporter, 'socket')
    def test_udp_send_batch(self, mock):
        self.reporter = StatsDReporter('localhost', 3333,
                                       batch_size=2, conn_type='udp')

        Metrology.meter('meter').mark()
        Metrology.counter('counter').increment()
        Metrology.timer('timer').update(5)
        Metrology.utilization_timer('utimer').update(5)
        Metrology.histogram('histogram').update(5)
        self.reporter.write()
        self.assertTrue(mock.sendto.called)
        self.assertEqual(19, len(mock.sendto.call_args_list))
        self.reporter.stop()
