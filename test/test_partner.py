import logging
import time
import unittest as unittest
from unittest import mock
from multiprocessing.context import Process


import snap7.partner
from snap7.exceptions import Snap7Exception

logging.basicConfig(level=logging.WARNING)


def passive_partner_echo():
    passive_partner = snap7.partner.Partner(active=False)
    passive_partner.start_to("0.0.0.0", "127.0.0.1", 4098, 4098)
    recived = passive_partner.check_as_b_recv_completion()
    while True:
        while passive_partner.get_status() == 3:
            recived = passive_partner.check_as_b_recv_completion()
            if recived:
                rid, data = recived
                passive_partner.b_send(data, rid)
            time.sleep(0.01)


class TestPartner(unittest.TestCase):

    process = None

    @classmethod
    def setUpClass(cls):
        cls.process = Process(target=passive_partner_echo)
        cls.process.start()

    @classmethod
    def tearDownClass(cls):
        cls.process.terminate()
        cls.process.join(1)
        if cls.process.is_alive():
            cls.process.kill()

    def setUp(self):
        self.active_partner = snap7.partner.Partner(active=True)
        self.active_partner.start_to("0.0.0.0", "127.0.0.1", 4098, 4098)
        time.sleep(1)
        self.assertEqual(self.active_partner.get_status(), 3)

    def tearDown(self):
        self.active_partner.stop()
        self.active_partner.destroy()

    def test_as_b_send(self):
        self.active_partner.as_b_send(bytearray(b'test'), 1)
        self.assertEqual(self.active_partner.b_recv(500), (1, bytearray(b'test')))

    def test_b_recv(self):
        self.active_partner.as_b_send(bytearray(b'test'), 1)
        self.assertEqual(self.active_partner.b_recv(200), (1, bytearray(b'test')))

    def test_b_send(self):
        self.active_partner.b_send(bytearray(b'test'), 1)
        self.assertEqual(self.active_partner.b_recv(200), (1, bytearray(b'test')))

    def test_check_as_b_recv_completion(self):
        self.active_partner.as_b_send(bytearray(b'test'), 1)
        time.sleep(0.2)
        self.assertEqual(self.active_partner.check_as_b_recv_completion(), (1, bytearray(b'test')))

    def test_check_as_b_send_completion(self):
        self.active_partner.b_send(bytearray(b'test'), 1)
        time.sleep(0.01)
        self.active_partner.check_as_b_send_completion()

    def test_create(self):
        self.active_partner.create()

    def test_destroy(self):
        self.active_partner.destroy()

    def test_error_text(self):
        snap7.common.error_text(0, context="partner")

    def test_get_last_error(self):
        self.active_partner.get_last_error()

    def test_get_param(self):
        expected = (
            (snap7.types.LocalPort, 0),
            (snap7.types.RemotePort, 102),
            (snap7.types.PingTimeout, 750),
            (snap7.types.SendTimeout, 10),
            (snap7.types.RecvTimeout, 3000),
            (snap7.types.SrcRef, 256),
            (snap7.types.DstRef, 0),
            (snap7.types.PDURequest, 480),
            (snap7.types.WorkInterval, 100),
            (snap7.types.BSendTimeout, 3000),
            (snap7.types.BRecvTimeout, 3000),
            (snap7.types.RecoveryTime, 500),
            (snap7.types.KeepAliveTime, 5000),
        )
        for param, value in expected:
            self.assertEqual(self.active_partner.get_param(param), value)

        self.assertRaises(Exception, self.active_partner.get_param,
                          snap7.types.MaxClients)

    def test_get_stats(self):
        self.assertTupleEqual(self.active_partner.get_stats(), (0, 0, 0, 0))

    def test_get_status(self):
        self.assertIn(self.active_partner.get_status(), (0, 1, 2, 3))

    def test_get_times(self):
        self.assertTupleEqual(self.active_partner.get_times(), (0, 0))

    def test_set_param(self):
        values = (
            (snap7.types.PingTimeout, 800),
            (snap7.types.SendTimeout, 15),
            (snap7.types.RecvTimeout, 3500),
            (snap7.types.WorkInterval, 50),
            (snap7.types.SrcRef, 128),
            (snap7.types.DstRef, 128),
            (snap7.types.SrcTSap, 128),
            (snap7.types.PDURequest, 470),
            (snap7.types.BSendTimeout, 2000),
            (snap7.types.BRecvTimeout, 2000),
            (snap7.types.RecoveryTime, 400),
            (snap7.types.KeepAliveTime, 4000),
        )
        for param, value in values:
            self.active_partner.set_param(param, value)

        self.assertRaises(Exception, self.active_partner.set_param,
                          snap7.types.RemotePort, 1)

    def test_set_recv_callback(self):
        self.active_partner.set_recv_callback()

    def test_set_send_callback(self):
        self.active_partner.set_send_callback()

    def test_start(self):
        self.active_partner.start()

    def test_start_to(self):
        self.active_partner.start_to('0.0.0.0', '0.0.0.0', 0, 0)

    def test_stop(self):
        self.active_partner.stop()

    def test_wait_as_b_send_completion(self):
        self.assertRaises(Snap7Exception, self.active_partner.wait_as_b_send_completion)


class TestLibraryIntegration(unittest.TestCase):
    def setUp(self):
        # replace the function load_library with a mock
        self.loadlib_patch = mock.patch('snap7.partner.load_library')
        self.loadlib_func = self.loadlib_patch.start()

        # have load_library return another mock
        self.mocklib = mock.MagicMock()
        self.loadlib_func.return_value = self.mocklib

        # have the Par_Create of the mock return None
        self.mocklib.Par_Create.return_value = None

    def tearDown(self):
        # restore load_library
        self.loadlib_patch.stop()

    def test_create(self):
        partner = snap7.partner.Partner()
        self.mocklib.Par_Create.assert_called_once()

    def test_gc(self):
        partner = snap7.partner.Partner()
        del partner
        self.mocklib.Par_Destroy.assert_called_once()


if __name__ == '__main__':
    unittest.main()
