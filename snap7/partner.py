"""
Snap7 code for partnering with a siemens 7 server.

This allows you to create a S7 peer to peer communication. Unlike the
client-server model, where the client makes a request and the server replies to
it, the peer to peer model sees two components with same rights, each of them
can send data asynchronously. The only difference between them is the one who
is requesting the connection.
"""
import logging
import re
from ctypes import (CFUNCTYPE, byref, c_int, c_int32, c_ubyte, c_uint32,
                    c_void_p)
from typing import Any, Callable, Optional, Tuple, Union

import snap7.types
from snap7.common import check_error, ipv4, load_library
from snap7.exceptions import Snap7Exception, Snap7TimeoutException

logger = logging.getLogger(__name__)

err_Par_Recv_Timeout = 0x00B00000
err_Par_Send_Timeout = 0x00A00000


def error_wrap(func):
    """Parses a s7 error code returned the decorated function.
    """

    def f(*args, **kw):
        code = func(*args, **kw)
        check_error(code, context="partner")
        return code

    return f


class Partner:
    """
    A snap7 partner.
    """
    _pointer: Optional[c_void_p]

    def __init__(self, active: bool = False):
        self._library = load_library()
        self._pointer = None
        self.create(active)
        self._recv_buffer = bytearray(snap7.types.buffer_size)
        self._recv_buffer_c = (
            c_ubyte * snap7.types.buffer_size).from_buffer(self._recv_buffer)
        self._recv_callback = None
        self._send_callback = None

    def __del__(self):
        self.destroy()

    @error_wrap
    def as_b_send(self, data: bytearray, rid: int = 1) -> int:
        """Sends a data packet to the partner.

        This function is asynchronous, i.e. it terminates immediately, a completion
        method is needed to know when the transfer is complete.

        Args:
            data: data which should be sended length must be smaller than 65535
            rid: Routing User parameter. Defaults to 1.

        Raises:
            ValueError: if length of data is bigger than 65535

        Returns:
            Error code from snap7 library.
        """
        size = len(data)
        if size >= snap7.types.buffer_size:
            raise ValueError('data to send too big maxsize =  65535')
        cdata = (c_ubyte * size).from_buffer_copy(data)
        result = self._library.Par_AsBSend(
            self._pointer, snap7.types.longword(rid), byref(cdata), size)
        return result

    def b_recv(self, timeout: int = snap7.types.BRecvTimeout) -> Optional[Tuple[int, bytearray]]:
        """Receives a data packet from the partner.

        This function is synchronous, it waits until a packet is received or the timeout
        supplied expires.

        Args:
            timeout: Timeout value in ms. Defaults to snap7.types.BRecvTimeout.

        Raises:
            Snap7TimeoutException: if Timeout expires and no packet was received

        Returns:
             Tuple containing rid(Routing User parameter) of received packet and the packet
        """
        size = c_int32()
        rid = snap7.types.longword()
        result = self._library.Par_BRecv(self._pointer, byref(
            rid), self._recv_buffer_c, byref(size), snap7.types.longword(timeout))
        if result == err_Par_Recv_Timeout:
            raise Snap7TimeoutException('Partner recv Timeout')

        else:
            check_error(result, context="partner")
            return rid.value, self._recv_buffer[0:size.value]

    @error_wrap
    def b_send(self, data: bytearray, rid: int = 1) -> int:
        """Sends a data packet to the partner.

        This function is synchronous, i.e. it terminates when the transfer job (send+ack) is complete.

        Args:
            data: data which should be sended length must be smaller than 65535
            rid: Routing User parameter. Defaults to 1.

        Raises:
            ValueError: if length of data is bigger than 65535
            Snap7TimeoutException: if snap7 lib returned a send timeout error code

        Returns:
            Error code from snap7 library.
        """
        size = len(data)
        if size >= snap7.types.buffer_size:
            raise ValueError('data to send too big maxsize =  65535')
        cdata = (
            snap7.types.wordlen_to_ctypes[snap7.types.S7WLByte] * size).from_buffer(data)
        result = self._library.Par_BSend(
            self._pointer, snap7.types.longword(rid), byref(cdata), size)

        if result == err_Par_Send_Timeout:
            raise Snap7TimeoutException('Partner send Timeout')
        else:
            check_error(result, context="partner")
            return result

    def check_as_b_recv_completion(self) -> Union[bool, Tuple[int, bytearray]]:
        """Checks if a packed received was received and return its rid and data

        if no packed received returns  False

        Raises:
            Snap7Exception: invalid call

        Returns:
            Tuple containing rid(Routing User parameter) of received packet and the packet
        """
        op_result = c_int32()
        rid = snap7.types.longword()
        size = c_int32()

        result = self._library.Par_CheckAsBRecvCompletion(
            self._pointer, byref(op_result), byref(rid), self._recv_buffer_c, byref(size))

        if result == -2:
            raise Snap7Exception(
                "The check_as_b_recv_completion parameter was invalid")
        elif result == 0:
            check_error(op_result.value, context="partner")
            return rid.value, self._recv_buffer[0:size.value]
        else:
            return False

    def check_as_b_send_completion(self) -> bool:
        """checks if the current asynchronous send job was completed.

        Raises:
            Snap7Exception: invalid call

        Returns:
            True if completed False if not
        """
        op_result = c_int32()
        result = self._library.Par_CheckAsBSendCompletion(
            self._pointer, byref(op_result))

        if result == -2:
            raise Snap7Exception("The check_as_b_send_completion parameter was invalid")

        elif result == 0:
            check_error(op_result.value, context="partner")
            return True

        else:
            return False

    def create(self, active: bool = False):
        """Creates a active or passive Partner

        Args:
            active: True for active Partner False for passive . Defaults to False.
        """
        self._library.Par_Create.restype = snap7.types.S7Object
        self._pointer = snap7.types.S7Object(self._library.Par_Create(int(active)))

    def destroy(self):
        """Destroys the Partner

        Before destruction the Partner is stopped, all clients disconnected and
        all shared memory blocks released.
        """
        if self._library:
            return self._library.Par_Destroy(byref(self._pointer))
        return None

    def get_last_error(self) -> int:
        """Returns the last job result.
        """
        error = c_int32()
        result = self._library.Par_GetLastError(self._pointer, byref(error))
        check_error(result, "partner")
        return error.value

    def get_param(self, number) -> int:
        """Reads an internal Partner object parameter.

        Args:
            number: number of param

        Returns:
            value of param
        """
        logger.debug(f"retreiving param number {number}")
        type_ = snap7.types.param_types[number]
        value = type_()
        code = self._library.Par_GetParam(self._pointer, c_int(number),
                                          byref(value))
        check_error(code)
        return value.value

    def get_stats(self) -> Tuple[int, int, int, int]:
        """Returns some statistics.

        Returns:
            a tuple containing bytes send, received, send errors, recv errors
        """
        sent = c_uint32()
        recv = c_uint32()
        send_errors = c_uint32()
        recv_errors = c_uint32()
        result = self._library.Par_GetStats(self._pointer, byref(sent),
                                            byref(recv),
                                            byref(send_errors),
                                            byref(recv_errors))
        check_error(result, "partner")
        return sent.value, recv.value, send_errors.value, recv_errors.value

    def get_status(self) -> int:
        """Returns the Partner status.
        """
        status = c_int32()
        result = self._library.Par_GetStatus(self._pointer, byref(status))
        check_error(result, "partner")
        return status.value

    def get_times(self) -> Tuple[int, int]:
        """Returns the last send and recv jobs execution time in milliseconds.

        Returns:
            Tuple of send and recv time execution time
        """
        send_time = c_int32()
        recv_time = c_int32()
        result = self._library.Par_GetTimes(self._pointer, byref(send_time), byref(recv_time))
        check_error(result, "partner")
        return send_time.value, recv_time.value

    @error_wrap
    def set_param(self, number: int, value) -> int:
        """Sets an internal Partner object parameter.

        Args:
            number: number of param
            value: value of param to set

        Returns:
            Error code from snap7 library.
        """
        logger.debug(f"setting param number {number} to {value}")
        type_ = snap7.types.param_types[number]
        return self._library.Par_SetParam(self._pointer, number,
                                          byref(type_(value)))

    @error_wrap
    def set_recv_callback(self, callback: Callable[[int, int, bytearray], None]) -> int:
        """Sets the user callback that the Partner object has to call when a data
        packet is incoming.

        Args:
            callback: callback funtion Callable[[int, int, bytearray]

        Returns:
            Error code from snap7 library.
        """
        usr_ptr = c_void_p()
        callback_c_function: Callable[..., Any] = CFUNCTYPE(
            None, c_void_p, c_int32, snap7.types.longword, c_void_p, c_int32)

        def wrapper(usr_ptr: Optional[c_void_p], op_result: int, rid: int, data_pointer: int, size: int):
            """Wraps python function into a ctypes function

            Args:
                usrptr: not used
                op_result: snap7 error code of the recv operation
                rid: Routing User parameter of the received packet
                data_pointer: pointer to the buffer memory of snap7 lib
                size: size of received packet
            """
            logger.info(f"recv callback: rid = {rid}  size = {size} ")

            if op_result == 0:
                # making a c array with rigth size from the pointer and copy it to bytearray
                data = bytearray((c_ubyte * size).from_address(data_pointer))
                callback(op_result, rid, data)
            else:
                callback(op_result, None, None)
            return None

        self._recv_callback = callback_c_function(wrapper)

        return self._library.Par_SetRecvCallback(self._pointer, self._recv_callback, usr_ptr)

    @error_wrap
    def set_send_callback(self, callback: Callable[[int, int], None]) -> int:
        """Sets the user callback that the Partner object has to call when the
        asynchronous data sent is complete.

        Args:
            callback: callback funtion Callable[[int, int], None])

        Returns:
            Error code from snap7 library.
        """
        usrPtr = c_void_p()  # c Null pointer
        callback_c_function: Callable[..., Any] = CFUNCTYPE(None, c_void_p, c_int32)
        self._send_callback = callback_c_function(lambda _, op_result: callback(op_result))
        return self._library.Par_SetSendCallback(self._pointer, self._send_callback, usrPtr)

    @error_wrap
    def start(self) -> int:
        """Starts the Partner and binds it to the IP address specified in the previous call of start_to
        """
        return self._library.Par_Start(self._pointer)

    @error_wrap
    def start_to(self, local_ip: str, remote_ip: str, local_tsap: int, remote_tsap: int) -> int:
        """Starts the Partner and binds it to the specified IP address and the IsoTCP port.


        Args:
            local_ip: PC host IPV4 Address. "0.0.0.0" is the default adapter
            remote_ip: PLC IPV4 Address
            local_tsap: Local TSAP as int
            remote_tsap: Local TSAP as int

        Raises:
            ValueError: if invalid value for local_ip or remote_ip suplied

        Returns:
            Error code from snap7 library.
        """
        if not re.match(ipv4, local_ip):
            raise ValueError(f"{local_ip} is invalid ipv4")
        if not re.match(ipv4, remote_ip):
            raise ValueError(f"{remote_ip} is invalid ipv4")
        logger.info(f"starting partnering from {local_ip} to {remote_ip}")
        return self._library.Par_StartTo(self._pointer, local_ip.encode(), remote_ip.encode(),
                                         snap7.types.word(local_tsap),
                                         snap7.types.word(remote_tsap))

    def stop(self) -> int:
        """Stops the Partner, disconnects gracefully the remote partner.
        """
        return self._library.Par_Stop(self._pointer)

    @error_wrap
    def wait_as_b_send_completion(self, timeout: int = 0) -> int:
        """waits until the current asynchronous send job is done or the timeout expires.

        Args:
            timeout: Timeout value in ms. Defaults to snap7.types.BSendTimeout.

        Raises:
            Snap7TimeoutException: if Timeout expires and send job is not done

        Returns:
            Error code from snap7 library.
         """
        
        result = self._library.Par_WaitAsBSendCompletion(
            self._pointer, timeout)

        if result == err_Par_Send_Timeout:
            raise Snap7TimeoutException(
                'Partner wait for asynchronous send  Timeout')
        else:
            return result