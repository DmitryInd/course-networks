import socket
import logging
import time
from queue import PriorityQueue

logger = logging.getLogger(__name__)


class TCPSegment:
    service_len = 8 + 8
    ack_timeout = 0.05

    def __init__(self, seq_number: int, ack_number: int, data: bytes):
        self.seq_number = seq_number
        self.ack_number = ack_number
        self.data = data
        self.acknowledged = False
        self._sending_time = time.time()

    def dump(self) -> bytes:
        seq = self.seq_number.to_bytes(8, "big", signed=False)
        ack = self.ack_number.to_bytes(8, "big", signed=False)
        return seq + ack + self.data

    @staticmethod
    def load(data: bytes) -> 'TCPSegment':
        seq = int.from_bytes(data[:8], "big", signed=False)
        ack = int.from_bytes(data[8:16], "big", signed=False)
        return TCPSegment(seq, ack, data[16:])

    def update_sending_time(self, sending_time=None):
        self._sending_time = sending_time if sending_time is not None else time.time()

    @property
    def expired(self):
        return not self.acknowledged and (time.time() - self._sending_time > self.ack_timeout)

    def __len__(self):
        return len(self.data)


class UDPBasedProtocol:
    def __init__(self, *, local_addr, remote_addr):
        self.udp_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
        self.remote_addr = remote_addr
        self.udp_socket.bind(local_addr)

    def sendto(self, data):
        return self.udp_socket.sendto(data, self.remote_addr)

    def recvfrom(self, n):
        msg, addr = self.udp_socket.recvfrom(n)
        return msg


class MyTCPProtocol(UDPBasedProtocol):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Hyperparameters
        self.mss = 1500
        self.window_size = self.mss * 12
        self.read_timeout = 2
        self.lag = 0
        self.critical_lag = 12
        # Internal buffers
        self._sent_bytes_n = 0
        self._confirmed_bytes_n = 0
        self._received_bytes_n = 0
        self._send_window = PriorityQueue()
        self._recv_window = PriorityQueue()
        self._buffer = bytes()

    def send(self, data: bytes) -> int:
        window_lock = (self._sent_bytes_n - self._confirmed_bytes_n > self.window_size)
        while data:
            if not window_lock:
                right_border = min(self.mss, len(data))
                sent_length = self._send_segment(TCPSegment(self._sent_bytes_n,
                                                            self._confirmed_bytes_n,
                                                            data[: right_border]))
                data = data[sent_length:]
            window_lock = (self._sent_bytes_n - self._confirmed_bytes_n > self.window_size)
            self._receive_segment(TCPSegment.ack_timeout if window_lock else 0.0)
            self._resend_earliest_segment()

        return self._sent_bytes_n

    def recv(self, n: int):
        right_border = min(n, len(self._buffer))
        data = self._buffer[:right_border]
        self._buffer = self._buffer[right_border:]
        while len(data) < n:
            self._receive_segment(self.read_timeout)
            right_border = min(n, len(self._buffer))
            data = self._buffer[:right_border]
            self._buffer = self._buffer[right_border:]
        return self.recvfrom(n)

    def _receive_segment(self, timeout: float = None) -> TCPSegment:
        self.udp_socket.settimeout(timeout)
        try:
            segment = TCPSegment.load(self.recvfrom(self.mss))
        except socket.error:
            segment = TCPSegment(-1, -1, bytes())

        if len(segment):
            self._recv_window.put((segment.seq_number, segment))
            self._shift_recv_window()
        if segment.ack_number > self._confirmed_bytes_n:
            self._confirmed_bytes_n = segment.ack_number
            self._shift_send_window()
        elif self.lag < self.critical_lag:
            self.lag += 1
        else:
            self.lag = 0
            self._resend_earliest_segment(force=True)

        return segment

    def _send_segment(self, segment: TCPSegment) -> int:
        """
        @return: длина отправленных данных
        """
        # В будущем обернуть в try: ... except socket.error: just_sent = 0
        self.udp_socket.settimeout(None)
        just_sent = self.sendto(segment.dump()) - segment.service_len
        self._sent_bytes_n += just_sent
        segment.data = segment.data[: just_sent]
        if len(segment):
            segment.update_sending_time()
            self._send_window.put((segment.seq_number, segment))
        return just_sent

    def _shift_recv_window(self):
        earliest_segment = None
        while not self._recv_window.empty():
            _, earliest_segment = self._recv_window.get(block=False)
            if earliest_segment.seq_number == self._received_bytes_n:
                self._buffer += earliest_segment.data
                self._received_bytes_n += len(earliest_segment)
                earliest_segment.acknowledged = True
            elif earliest_segment.seq_number > self._received_bytes_n:
                self._recv_window.put((earliest_segment.seq_number, earliest_segment))
                break

        if earliest_segment is not None and earliest_segment.acknowledged:
            self._send_segment(TCPSegment(self._sent_bytes_n, self._received_bytes_n, bytes()))

    def _shift_send_window(self):
        while not self._send_window.empty():
            _, earliest_segment = self._send_window.get(block=False)
            if earliest_segment.ack_number > self._confirmed_bytes_n:
                self._send_window.put((earliest_segment.seq_number, earliest_segment))
                break

    def _resend_earliest_segment(self, force=False):
        if self._send_window.empty():
            return
        _, earliest_segment = self._send_window.get(block=False)
        if earliest_segment.expired or force:
            self._send_segment(earliest_segment)
        else:
            self._send_window.put((earliest_segment.seq_number, earliest_segment))

