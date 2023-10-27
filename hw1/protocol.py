import socket
import logging
from queue import PriorityQueue

logger = logging.getLogger(__name__)


class TCPSegment:
    service_len = 8 + 8 + 1

    def __init__(self, seq_number: int, ack_number: int, data: bytes):
        # TODO: ввести время создания сегмента и проверку истечения срока годности
        self.seq_number = seq_number
        self.ack_number = ack_number
        self.data = data

    def dump(self) -> bytes:
        seq = self.seq_number.to_bytes(8, "big", signed=False)
        ack = self.ack_number.to_bytes(8, "big", signed=False)
        return seq + ack + self.data

    @staticmethod
    def load(data: bytes) -> 'TCPSegment':
        seq = int.from_bytes(data[:8], "big", signed=False)
        ack = int.from_bytes(data[8:16], "big", signed=False)
        return TCPSegment(seq, ack, data[16:])

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
        self.read_timeout = 10
        self.ack_timeout = 0.05
        # Internal buffers
        self._sent_bytes_n = 0
        self._confirmed_bytes_n = 0
        self._received_bytes = 0
        self._send_window = PriorityQueue()
        self._recv_window = PriorityQueue()
        self._buffer = bytes()

    def send(self, data: bytes) -> int:
        while data:
            right_border = min(self.mss, len(data))
            sent_length = self.send_segment(TCPSegment(self._sent_bytes_n,
                                                       self._confirmed_bytes_n,
                                                       data[: right_border]))
            data = data[sent_length:]
            is_blocking = (self._sent_bytes_n - self._confirmed_bytes_n > self.window_size)
            ack_datagram = self.receive_segment(self.ack_timeout if is_blocking else 0.0)
            # TODO Сдвинуть окно и настроить получение датаграммы
        return self._sent_bytes_n

    def recv(self, n: int):
        return self.recvfrom(n)

    def receive_segment(self, timeout: float = None) -> TCPSegment:
        self.udp_socket.settimeout(timeout)
        try:
            datagram = TCPSegment.load(self.recvfrom(self.mss))
            if len(datagram):
                self._recv_window.put((datagram.seq_number, datagram))
        except socket.error:
            datagram = TCPSegment(-1, -1, bytes())

        while len(datagram):
            newest_segment = self._recv_window.get()
            if newest_segment.seq_number == self._received_bytes:
                self._buffer += newest_segment.data
                self._received_bytes += len(newest_segment)
                self.send_segment(TCPSegment(self._sent_bytes_n, self._received_bytes, bytes()))
            elif newest_segment.seq_number > self._received_bytes:
                self._recv_window.put(newest_segment)
                break

        # TODO: сдвиг окна отправленных данных
        return datagram

    def send_segment(self, datagram: TCPSegment):
        # В будущем обернуть в try: ... except socket.error: just_sent = 0
        self.udp_socket.settimeout(None)
        just_sent = self.sendto(datagram.dump()) - datagram.service_len
        self._sent_bytes_n += just_sent
        datagram.data = datagram.data[: just_sent]
        if len(datagram):
            self._send_window.put((datagram.seq_number, datagram))
        return just_sent
