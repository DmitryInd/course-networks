import socket
import logging
import math
import errno


logger = logging.getLogger(__name__)


class Datagram:
    def __init__(self, seq_number: int, ack_number: int, data: bytes):
        self.seq_number = seq_number
        self.ack_number = ack_number
        self.data = data

    def serialise(self) -> bytes:
        seq = self.seq_number.to_bytes(8, "big", signed=False)
        ack = self.ack_number.to_bytes(8, "big", signed=False)
        return seq + ack + self.data

    @staticmethod
    def load(data: bytes) -> 'Datagram':
        seq = int.from_bytes(data[:8], "big", signed=False)
        ack = int.from_bytes(data[8:16], "big", signed=False)
        return Datagram(seq, ack, data[16:])
    
    def __len__(self):
        return len(self.data)


class UDPBasedProtocol:
    def __init__(self, *, local_addr, remote_addr):
        self.udp_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
        self.remote_addr = remote_addr
        self.udp_socket.bind(local_addr)

    def sendto(self, data):
        return self.udp_socket.sendto(data, self.remote_addr)

    def recvfrom(self, n, blocking=True):
        self.udp_socket.setblocking(blocking)
        try:
            msg, addr = self.udp_socket.recvfrom(n)
        except socket.error as e:
            err = e.args[0]
            if err != errno.EAGAIN and err != errno.EWOULDBLOCK:
                raise e
            else:
                msg = bytes()
        self.udp_socket.setblocking(True)
        return msg


class MyTCPProtocol(UDPBasedProtocol):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mss = 1500
        self.window_size = self.mss * 12
        self.send_buffer = []
        self.recv_buffer = []

    def send(self, data: bytes) -> int:
        sent_bytes = 0
        confirmed_bytes = 0
        for i in range(math.ceil(len(data)/self.mss)):
            left_border, right_border = i*self.mss, min((i+1)*self.mss, len(data))
            self.send_buffer.append(Datagram(sent_bytes, confirmed_bytes, data[left_border: right_border]))
            sent_bytes += len(self.send_buffer[-1])
            self.sendto(self.send_buffer[-1].serialise())
            is_blocking = (sent_bytes - confirmed_bytes > self.window_size)
            ack_datagram = self.recvfrom(self.mss, is_blocking)
            # TODO Сдвинуть окно и настроить получение датаграмы
        return sent_bytes

    def recv(self, n: int):
        return self.recvfrom(n)
    
    async def receive_datagram(self) -> Datagram:
        return Datagram.load(self.recvfrom(self.mss))

