import socket


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
        self.mtu = 1500
        self.window_size = self.mtu * 10

    def send(self, data: bytes):
        sent_bytes = 0
        for i in range(self.mtu, len(data), self.mtu):
            sent_bytes += self.sendto(data[:i])
            if i != len(data):
                data = data[i:]
            else:
                data = bytes()
            # TODO: check receiving message if windows size is achieved
        if data:
            sent_bytes += self.sendto(data)
        return sent_bytes

    def recv(self, n: int):
        return self.recvfrom(n)

