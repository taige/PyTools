import asyncio
import logging
import socket
import time
from concurrent.futures import CancelledError

from tsproxy import common, topendns

logger = logging.getLogger(__name__)


async def start_listener(handler, host=None, port=None, *, loop=None, encoder=None, decoder=None, acl_ips=None, ssl=None, **kwargs):
    if loop is None:
        loop = asyncio.get_event_loop()

    def factory():
        _protocol = StreamProtocol(handler, loop=loop, encoder=encoder, decoder=decoder, acl_ips=acl_ips, **kwargs)
        return _protocol

    return await loop.create_server(factory, host, port, backlog=1024, ssl=ssl)


def start_connection(handler, ip, port, host=None, *, loop=None, encoder=None, decoder=None, connect_timeout=common.default_timeout, **kwargs):
    if loop is None:
        loop = asyncio.get_event_loop()
    if host is None:
        host = ip

    create_time = time.time()

    try:
        if topendns.is_ipv6(ip):
            sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        else:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setblocking(False)
        logger.debug('connecting (%s/%s:%d) ...', host, ip, port)
        with common.Timeout(connect_timeout):
            yield from loop.sock_connect(sock, (ip, port))
        logger.debug('connected (%s/%s:%d) used %.3f seconds', host, ip, port, (time.time() - create_time))
    except BaseException as ex:
        if isinstance(ex, ConnectionError) \
                or isinstance(ex, asyncio.TimeoutError) or isinstance(ex, TimeoutError):
            topendns.del_cache(host)
        raise

    def factory():
        _protocol = StreamProtocol(handler, loop=loop, create_time=create_time, encoder=encoder, decoder=decoder, **kwargs)
        return _protocol

    _, protocol = yield from loop.create_connection(factory, sock=sock)

    if protocol.connection is None:
        if hasattr(protocol, 'init_exception') and protocol.init_exception is not None:
            raise protocol.init_exception
        else:
            raise ConnectionError('connect to %s/%s:%d failed' % (host, ip, port))
    return protocol.connection


def _close_sock_slient(sock):
    try:
        sock.close()
    except BaseException:
        pass


class StreamProtocol(asyncio.StreamReaderProtocol):

    _connection_counter = 0

    def __init__(self, handler, loop=None, create_time=None, encoder=None, decoder=None, acl_ips=None, **kwargs):
        super().__init__(StreamReader(decoder=decoder, loop=loop), client_connected_cb=handler, loop=loop)
        self._create_time = create_time
        self._connection = None
        self._encoder = encoder
        self._acl_ips = acl_ips
        self._close_waiter = None
        self.init_exception = None

    def connection_made(self, transport):
        try:
            self._connection_made(transport)
        except BaseException as ex:
            self.init_exception = ex
            raise

    def _connection_made(self, transport):
        _socket = transport.get_extra_info('socket')
        if _socket is None:
            raise Exception("socket is None")
        if self._acl_ips is not None and self._create_time is None:
            try:
                _client = _socket.getpeername()
                _server = _socket.getsockname()
            except Exception as ex1:
                logger.warning("handle socket %s %s: %s", _socket, common.clazz_fullname(ex1), ex1)
                # _close_sock_slient(_socket)
                return
            laddr = _client[0]
            raddr = _server[0]
            if laddr != '127.0.0.1' and laddr != raddr and not topendns.is_subnet(laddr, self._acl_ips):
                raise Exception("%s NOT in ACLs" % laddr)
                # logger.warning("%s NOT in ACLs" % laddr)
                # _close_sock_slient(_socket)
                # self.eof_received()
                # return

        self._stream_reader.set_transport(transport)
        self._over_ssl = transport.get_extra_info('sslcontext') is not None
        self._stream_writer = StreamWriter(transport, self,
                                           self._stream_reader,
                                           self._loop,
                                           encoder=self._encoder)
        self._connection = StreamConnection(self._stream_reader, self._stream_writer, _socket, self._create_time)
        if self._client_connected_cb is not None:
            res = self._client_connected_cb(self._connection)
            if asyncio.coroutines.iscoroutine(res):
                def wrapper():
                    try:
                        yield from res
                    except ConnectionError as ex:
                        logger.debug("handle connect %s %s: %s", self._connection, common.clazz_fullname(ex), ex)
                    except OSError as ex:
                        logger.info("handle connect %s %s: %s", self._connection, common.clazz_fullname(ex), ex)
                    except BaseException as ex:
                        logger.exception("handle connect %s %s: %s", self._connection, common.clazz_fullname(ex), ex)
                    finally:
                        try:
                            if self._connection.reader.exception() is None and not self._connection_lost \
                                    and (self._drain_waiter is None or self._drain_waiter.cancelled()):
                                yield from self._connection.writer.drain()
                            self._connection.writer.close()
                        except ConnectionError as ex:
                            logger.exception("handle connect %s %s: %s", self._connection, common.clazz_fullname(ex), ex)
                        # logger.info('%s lived %.2f seconds (%d)', self._connection, self._connection.life_time, StreamProtocol._connection_counter)
                self._loop.create_task(wrapper())
        StreamProtocol._connection_counter += 1
        logger.debug('connection_made#%d: %s%s', StreamProtocol._connection_counter, self._connection,
                     ', used %.3f sec' % (time.time()-self._create_time) if self._create_time is not None else '')

    def connection_lost(self, exc):
        if self._connection is not None:
            StreamProtocol._connection_counter -= 1
            logger.debug('connection_lost#%d: %s lived %.2f seconds', StreamProtocol._connection_counter, self._connection, self._connection.life_time)
        super().connection_lost(exc)
        waiter = self._close_waiter
        if waiter is None:
            return
        self._close_waiter = None
        if waiter.done():
            return
        if exc is None:
            waiter.set_result(None)
        else:
            waiter.set_exception(exc)

    def data_received(self, data):
        _log_data('%s received' % self.connection, data)
        super().data_received(data)

    def eof_received(self):
        buf_len = len(self._stream_reader._buffer)
        r = super().eof_received()
        logger.debug('eof_received: %s with buffer(%d)', self.connection, buf_len)
        return r

    def pause_writing(self):
        super().pause_writing()
        logger.debug('pause_writing: %s', self.connection)

    def resume_writing(self):
        super().resume_writing()
        logger.debug('resume_writing: %s', self.connection)

    @property
    def connection(self):
        return self._connection

    @asyncio.coroutine
    def _close_helper(self):
        if self._connection_lost:
            return
        waiter = self._close_waiter
        assert waiter is None or waiter.cancelled()
        waiter = self._loop.create_future()
        self._close_waiter = waiter
        yield from waiter


class StreamConnection(dict):

    def __init__(self, reader, writer, sock, create_time=None, **kwargs):
        super().__init__(**kwargs)
        self._reader = reader
        self._writer = writer
        self._sock = sock
        self._sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
        if create_time is None:
            # accepted connection
            _client = self._sock.getpeername()
            _server = self._sock.getsockname()
        else:
            _client = self._sock.getsockname()
            _server = self._sock.getpeername()
        self.family = self._sock.family
        self.laddr = _client[0]
        self.lport = _client[1]  # client
        self.raddr = _server[0]
        self.rport = _server[1]  # server
        self.create_time = time.time() if create_time is None else create_time
        self.connected_time = time.time()
        self._address_info = '%s:%d->%s:%d' % (self.laddr, self.lport, self.raddr, self.rport)
        self.response_timeout = False
        self._reader._connection = self._writer._connection = self
        self._fileno = self._sock.fileno()
        self._closing = False
        # self._active_time = time.time()
        self.proxy_info = None
        self.target_host = None

    def __repr__(self):
        s = "[conn#%i %s]" \
            % (self.fileno,
               self._address_info if self.proxy_info is None else self.proxy_info)
        return s

    @property
    def active_time(self):
        return max(self._reader._active_time, self._writer._active_time)

    @property
    def read_time(self):
        return self._reader._active_time

    @property
    def written_time(self):
        return self._writer._active_time

    @property
    def address_info(self):
        s = "[conn#%i %s]" \
            % (self.fileno, self._address_info)
        return s

    @property
    def life_time(self):
        return time.time() - self.connected_time

    @property
    def idle_time(self):
        return time.time() - self.active_time

    @property
    def fileno(self):
        return self._fileno

    @property
    def writer(self):
        return self._writer

    @property
    def reader(self):
        return self._reader

    def get_attr(self, key, default=None):
        return self[key] if key in self else default

    def set_attr(self, key, value=None):
        if value is None and key in self:
            del self[key]
        elif value is not None:
            self[key] = value
        return value

    def close(self):
        if self._closing:
            return
        self._closing = True
        if not self._reader.at_eof():
            self._reader.feed_eof_by_close()

    @asyncio.coroutine
    def aclose(self):
        self.close()
        yield
        yield from self._writer._protocol._close_helper()

    @property
    def is_closing(self):
        return self._closing or self._writer.is_closing


class StreamReader(asyncio.StreamReader):

    def __init__(self, decoder=None, loop=None):
        super().__init__(loop=loop)
        self._decoder = decoder
        self._connection = None
        self._recv_bytes = 0
        self._active_time = time.time()

    def __len__(self):
        return len(self._buffer)

    @property
    def recv_bytes(self):
        return self._recv_bytes

    def feed_data(self, data):
        if not self._eof:
            super().feed_data(data)
            self._recv_bytes += len(data)
            self._active_time = time.time()
        else:
            logger.debug('feed_data after feed_eof is NOT allow')

    def read_bytes(self, size=None, read_timeout=common.default_timeout, exactly=False) -> bytes:
        if size is None or size <= 0:
            l = len(self)
            _size = l if l > 0 else self._limit
        else:
            _size = size
        try:
            with common.Timeout(read_timeout, loop=self._loop):
                if exactly:
                    r = yield from super().readexactly(_size)
                else:
                    r = yield from super().read(_size)
                return r
        except CancelledError:
            return None
        # except TimeoutError as ex1:
        #     logger.debug("%s read_bytes(%s %d %s) %s: %s", self._connection, size, read_timeout, exactly, ex1.__class__.__name__, ex1)
        #     return None
        except ConnectionError as ex1:
            logger.debug("%s read_bytes(%s %d %s) %s: %s", self._connection, size, read_timeout, exactly, common.clazz_fullname(ex1), ex1)
            return None

    def read(self, n=None, read_timeout=common.default_timeout) -> bytes:
        if self._decoder:
            r = yield from self._decoder(self._connection, read_timeout)
        else:
            r = yield from self.read_bytes(size=n, read_timeout=read_timeout)
        return r

    def feed_eof_by_close(self):
        logger.debug('%s feed_eof_by_close with buffer(%d)', self._connection, len(self._buffer))
        super().feed_eof()

    def pause_reading(self):
        self._transport.pause_reading()

    def resume_reading(self):
        self._transport.resume_reading()


class StreamWriter(asyncio.StreamWriter):

    def __init__(self, transport, protocol, reader, loop, encoder=None):
        super().__init__(transport, protocol, reader, loop)
        self._encoder = encoder
        self._connection = None
        self._written_bytes = 0
        self._active_time = time.time()

    def write(self, data):
        if self._encoder:
            data = self._encoder(data, self._connection)
        super().write(data)
        _log_data('%s written' % self._connection, data)
        self._written_bytes += len(data)
        self._active_time = time.time()

    @property
    def written_bytes(self):
        return self._written_bytes

    @property
    def is_closing(self):
        return self._transport.is_closing()


def _log_data(log_info, data, loglevel=5, max_len=200):
    if len(data) > max_len:
        logger.log(loglevel, "%s [%s...%s %d]", log_info, data[:max_len-20], data[len(data)-20:], len(data))
    else:
        logger.log(loglevel, "%s [%s]", log_info, data)


class Encoder:

    def __call__(self, data, connection):
        raise NotImplementedError()


class Decoder:

    def __call__(self, connection, read_timeout):
        raise NotImplementedError()
