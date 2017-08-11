import asyncio
import errno
import logging
import os
import socket
import time

import yaml

import tsproxy.proxy
from tsproxy import common, streams, topendns


logger = logging.getLogger(__name__)


class Connector(object):

    def __init__(self, loop=None):
        self._loop = loop if loop else asyncio.get_event_loop()

    def connect(self, peer, target_host, target_port, proxy_name=None, loop=None, **kwargs):
        raise NotImplementedError()

    @staticmethod
    def _set_proxy_info(conn, addr, port, peer, proxy=None):
        proxyto = '%s:%d' % (addr, port)
        host = '/%s' % proxy.short_hostname if proxy else ''
        conn_name = '%d->%s%s' % (conn.lport, proxyto, host)
        # conn.connection_name = conn_name

        client = '%s:%d' % (peer.laddr, peer.lport)
        conn.proxy_info = '%s-(%s#%d)' % (conn_name, client, peer.fileno)
        peer.proxy_info = '%s->(%d-%s%s#%d)' % (client, conn.lport, proxyto, host, conn.fileno)
        conn.target_host = peer.target_host = addr

    @asyncio.coroutine
    def _connect(self, handler, peer, host, port, loop=None,
                 encoder=None, decoder=None, init_coro=None, connect_timeout=common.default_timeout, **kwargs):

        init_done = asyncio.Event()
        init_ex = None

        @asyncio.coroutine
        def _handler_wrapper(_conn):
            yield from init_done.wait()
            if init_ex is None:
                yield from handler(_conn, peer)

        kwargs.setdefault('local_dns', False)
        try:
            with common.Timeout(connect_timeout):
                connection = yield from streams.start_connection(_handler_wrapper, host=host, port=port, loop=loop,
                                                                 encoder=encoder, decoder=decoder, connect_timeout=connect_timeout, **kwargs)
                connection.set_attr(tsproxy.proxy.PEER_CONNECTION, peer)
                if init_coro:
                    res = init_coro(connection)
                    if asyncio.coroutines.iscoroutine(res):
                        yield from res
        except Exception as _init_ex:
            init_ex = _init_ex
            if isinstance(init_ex, socket.gaierror):
                raise socket.gaierror(common.errno_from_exception(init_ex), 'Dns query(%s) fail' % host) from init_ex
            else:
                raise
        finally:
            init_done.set()
        return connection


class DirectConnector(Connector):

    def __init__(self, loop=None):
        super().__init__(loop)
        self._proxy = tsproxy.proxy.DirectForward()

    @asyncio.coroutine
    def connect(self, peer, target_host, target_port, proxy_name=None, loop=None, **kwargs):
        # peer.set_attr(tsproxy.proxy.PROXY_NAME, 'D')
        connection = yield from super()._connect(self._proxy, peer, target_host, target_port, loop, local_dns=True, **kwargs)
        self._set_proxy_info(connection, target_host, target_port, peer)
        return connection


class ProxyConnector(Connector):

    def __init__(self, proxy_holder, loop=None):
        super().__init__(loop)
        self.proxy_holder = proxy_holder

    @asyncio.coroutine
    def _connect_proxy(self, proxy, peer, target_host, target_port, connect_timeout, loop=None, **kwargs):

        @asyncio.coroutine
        def _init_core(_conn):
            yield from proxy.init_connection(_conn, target_host, target_port, **kwargs)

        timeout = connect_timeout + time.time()
        proxy_host, proxy_port = proxy.addr
        peer.set_attr(tsproxy.proxy.PROXY_NAME, proxy.short_hostname)
        logger.debug('connecting to proxy(%s:%d) for (%s->%s:%d)', proxy_host, proxy_port, peer, target_host, target_port)
        for i in range(0, 2):
            try:
                proxy_conn = yield from super()._connect(proxy, peer, proxy_host, proxy_port, loop,
                                                         encoder=None if not hasattr(proxy, 'encoder') else proxy.encoder,
                                                         decoder=None if not hasattr(proxy, 'decoder') else proxy.decoder,
                                                         init_coro=_init_core, connect_timeout=connect_timeout, **kwargs)
                proxy.resolved_addr = (proxy_conn.raddr, proxy_conn.rport)
                self._set_proxy_info(proxy_conn, target_host, target_port, peer, proxy)
                return proxy_conn
            except socket.gaierror as ex:
                connect_timeout = timeout - time.time()
                # DNS解析失败，则用之前解析的IP地址进行一次尝试
                if i == 0 and proxy.resolved_addr and connect_timeout > 0:
                    proxy_host, proxy_port = proxy.resolved_addr
                    logger.debug('%s, use resolved address(%s) try again...', ex, proxy_host)
                    continue
                else:
                    raise

    @asyncio.coroutine
    def connect(self, peer, target_host, target_port, proxy_name=None, loop=None, **kwargs):
        timeout = time.time() + common.default_timeout
        proxy_count = self.proxy_holder.psize
        if proxy_count <= 0:
            raise Exception('NO FOUND PROXY CONFIG')
        connect_ex = None
        for i in range(0, proxy_count):
            left_time = timeout - time.time()
            if proxy_name is not None:
                if i > 0:
                    break
                proxy, _ = self.proxy_holder.find_proxy(proxy_name)
                if proxy is None:
                    raise Exception('NOT FOUND PROXY: %s' % proxy_name)
            else:
                proxy = self.proxy_holder.head_proxy
                if (proxy_count - i) > 1:
                    # 每次的超时时间留一半给下一个proxy进行尝试
                    left_time /= 2
            if left_time <= 0:
                break
            elif left_time < 1:
                left_time = 1
            try:
                proxy_conn = yield from self._connect_proxy(proxy, peer, target_host, target_port, left_time, loop, **kwargs)
                proxy_conn.set_attr('Proxy-Name', proxy_name)
                proxy.error_count = 0
                return proxy_conn
            except BaseException as ex1:
                connect_ex = ex1
                logger.debug("connect to proxy(%s:%d) for (%s, %s, %s) %s: %s",
                             proxy.hostname, proxy.port, peer, target_host, target_port, ex1.__class__.__name__, ex1)
                # move the head to tail
                # if proxy_name is not None:
                used = time.time()-timeout+common.default_timeout
                err_no = common.errno_from_exception(ex1)
                if err_no not in (errno.ENETDOWN, errno.ENETRESET, errno.ENETUNREACH):
                    if proxy_name is not None \
                            or self.proxy_holder.move_head_to_tail(proxy, logging.WARNING, 'connect %s: %s', ex1.__class__.__name__, ex1) :
                        proxy.error_time = time.time()
                        proxy.error_count += 1
                        proxy.update_stat_info(used, proxy_fail=True, proxy_name=proxy_name)
                        if proxy_name is None:
                            self.proxy_holder.check(proxy, '%s: %s' % (ex1.__class__.__name__, ex1))
                else:
                    break
        used = time.time() - timeout + common.default_timeout
        if time.time() >= timeout:
            logger.info("connect to proxy for %s timeout(%.1f)", peer, used)
        else:
            logger.info("can't connect to proxy%s for %s used %.1f sec", "(%s)" % proxy_name if proxy_name else '', peer, used)
        if connect_ex:
            err_no = common.errno_from_exception(connect_ex)
            if isinstance(connect_ex, OSError) \
                    and (err_no in (errno.ENETDOWN, errno.ENETRESET, errno.ENETUNREACH) or isinstance(connect_ex, socket.gaierror)):
                raise ConnectionError(err_no, connect_ex.strerror) from connect_ex
            else:
                raise connect_ex
        else:
            raise Exception("connect to proxy fail")


class SmartConnector(Connector):

    def __init__(self, proxy_holder=None, smart_mode=1, loop=None):
        ''' smart_mode:
              0: direct connect
              1: smart(director connect if china ip else use proxy)
              2: proxy always
        '''
        super().__init__(loop)
        self.proxy_holder = proxy_holder
        self.smart_mode = smart_mode if proxy_holder else 0
        if self.smart_mode <= 1:
            self.direct_connector = DirectConnector(loop)
        if self.smart_mode >= 1:
            self.proxy_connector = ProxyConnector(self.proxy_holder, loop)

    def connect(self, peer, target_host, target_port, proxy_name=None, loop=None, **kwargs):
        connector = None
        if self.smart_mode <= 0:
            connector = self.direct_connector
        elif self.smart_mode >= 2 or proxy_name is not None:
            connector = self.proxy_connector
        else:
            atype = 0x01 if topendns.is_ipv4(target_host) else 0x04 if topendns.is_ipv6(target_host) else 0x03
            if atype == 0x03:  # domain
                if topendns.is_foreign_domain(target_host):
                    connector = self.proxy_connector
                elif topendns.is_cn_domain(target_host):
                    connector = self.direct_connector
            if connector is None:
                if atype == 0x03:
                    ip = yield from topendns.async_dns_query(target_host, loop=self._loop)
                else:
                    ip = target_host
                if ip is not None and topendns.is_cn_ip(0x01 if topendns.is_ipv4(ip) else 0x04, ip):
                    connector = self.direct_connector
                else:
                    connector = self.proxy_connector
        return (yield from connector.connect(peer, target_host, target_port, proxy_name, loop, **kwargs))


class RouterableConnector(SmartConnector):
    '''
    router_conf: router.yaml
        default: jp.f
        match_con1:
            url: http://www.baidu.com/abc
            protocol: http
            host: www.baidu.com
            port:
                - 80
                - 443
            path: abc
            method:
                - GET
                - POST
            app:
                - Microsoft PowerPoint
                - QQ
            User-Agent: netdisk
        router:
            - match_con1: jp.a
            - match_con2: jp.a
    '''

    def __init__(self, proxy_holder=None, smart_mode=1, loop=None, router_conf='router.yaml', **kwargs):
        super().__init__(proxy_holder, smart_mode, loop)
        self.yaml_conf_file = common.lookup_conf_file(router_conf)
        self.conf_update_time = 0
        self.yaml_conf_mod = 0
        self.yaml_conf = {}
        self.load_yaml_conf()

    def connect(self, peer, target_host, target_port, proxy_name=None, loop=None, **kwargs):
        request = kwargs['request'] if 'request' in kwargs else None
        if proxy_name is None and request is not None:
            proxy_name = self.get_proxy_name(request, peer)
            if proxy_name is not None:
                if proxy_name == 'F':
                    yield from asyncio.sleep(5)
                    raise ConnectionError('match forbidden access %s' % request.request_line)
                elif proxy_name == 'P':
                    return (yield from self.proxy_connector.connect(peer, target_host, target_port, loop=loop, **kwargs))
                elif proxy_name == 'D':
                    return (yield from self.direct_connector.connect(peer, target_host, target_port, loop=loop, **kwargs))
        return (yield from super().connect(peer, target_host, target_port, proxy_name=proxy_name, loop=loop, **kwargs))

    def load_yaml_conf(self):
        if (time.time() - self.conf_update_time) < 1:
            return
        self.conf_update_time = time.time()

        try:
            mtime = os.stat(self.yaml_conf_file).st_mtime
            if self.yaml_conf_mod < mtime:
                if self.yaml_conf_mod >= mtime:
                    return
                self.yaml_conf_mod = mtime
                with open(self.yaml_conf_file, 'r') as f:
                    _conf = yaml.load(f)
                _conf.setdefault('router', [])
                if not self.check_yaml_conf(_conf):
                    logger.warning('%s check FAIL!', self.yaml_conf_file)
                    return
                self.yaml_conf = _conf
                logger.info('%s reloaded', self.yaml_conf_file)
        except BaseException as e:
            self.conf_update_time = time.time() + 10 * 60

    def check_yaml_conf(self, conf):
        ok = True
        for k in conf:
            v = conf[k]
            if 'default' == k:
                p, i = self.proxy_holder.find_proxy(v)
                if p is None and v not in ('D', 'P', 'F'):
                    logger.warning('default proxy: %s NOT found', v)
                    ok = False
            elif 'router' != k:
                for con in v:
                    if con not in ('url', 'protocol', 'host', 'port', 'path', 'method', 'app'):
                        logger.warning('headers condition: %s', con)
                        # ok = False
        for _r in conf['router']:
            for _con in _r:
                _to = _r[_con]
                if _con not in conf:
                    logger.warning('condition: %s NOT found', _con)
                    ok = False
                p, i = self.proxy_holder.find_proxy(_to)
                if p is None and _to not in ('D', 'P', 'F'):
                    logger.warning('condition(%s) to proxy: %s NOT found', _con, _to)
                    ok = False
        return ok

    def get_proxy_name(self, request, connection):
        self.load_yaml_conf()
        for _r in self.yaml_conf['router']:
            for _con_name in _r:
                _to = _r[_con_name]
                if _con_name not in self.yaml_conf:
                    continue
                _con = self.yaml_conf[_con_name]
                _con_match = True
                for k in _con:
                    v = _con[k]
                    if isinstance(v, list):
                        _m = False
                        for _v in v:
                            _m = self._match(k, _v, request, connection)
                            if (_m and _v[0] != '!') \
                                    or (not _m and _v[0] == '!'):
                                break
                    else:
                        _m = self._match(k, v, request, connection)
                    if not _m:
                        _con_match = False
                        break
                if _con_match:
                    logger.debug('match router: %s', _con_name)
                    return _to
        return self.yaml_conf['default'] if 'default' in self.yaml_conf else None

    def _match(self, k, v, request, connection):
        if v[0] == '!':
            return self.__match(k, v[1:], request, connection, True)
        else:
            return self.__match(k, v, request, connection)

    def __match(self, k, v, request, connection, rev=False):
        _m = False
        if k == 'url':
            _m = request.url.full_url.lower().startswith(v.lower())
        elif k == 'protocol':
            if v.upper() == 'HTTPS':
                _m = request.method == 'CONNECT'
            elif v.upper() == 'HTTP':
                _m = request.url.scheme.lower() == v.lower()
        elif k == 'host':
            _m = request.url.hostname.lower().endswith(v.lower())
        elif k == 'port':
            v = v if isinstance(v, int) else int(v)
            _m = request.url.port == v
        elif k == 'path':
            _m = request.url.path.lower().startswith(v.lower())
        elif k == 'method':
            _m = request.method.lower() == v.lower()
        elif k == 'app':
            if 'process_name' in connection:
                _m = connection['process_name'] == v
        else:
            _exists = k in request.headers
            if not _exists:
                _m = False
            else:
                _m = v.lower() in request.headers[k].lower()
        return _m if not rev else not _m


class CheckConnector(ProxyConnector):

    def __init__(self, proxy_holder=None, loop=None):
        super().__init__(proxy_holder=proxy_holder, loop=loop)

    def connect(self, peer, target_host, target_port, proxy_name=None, loop=None, **kwargs):
        logger.debug("speed_testing_proxy=%s", self.proxy_holder.testing_proxy)
        return (yield from super().connect(peer, target_host, target_port, self.proxy_holder.testing_proxy, loop, **kwargs))
