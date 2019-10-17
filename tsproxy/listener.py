import argparse
import asyncio
import concurrent.futures
import logging
import socket
import time
import psutil
import os
import platform
from io import BytesIO
from io import StringIO

import tsproxy.proxy
from tsproxy import httphelper2 as httphelper
from tsproxy import common, proxy, streams, topendns, str_datetime, __version__


HTTP_REQUEST = 'listener.HTTP_REQUEST'

HTTP_RESPONSE = 'listener.HTTP_RESPONSE'
HTTP_RESPONSE_RAW_DATA = 'listener.HTTP_RESPONSE_RAW_DATA'

HTTP_REQUEST_LENGTH = 'listener.HTTP_REQUEST_LENGTH'
HTTP_RESPONSE_LENGTH = 'listener.HTTP_RESPONSE_LENGTH'
HTTP_RESPONSE_CONTENT_LENGTH = 'listener.HTTP_RESPONSE_CONTENT_LENGTH'

logger = logging.getLogger(__name__)
common_logger = logging.getLogger('http_common_log')


class Listener:

    def __init__(self, listen_addr, name='tcp', loop=None, **kwargs):
        self.listen_address = listen_addr
        self.name = name
        self.loop = loop if loop is not None else asyncio.get_event_loop()
        self._acl = set()
        self.kwargs = kwargs

    async def start(self):
        _server = await streams.start_listener(self, host=self.listen_address[0], port=self.listen_address[1], loop=self.loop, acl_ips=self._acl, **self.kwargs)
        logger.info('%s://%s listen at %s:%d', self.name, self.__class__.__name__, self.listen_address[0], self.listen_address[1])
        return _server

    def load_acl(self, j):
        if 'acl' in j:
            for ip, mask in j['acl']:
                self._acl.add((ip, mask))

    def dump_acl(self, j):
        j.update({
            'acl': [*self._acl]
        })

    def acl_op(self, ipv4='10.0.0.*', delete=False):
        starting_ip, imask = topendns.subnet_to_ipmask(ipv4)
        if starting_ip is None or imask is None:
            return False
        if not delete:
            self._acl.add((starting_ip, imask))
        else:
            try:
                self._acl.remove((starting_ip, imask))
            except KeyError:
                return False
        return True


class HttpListener(Listener):

    def __init__(self, listen_addr, connector, loop=None, **kwargs):
        import psutil
        kwargs.setdefault('name', 'http')
        super().__init__(listen_addr, loop=loop,
                         decoder=HttpRequestDecoder(), encoder=HttpResponseEncoder(), **kwargs)
        self.connector = connector
        self.connections = {}
        self._processes = common.FIFOCache(cache_timeout=60, lru=True)
        self._pid = psutil.Process().pid
        self._root_access_deny = 0
        self._root_access_deny_time = 0
        self._get_connection_process_macos_count = 0

    def __call__(self, connection):
        try:
            self.connections[connection.fileno] = connection
            self.get_connection_process(connection)
            while True:
                next_forward = yield from self.do_forward(connection)
                if not next_forward:
                    break
                yield
        finally:
            http_common_log(connection, mark='.')
            del self.connections[connection.fileno]

    def on_no_forwardhost(self, connection, request):
        pass

    def get_proxy_name(self, request):
        if 'Proxy-Name' in request.headers:
            proxy_name = request.headers['Proxy-Name']
        else:
            proxy_name = None
        return proxy_name

    def _get_connection_process_macos(self, connection):
        import subprocess  # -sTCP:ESTABLISHED
        p = subprocess.Popen('lsof -nP -iTCP:%d' % connection.lport, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        line_num = 0
        pids = []
        for line in p.stdout.readlines():
            if isinstance(line, bytes):
                line = line.decode().strip()
            line_num += 1
            logger.debug('_get_connection_process_macos(:%d) lsof output[%d]: %s', connection.lport, line_num, line)
            if line_num == 1:
                continue
            tmp = line.split()
            if len(tmp) < 2:
                continue
            pid = int(tmp[1])
            pids.append(pid)
        for pid in sorted(pids, key=lambda _pid: 0 if _pid == self._pid else _pid, reverse=True):
            if pid in self._processes:
                proc = self._processes[pid]
            else:
                proc = psutil.Process(pid)
                self._processes[pid] = proc
            connection['process_pid'] = proc.pid
            connection['process_name'] = proc.name()
            return True
        logger.info('_get_connection_process_macos(:%d) did\'t found process', connection.lport)
        return False

    def get_connection_process(self, connection):
        if self._root_access_deny >= 10 and (time.time() - self._root_access_deny_time) < 60:
            return
        try:
            if platform.system() == 'Darwin' and self._get_connection_process_macos_count > 10 and (time.time() - self._root_access_deny_time) < 60 \
                    and self._get_connection_process_macos(connection):
                return
            for c in psutil.net_connections(kind='tcp'):
                self._root_access_deny = 0
                self._get_connection_process_macos_count = 0
                if connection.lport == c.laddr[1] and c.pid and c.pid != self._pid:
                    ip1 = int.from_bytes(socket.inet_pton(c.family, c.laddr[0]), byteorder='big')
                    ip2 = int.from_bytes(socket.inet_pton(connection.family, connection.laddr), byteorder='big')
                    if ip1 == ip2:
                        if c.pid in self._processes:
                            proc = self._processes[c.pid]
                        else:
                            proc = psutil.Process(c.pid)
                            self._processes[c.pid] = proc
                        connection['process_pid'] = proc.pid
                        connection['process_name'] = proc.name()
                        return
        except psutil.AccessDenied:
            logger.warning('psutil ROOT access denied #%d(PID:%d) for :%d', self._root_access_deny, os.getpid(), connection.lport, stack_info=True)
            if self._root_access_deny == 0:
                self._root_access_deny_time = time.time()
            if platform.system() == 'Darwin' and self._get_connection_process_macos(connection):
                self._get_connection_process_macos_count += 1
                logger.warning('psutil need\'t ROOT access (PID:%d) for :%d  %s[PID:%d]', os.getpid(), connection.lport, connection['process_name'], connection['process_pid'])
                return
            self._root_access_deny += 1
        except BaseException as ex:
            logger.exception("%s get_connection_process fail: %s(%s)", connection, common.clazz_fullname(ex), ex)

    def do_forward(self, connection):
        if HTTP_REQUEST in connection:
            head_request = connection.get_attr(HTTP_REQUEST)
        else:
            try:
                head_request = yield from connection.reader.read()
            except Exception as ex:
                logger.info("%s head request fail: %s(%s)", connection, common.clazz_fullname(ex), ex)
                return False

        if not head_request:
            logger.info("%s head request is None", connection)
            return False
        elif head_request.error:
            return False

        logger.info('%s handling "%s"' % (connection, head_request.request_line))
        for k in head_request.headers:
            logger.log(5, '%s "%s: %s"', connection, k, head_request.headers[k])

        host = head_request.url.hostname
        port = head_request.url.port
        if not host:
            # it's not a proxy request, but http-proxy healthy detective
            yield from self.on_no_forwardhost(connection, head_request)
            return False

        proxy_name = self.get_proxy_name(head_request)

        try:
            peer_conn = yield from self.connector.connect(peer=connection, target_host=host, target_port=port, proxy_name=proxy_name, loop=self.loop, request=head_request)
        except concurrent.futures.CancelledError:
            connection.set_attr(HTTP_RESPONSE, httphelper.http_response(head_request.version, 500, 'Proxy is close(TSP)'))
            return False
        except (TimeoutError, asyncio.TimeoutError):
            connection.set_attr(HTTP_RESPONSE, httphelper.http_response(head_request.version, 503, 'Connect proxy timeout(TSP)'))
            return False
        except socket.gaierror as ex:
            logger.debug("%s connector.connect to %s:%d fail: %s(%s)", connection, host, port, common.clazz_fullname(ex), ex)
            connection.set_attr(HTTP_RESPONSE, httphelper.http_response(head_request.version, 503, 'Dns(%s) fail(TSP)' % host))
            return False
        except ConnectionError as ex:
            logger.info("%s connector.connect to %s:%d fail: %s(%s)", connection, host, port, common.clazz_fullname(ex), ex)
            connection.set_attr(HTTP_RESPONSE, httphelper.http_response(head_request.version, 503, '%s(TSP)' % ex.strerror))
            return False
        except BaseException as ex:
            logger.exception("%s connector.connect to %s:%d fail: %s(%s)", connection, host, port, common.clazz_fullname(ex), ex)
            connection.set_attr(HTTP_RESPONSE, httphelper.http_response(head_request.version, 503))
            return False

        connection.set_attr(proxy.PEER_CONNECTION, peer_conn)

        if head_request.method == common.HTTPS_METHOD_CONNECT:
            logger.info("https-proxy(%s) HANDLING '%s'", connection, head_request.request_line)
            yield from self.do_https_forward(head_request, connection, peer_conn)
            logger.info("https-proxy(%s) DONE", connection)
            peer_conn.close()
            return False

        while True:
            logger.info("http-proxy(%s) HANDLING '%s'", connection, head_request.request_line)
            # handle http forward
            next_request = yield from self.do_http_forward(head_request, connection, peer_conn)
            if next_request:
                if HTTP_RESPONSE in connection:
                    # 已经有过响应，说明是同一连接上的一个新请求
                    http_common_log(connection, request=head_request, mark=',')
                # connection.set_attr(HTTP_REQUEST, next_request)
                # has next http request
                if next_request.error is None and not peer_conn.is_closing \
                        and next_request.url.hostname == host and next_request.url.port == port:
                    # same host and port, reuse peer_conn
                    logger.debug("http-proxy(%s) DONE for '%s'", connection, head_request.request_line)
                    head_request = next_request
                    continue
                else:
                    logger.info("http-proxy(%s) DONE", connection)
                    peer_conn.close()
                    return True
            else:
                logger.info("http-proxy(%s) DONE", connection)
                peer_conn.close()
                return False

    def do_https_forward(self, request, connection, peer_conn):
        connection.writer.write(httphelper.https_proxy_response(request.version))
        yield from common.forward_forever(connection, peer_conn, is_responsed=True)

    @staticmethod
    def stop_on_httprequest(data):
        return isinstance(data, httphelper.RequestMessage)

    def do_http_forward(self, request, connection, peer_conn):
        peer_conn[common.KEY_FIRST_HTTP_REQUEST] = request
        data = self.rewrite_request(request)
        peer_conn.writer.write(data)
        yield from peer_conn.writer.drain()
        common.forward_log(logger, connection, peer_conn, data)

        data, _ = yield from common.forward_forever(connection, peer_conn, is_responsed=True, stop_func=self.stop_on_httprequest)
        return data

    def rewrite_request(self, parsed):
        buf = BytesIO()
        buf.write(parsed.method.encode() + b' ' +
                  parsed.path.encode() + b' ' + parsed.version.encode() + b'\r\n')
        for key in parsed.headers:
            if key == 'Proxy-Connection' or key == 'Proxy-Name':
                continue
            buf.write(key.encode() + b': ' + parsed.headers[key].encode() + b'\r\n')
        buf.write(b'\r\n')
        buf.write(parsed.body)
        return buf.getvalue()


class ManageableHttpListener(HttpListener):

    class InnerArgParser(argparse.ArgumentParser):
        def error(self, message):
            raise TypeError(message)

    def __init__(self, listen_addr, connector, proxy_holder=None, dump_config=None, loop=None, **kwargs):
        super().__init__(listen_addr, connector, loop=loop, **kwargs)
        self.proxy_holder = proxy_holder
        self._cmd_parser = self._get_cmd_parse()
        self._dump_config = dump_config

    @property
    def cmd_parser(self):
        return self._cmd_parser

    def _get_cmd_parse(self):
        parser = self.InnerArgParser(prog='', add_help=False, formatter_class=argparse.RawTextHelpFormatter)
        parser.add_argument('--help', action='store_true', default=False, help="print this help")
        parser.add_argument('--conn', action='store_true', default=False, help="list current connections")
        parser.add_argument('--list', action='store_true', default=False, help="list proxies information")
        parser.add_argument('--domain', action='store_true', default=False, help="show domain speed route table")
        parser.add_argument('--insert', metavar='proxy-info', nargs='+', dest='inss',
                            help='use "hostname:port/short-name" to insert a socks5 proxy to list head, \n'
                                 '"passord/method@hostname:server_port" or "hostname" to insert a shadowsocks proxy.\n'
                                 'hostname is a valid shadowsocks proxy host, \n'
                                 'and server_port/password/method in the config file "{hostname}.json".')
        parser.add_argument('--add', metavar='proxy-info', nargs='+', dest='adds',
                            help='append some proxies to list tail. proxy-info same as "insert" command.')
        parser.add_argument('--delete', metavar='hostname', nargs='+', dest='dels', help='delete the proxies')
        parser.add_argument('--acl', action='store_true', default=False, help="list current ACLs")
        parser.add_argument('--acl_add', metavar='IP', nargs='+', dest='acl_add_ips',
                            help='append IPs to ACL. IP can use *, ex: "192.168.20.*", for subnet')
        parser.add_argument('--acl_del', metavar='IP', nargs='+', dest='acl_del_ips',
                            help='delete IPs from ACL. IP can use *, ex: "192.168.20.*", for subnet')
        parser.add_argument('--pause', metavar='hostname', nargs='+', dest='pauses', help='pause the proxies')
        parser.add_argument('--head', metavar='hostname', nargs=1, dest='head', help='move the proxy to list head')
        parser.add_argument('--tail', action='store_true', dest='tail', default=False, help="move the head proxy to list tail")
        parser.add_argument('--stack', action='store_true', dest='stack', default=False, help="print threads stack trace")
        parser.add_argument('--dump', action='store_true', dest='dump', default=False, help="dump proxy info to file")
        parser.add_argument('--speed', metavar='hostname', nargs='*', dest='speed', help='test the proxy/proxies speed with background mode')
        parser.add_argument('--fspeed', metavar='hostname', nargs='*', dest='fspeed', help='test the proxy/proxies speed with foreground mode')
        parser.add_argument('--top', metavar='hostname', nargs=1, dest='top', help='fix the proxy to list top')
        parser.add_argument('--untop', action='store_true', dest='untop', default=False, help="unfix the top proxy")
        return parser

    def on_no_forwardhost(self, connection, request):
        try:
            ua = request.headers['User-Agent']
            if self.proxy_holder is None:
                res_cont = "it's ok"
                code = 200
            elif request.url.path == '/favicon.ico':
                res_cont = ''
                code = 404
            else:
                if request.url.path == '/' or request.url.path == '/list':
                    cmd_line = '--list'
                else:
                    cmd_line = '%s %s' % (request.url.path.replace('/', '--', 1), request.url.query.replace('&', ' '))
                out = StringIO()
                try:
                    code = yield from self.do_command(cmd_line, out, connection, ua)
                except Exception as pe:
                    code = 500
                    logger.info("do_command(%s) error: %s(%s)", request.url.path, common.clazz_fullname(pe), pe)
                    out.write('%s\n\n' % pe)
                    self.print_help(out)
                out.write('\nS=%s\n%s TSProxy v%s %s\n' % (str_datetime(self.proxy_holder.last_speed_test_time), '>' if self.proxy_holder.available else '=', __version__, str_datetime()))
                res_cont = out.getvalue()
                # code = 200
            if 'Mozilla' in ua or 'Chrome' in ua or 'Safari' in ua:
                cont_type = 'text/plain'
                # cont_type = 'text/html'
                # res_cont = '<body><text>' + res_cont.replace('\n', '</br>') + '</text></body>'
            else:
                cont_type = 'text/plain'
            connection.writer.write(httphelper.http_response(request.version, code, headers={
                'Content-Type': cont_type,
                'Cache-Control': 'no-store',
                'Pragma': 'no-cache',
                'Connection': 'close'
            }, content=res_cont))
        except BaseException as e:
            logger.exception(e)

    def do_command(self, cmd_line, out, cmd_conneciton, user_agent=None):
        cookie = 200
        cmd = self.cmd_parser.parse_args(cmd_line.split())
        if cmd.help:
            self.print_help(out)
        if cmd.conn:
            self.do_list_conn(out, cmd_conneciton)
            return cookie
        if cmd.acl:
            self.do_list_acl(out)
            return cookie
        if cmd.acl_add_ips:
            self.do_acl_add(out, cmd.acl_add_ips)
            self.do_dump(out)
            return cookie
        if cmd.acl_del_ips:
            self.do_acl_del(out, cmd.acl_del_ips)
            self.do_dump(out)
            return cookie
        if cmd.inss:
            cookie = self.do_insert(out, cmd.inss)
        if cmd.adds:
            cookie = self.do_add(out, cmd.adds)
        if cmd.dels:
            self.do_del(out, cmd.dels)
        if cmd.pauses:
            self.do_pause(out, cmd.pauses)
        if cmd.head:
            self.do_head(out, cmd.head[0])
        if cmd.top:
            self.do_top(out, cmd.top[0])
        if cmd.untop:
            self.do_untop(out)
        if cmd.tail:
            self.do_tail(out)
        if cmd.stack:
            self.do_stack(out)
        if cmd.speed is not None:
            cookie = yield from self.do_speed(out, cmd.speed)  # cmd.speed)
        if cmd.fspeed is not None:
            cookie = yield from self.do_speed(out, cmd.fspeed, True)  # cmd.speed)
        if cmd.dump or cmd.inss or cmd.adds or cmd.dels or cmd.pauses:
            self.do_dump(out)
        if cmd.domain:
            self.do_domain(out)
        if not cmd.help and not cmd.stack and not cmd.domain:
            self.do_list(out, cmd.fspeed if user_agent is not None and 'curl' in user_agent else None)
        return cookie

    def print_help(self, out):
        self.cmd_parser.print_help(out)

    def do_list_conn(self, out, connection):
        i = 0
        for conn in sorted(self.connections.values(), key=lambda c: c.idle_time, reverse=False):
            if conn.fileno == connection.fileno:
                continue
            r = conn.get_attr(HTTP_REQUEST, '')
            if r:
                r = r.request_line
            peer_conn = conn.get_attr(proxy.PEER_CONNECTION)
            out.write('[%2d] %-80s lives %.1f/%.1f seconds, R/W: %s/%s, realtime_speed: %sB/S\r\n'
                      % (i, conn, conn.idle_time, conn.life_time,
                         common.fmt_human_bytes(conn.reader.recv_bytes),
                         common.fmt_human_bytes(conn.writer.written_bytes),
                         common.fmt_human_bytes(peer_conn['_realtime_speed_']) if peer_conn is not None and '_realtime_speed_' in peer_conn else '-'))
            res = conn.get_attr(HTTP_RESPONSE)
            code = '%d(%s)' % (res.code, res.reason) if res is not None else ''
            recv_len = conn.get_attr(HTTP_RESPONSE_CONTENT_LENGTH)
            recv_info = ('%.1f%%(%s/%s)' % (recv_len*100/res.content_length, recv_len, res.content_length)) if res and recv_len and res.content_length else ''
            out.write('%14shandling %s %s %s %s\r\n\r\n' % ('', r, code, recv_info,
                                                            ('"%s".%d' % (conn['process_name'], conn['process_pid'])) if 'process_name' in conn else ''))
            i += 1

    def do_list_acl(self, out):
        for ipn, mask in self._acl:
            out.write('%s, 0x%x\n' % (socket.inet_ntoa(ipn.to_bytes(length=4, byteorder='big')), mask))

    def do_acl_add(self, out, ips):
        for ipv4 in ips:
            if self.acl_op(ipv4):
                out.write('ACL add %s ok.\n\n' % ipv4)
            else:
                out.write('ACL add %s FAIL.\n\n' % ipv4)
        self.do_list_acl(out)

    def do_acl_del(self, out, ips):
        for ipv4 in ips:
            if self.acl_op(ipv4, delete=True):
                out.write('ACL delete %s ok.\n\n' % ipv4)
            else:
                out.write('ACL delete %s FAIL.\n\n' % ipv4)
        self.do_list_acl(out)

    def _do_add(self, out, adds, insert=False):
        code = 200
        for key in adds:
            try:
                self.proxy_holder.add_proxy(key, insert=insert)
            except Exception as err:
                if code == 200:
                    code = 415
                out.write('\nError: %s\n\n' % err)
                continue
            out.write('%s %s to proxy list\n' % (key, 'insert' if insert else 'added'))
        return code

    def do_insert(self, out, inss):
        inss.reverse()
        return self._do_add(out, inss, insert=True)

    def do_add(self, out, adds):
        return self._do_add(out, adds)

    def do_speed(self, out, hosts, foreground=False):
        out.write('speed test started... %s \r\n' % hosts)
        if len(hosts) == 0 or (hosts[0] == '*' or hosts[0] == 'all'):
            _hosts = None
        else:
            _hosts = hosts
        t = self.loop.create_task(self.proxy_holder.test_proxies_speed(_hosts))
        if foreground:
            return (yield from t)
        else:
            return 200

    def do_pause(self, out, hosts):
        for host in hosts:
            p, idx = self.proxy_holder.find_proxy(host)
            if p is not None:
                p.pause = not p.pause
                out.write('%s %s\n' % (p, 'paused' if p.pause else 'resumed'))
                if not p.pause and p.hostname in self.proxy_holder.auto_pause_list:
                    self.proxy_holder.auto_pause_list.remove(p.hostname)
                if p.pause and idx == 0:
                    self.do_tail(out=None)
            else:
                out.write('\nError: %s DID NOT in proxy list\n\n' % host)

    def do_del(self, out, dels):
        for host in dels:
            p, _ = self.proxy_holder.find_proxy(host)
            if p and self.proxy_holder.psize == 1:
                out.write('\nWarn: we must keep ONE proxy, so stop delete %s\n\n' % host)
                break
            if p is not None:
                self.proxy_holder.remove_proxy(p)
                out.write('%s deleted from proxy list\n' % p)
            else:
                out.write('\nError: %s DID NOT in proxy list\n\n' % host)

    def do_list(self, out, high_light_proxies=None):
        out.write('global tp90: %.1fs/%d/%d\r\n' % (tsproxy.proxy.ProxyStat.calc_tp90(),
                                                    tsproxy.proxy.ProxyStat.global_tp90_len,
                                                    tsproxy.proxy.ProxyStat.global_resp_count))
        _max_total_count = sorted(self.proxy_holder.proxy_list, key=lambda p: p.total_count, reverse=True)[0].total_count
        _max_sess_count = sorted(self.proxy_holder.proxy_list, key=lambda p: p.proxy_count, reverse=True)[0].proxy_count
        for i in range(0, self.proxy_holder.psize):
            _proxy = self.proxy_holder.proxy_list[i]
            _proxy.print_info(i, out=out, max_total_count=_max_total_count, max_sess_count=_max_sess_count, high_light=(high_light_proxies is not None and _proxy.short_hostname in high_light_proxies))
            out.write('\r\n')

    def do_domain(self, out):
        self.proxy_holder.print_domain_speed(fmt='%-20s -> %s/%-15s @%-6s S=%s\r\n', out=out)

    def do_head(self, out, host):
        p, _ = self.proxy_holder.find_proxy(host)
        if p is not None:
            self.proxy_holder.move_to_head(p)
            out.write('%s move to proxy list head\n' % p)
        else:
            out.write('\nError: %s DID NOT in proxy list\n\n' % host)

    def do_top(self, out, host):
        p, _ = self.proxy_holder.find_proxy(host)
        if p is not None:
            self.proxy_holder.move_to_head(p)
            self.proxy_holder.fix_top = True
            out.write('%s fix to proxy list top\n' % p)
        else:
            out.write('\nError: %s DID NOT in proxy list\n\n' % host)

    def do_untop(self, out):
        self.proxy_holder.fix_top = False

    def do_dump(self, out):
        if self._dump_config is not None:
            self._dump_config()

    def do_tail(self, out):
        if self.proxy_holder.move_head_to_tail(head_proxy=None, msg='COMMAND'):
            if out:
                out.write('move head proxy to tail OK\n')
            else:
                logger.info('move head proxy to tail OK')
        else:
            if out:
                out.write('\nError: move head to tail FAIL\n\n')
            else:
                logger.info('Error: move head to tail FAIL')

    def do_stack(self, out):
        common.print_stack_trace(limit=None, out=out)


class HttpRequestDecoder(streams.Decoder):

    def __init__(self):
        self._http_parser = httphelper.HttpRequestParser()

    def __call__(self, connection, read_timeout):
        start_time = time.time()
        try:
            request = yield from connection.reader.read_bytes(read_timeout=read_timeout)
            if not request:
                return None
            if HTTP_REQUEST in connection and connection.get_attr(HTTP_REQUEST) != LOGGED \
                    and (connection.get_attr(HTTP_REQUEST).method == common.HTTPS_METHOD_CONNECT or HTTP_RESPONSE not in connection):
                # https或者http还未没有响应的话，直接二进制转发；
                # http已经有过响应的话，是一个新的request，需要解析
                return request
            # parse bytes to request object
            left_time = start_time + read_timeout - time.time()
            request = yield from self._http_parser.parse_request(connection.reader, request, read_timeout=left_time if left_time >= 1 else 1)
        except (TimeoutError, asyncio.TimeoutError):
            if HTTP_REQUEST in connection:
                raise
            else:
                request = httphelper.bad_request(timeout=read_timeout, request_time=start_time)
        except OSError as ex:
            logger.info("%s parse_request fail: %s(%s)", connection, common.clazz_fullname(ex), ex)
            return None
        except Exception as ex:
            logger.exception("%s parse_request fail: %s(%s)", connection, common.clazz_fullname(ex), ex)
            return None
        if request:
            # if HTTP_RESPONSE in connection:
            #     # 已经有过响应，说明是同一连接上的一个新请求
            #     http_common_log(connection, mark=',')
            connection.set_attr(HTTP_REQUEST, request)
        return request


class HttpResponseEncoder(streams.Encoder):

    def __init__(self):
        self._http_parser = httphelper.HttpResponseParser()

    def __call__(self, data, connection):
        if HTTP_REQUEST in connection:
            head_request = connection.get_attr(HTTP_REQUEST)
            if head_request.method == common.HTTPS_METHOD_CONNECT:
                if HTTP_RESPONSE not in connection and not isinstance(data, httphelper.ResponseMessage):
                    # 收到https的响应了，标记proxy成功
                    connection.set_attr(HTTP_RESPONSE, httphelper.https_proxy_response(head_request.version))
            else:
                if HTTP_RESPONSE not in connection:
                    if isinstance(data, httphelper.ResponseMessage):
                        connection.set_attr(HTTP_RESPONSE, data)
                        connection.set_attr(HTTP_RESPONSE_CONTENT_LENGTH, data.content_length)
                    else:
                        # real parse response from bytes
                        raw_data = connection.get_attr(HTTP_RESPONSE_RAW_DATA, b'') + data
                        response, consumed = self._http_parser.parse_response(raw_data, head_request.method)
                        connection.set_attr(HTTP_RESPONSE_RAW_DATA, raw_data[consumed:])
                        if response:
                            connection.set_attr(HTTP_RESPONSE, response)
                            connection.set_attr(HTTP_RESPONSE_CONTENT_LENGTH, len(response.body))
                            if 'Proxy-Name' in head_request.headers:
                                peer_conn = connection.get_attr(proxy.PEER_CONNECTION)
                                if peer_conn:
                                    rewrite_data = self.rewrite_response(response, {
                                        'Proxy-Server': peer_conn.raddr,
                                        'Proxy-LocalIP': peer_conn.laddr
                                    })
                                    logger.debug('rewrite_response: "%s" -> "%s"', data[:100], rewrite_data[:100])
                                    data = rewrite_data
                elif not isinstance(data, httphelper.ResponseMessage):
                    # 累计已接收的content-length
                    recv_length = connection.get_attr(HTTP_RESPONSE_CONTENT_LENGTH) + len(data)
                    connection.set_attr(HTTP_RESPONSE_CONTENT_LENGTH, recv_length)
                if HTTP_RESPONSE in connection:
                    # 检查content-length是否已经全部接收
                    response = connection.get_attr(HTTP_RESPONSE)
                    if response.content_length is not None:
                        recv_length = connection.get_attr(HTTP_RESPONSE_CONTENT_LENGTH)
                        logger.log(5, '%s %d/%d', head_request.request_line, recv_length, response.content_length)
                        # if recv_length >= response.content_length:
                        #     http_common_log(connection, mark='.')

        return data.raw_data if isinstance(data, httphelper.ResponseMessage) else data

    def rewrite_response(self, response, headers):
        buf = BytesIO()
        buf.write(('%s %d %s\r\n' % (response.version, response.code, response.reason)).encode())
        for key in response.headers:
            if key in headers:
                continue
            buf.write(('%s: %s\r\n' % (key, response.headers[key])).encode())
        for key in headers:
            buf.write(('%s: %s\r\n' % (key, headers[key])).encode())
        buf.write(b'\r\n')
        buf.write(response.body)
        return buf.getvalue()


LOGGED = 'LOGGED'


# http code:
# 500 - 没有响应就关闭连接
# 502 - 响应解析错误
# 503 - 连接服务器失败
# 504 - 响应超时了
# 400 - bad request
#
# mark:
#  ,  重用连接前的http log
#  .  重用连接前的http log（有content-length的响应）
#  .. 连接关闭时的http log
def http_common_log(connection, request=None, response=None, mark='..', proxy_protocol='http'):
    _request = connection.get_attr(HTTP_REQUEST) if not request else request
    if not response:
        response = connection.get_attr(HTTP_RESPONSE)
    peer_conn = connection.get_attr(proxy.PEER_CONNECTION)
    if _request is not None and _request == LOGGED:  # it's logged
        return
    if _request and response:
        status = response.code
        reason = response.reason
        # common_logger.info("%s %d(%s) request=%s, response=%s", mark, response.code, response.reason, _request, response)
    elif not _request:
        status = 400
        reason = 'No Request'
        # common_logger.info("%s no request", mark)
    else:  # no response
        if _request.error:
            status = _request.error.code
            reason = _request.error.message
            # common_logger.info("%s %d(%s) %s request=%s", mark, _request.error.code, _request.error.message, _request.request_line, _request)
        else:
            reason = None
            if peer_conn and peer_conn.response_timeout:
                status = 504
                # common_logger.info("%s 504(response timeout) request=%s", mark, _request)
            else:
                status = 500
                # common_logger.info("%s 500(connection close) request=%s", mark, _request)
    if not response:
        response = httphelper.http_response(version=_request.version if _request else None,
                                            status=status,
                                            reason=reason)

    proxy_name = connection.get_attr(proxy.PROXY_NAME, 'D')
    upload_bytes = connection.reader.recv_bytes if peer_conn is None else peer_conn.writer.written_bytes - peer_conn.get_attr(HTTP_REQUEST_LENGTH, 0)
    down_bytes = connection.writer.written_bytes if peer_conn is None else peer_conn.reader.recv_bytes - peer_conn.get_attr(HTTP_RESPONSE_LENGTH, 0)
    content_length = response.content_length if response.content_length else connection.get_attr(HTTP_RESPONSE_CONTENT_LENGTH)
    first_time = (response.response_time - _request.request_time) if _request else 0
    all_time = time.time() - (_request.request_time if _request else connection.create_time)

    common_logger.info('%s%s - %s/%s/"%s" - %s/%s%s/%.2fs/%.1fs - %s %s%s%s',
                       connection.laddr, ('/%d' % connection['process_pid']) if 'process_pid' in connection else '',
                       proxy_protocol, proxy_name, _request.request_line if _request else '',
                       format(upload_bytes, ','), format(down_bytes, ','),
                       "(%s)" % format(content_length, ',') if content_length is not None else '',
                       first_time, all_time, response.code, response.reason,
                       (' - %s' % connection['process_name']) if 'process_name' in connection else '',
                       mark)

    if not request:
        connection.set_attr(HTTP_REQUEST, LOGGED)  # mark it's logged
    connection.set_attr(HTTP_RESPONSE, None)
    connection.set_attr(HTTP_RESPONSE_RAW_DATA, None)
    connection.set_attr(HTTP_RESPONSE_CONTENT_LENGTH, None)
    if peer_conn:
        peer_conn.set_attr(HTTP_REQUEST_LENGTH, upload_bytes)
        peer_conn.set_attr(HTTP_RESPONSE_LENGTH, down_bytes)


