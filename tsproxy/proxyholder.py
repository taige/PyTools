import asyncio
import logging
import logging.config
import os
import socket
import time
from concurrent.futures import CancelledError

import requests

from tsproxy import common, topendns
from tsproxy.proxy import HttpProxy, ProxyStat, ShadowsocksProxy, Socks5Proxy
import tsproxy.proxy

logger = logging.getLogger(__name__)


TEST_URLS = [
    'http://www.flickr.com/',
    'http://twitter.com/',
    'http://www.facebook.com/',
    'http://www.tumblr.com/',
    'http://www.google.com/',
    'http://www.youtube.com/',
    'http://d.dropbox.com/',
    'http://plus.google.com/'
]


def sort_proxies(p):
    return 100 if p.pause else -p.sort_key


def get_wan_ip():
    from tsproxy.topendns import is_ipv4
    try:
        res = requests.get('http://members.3322.org/dyndns/getip', headers={
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.41 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
            'Accept-Language': 'zh-CN,zh;q=0.8'
        }, timeout=common.default_timeout)
        if 200 <= res.status_code < 400:
            wan_ip = res.text.rstrip()
            if is_ipv4(wan_ip):
                return wan_ip
            else:
                logger.warning('get_wan_ip() return not ipv4: %s', wan_ip)
    except Exception as ex:
        logger.warning('get_wan_ip() %s: %s', ex.__class__.__name__, ex)
    return None


class ProxyHolder(object):

    def __init__(self, proxy_port, loop=None, proxy_file='proxies.json'):
        self._loop = loop if loop else asyncio.get_event_loop()
        self.monitor_port = 0
        self._proxy_port = proxy_port
        self._proxy_count = 0
        self.hundred_c = 0  # hundred count
        self.proxy_list = []
        self.proxy_dict = {}
        # self.rlock = threading.RLock()
        self.proxy_check_queue = asyncio.Queue()
        self._executor = None
        self.speed_testing = False
        self.shutdowning = False
        self.dump_file = proxy_file
        self.fix_top = False
        self.wan_ip = None
        self.local_ip = None
        self.last_speed_test_time = 0
        self.testing_proxy = None
        self.auto_pause_list = set()

    @property
    def proxy_names(self):
        return self.proxy_dict.keys()

    @property
    def executor(self):
        if self._executor is None:
            max_workers = min(os.cpu_count()+1, self._proxy_count)
            self._executor = common.MyThreadPoolExecutor(max_workers=max_workers, pool_name='proxy-helper')
        return self._executor

    def load_proxies(self, j):
        # self.dump_file = json_file
        # with open(json_file, 'r') as f:
        #     j = json.load(f)
        if 'hundred_c' in j:
            self.hundred_c = j['hundred_c']
        if 'fix_top' in j:
            self.fix_top = j['fix_top']
        if 'wan_ip' in j:
            self.wan_ip = j['wan_ip']
        if 'local_ip' in j:
            self.local_ip = j['local_ip']
        if 'last_speed_test_time' in j:
            self.last_speed_test_time = j['last_speed_test_time']
        if 'auto_pause' in j:
            self.auto_pause_list = set(j['auto_pause'])
        for p in j['proxy_list']:
            px_classname = p['__class__']
            px_class = getattr(tsproxy.proxy, px_classname)
            proxy = px_class(self, **p)
            self.proxy_list.append(proxy)
            self.proxy_dict[proxy.short_hostname] = proxy
        self._proxy_count = len(self.proxy_list)
        # ProxyStat.global_proxy_count = j['global_proxy_count']
        # ProxyStat.global_proxy_count = FIFOList(common.tp90_expired_time, common.tp90_calc_count*self._proxy_count, lambda k: k[0], *(j['global_proxy_count']))
        # ProxyStat.global_fail_count = j['global_fail_count']
        if 'global_resp_time' in j:
            ProxyStat.global_resp_time = common.FIFOList(common.tp90_expired_time, common.tp90_calc_count*self._proxy_count, *(j['global_resp_time']))
        else:
            ProxyStat.global_resp_time = common.FIFOList(common.tp90_expired_time, common.tp90_calc_count*self._proxy_count)
        if 'global_tp90_inc' in j:
            ProxyStat.global_tp90_inc = j['global_tp90_inc']
        if 'global_last_tp90' in j:
            ProxyStat.global_last_tp90 = j['global_last_tp90']
        if 'global_tp90_inc_time' in j:
            ProxyStat.global_tp90_inc_time = j['global_tp90_inc_time']

    def dump_proxys(self, j):
        j.update({
            # 'proxy_count': self._proxy_count,
            'hundred_c': self.hundred_c,
            'proxy_list': self.proxy_list,
            'auto_pause': [*self.auto_pause_list],
            'fix_top': self.fix_top,
            'wan_ip': self.wan_ip,
            'local_ip': self.local_ip,
            'last_speed_test_time': self.last_speed_test_time,
            # 'global_proxy_count': ProxyStat.global_proxy_count,
            # 'global_fail_count': ProxyStat.global_fail_count,
            'global_resp_time': ProxyStat.global_resp_time,
            'global_tp90_inc': ProxyStat.global_tp90_inc,
            'global_last_tp90': ProxyStat.global_last_tp90,
            'global_tp90_inc_time': ProxyStat.global_tp90_inc_time,
        })

    def add_proxies(self, proxy_infos, insert=False):
        for p in proxy_infos:
            self.add_proxy(p, insert=insert)
        # ProxyStat.global_proxy_count = FIFOList(common.tp90_expired_time, common.tp90_calc_count*self._proxy_count, lambda k: k[0])
        ProxyStat.global_resp_time = common.FIFOList(common.tp90_expired_time, common.tp90_calc_count*self._proxy_count)  # , lambda k: k[0])

    def add_proxy(self, proxy_info, insert=False):
        p1 = proxy_info.find('/')
        p2 = proxy_info.find('@')
        p3 = proxy_info.find(':')
        http = ss = s5 = False
        if proxy_info.startswith('http://'):
            http = True
        elif 0 < p1 < p2 < p3:
            ss = True
        elif 0 < p3:
            s5 = True
        elif proxy_info.find('.') > 0:
            ss = True
        else:
            raise ValueError('unsupported proxy-info format: %s' % proxy_info)
        # p, _ = self.find_proxy(proxy_info)
        # if p:
        #     raise ValueError('%s is exist in proxy list' % proxy_info)
        if ss:
            p, _ = self.find_proxy(proxy_info)
            if p:
                raise ValueError('%s is exist in proxy list' % proxy_info)
            self.add_shadowsocks_proxy(proxy_info, insert=insert)
        else:
            short_hostname = None
            if proxy_info.find('/') > 0:
                proxy_info, short_hostname = proxy_info.rsplit('/', 1)
            hostname, port = proxy_info.rsplit(':', 1)
            if short_hostname is None:
                short_hostname = common.hostname2short(hostname)
                # if hostname.find('.') > 0:
                #     short_hostname, _ = hostname.split('.', 1)
                # else:
                #     short_hostname = hostname
            p, _ = self.find_proxy(short_hostname)
            if p:
                raise ValueError('%s is exist in proxy list' % proxy_info)
            if s5:
                self.add_socks5_proxy(hostname, int(port), short_hostname=short_hostname, insert=insert)
            elif http:
                _, hostname = hostname.split('//', 1)
                self.add_http_proxy(hostname, int(port), short_hostname=short_hostname, insert=insert)

    def add_socks5_proxys(self, zero_port, extra_port=None, *short_hostnames):
        self._proxy_count = len(short_hostnames)
        if extra_port is not None and type(extra_port) is int:
            self.monitor_port = extra_port
        if self._proxy_count == 0:
            if self.monitor_port == 0:
                raise TypeError("extra_port can't be 0 on NO proxy hostname")
            self._proxy_count = 1
        for i in range(1, self._proxy_count+1):
            if self._proxy_count == 1:
                p = self.monitor_port
            else:
                p = zero_port + i
            short_hostname = short_hostnames[i-1] if len(short_hostnames) >= i else 'lo%d' % (i-1)
            proxy = Socks5Proxy(self, '127.0.0.1', p, short_hostname=short_hostname)
            self.proxy_list.append(proxy)
            self.proxy_dict[proxy.short_hostname] = proxy

    def add_http_proxy(self, hostname, port, short_hostname=None, insert=False):
        if short_hostname is None:
            short_hostname = 'lo%d' % self._proxy_count
        proxy = HttpProxy(self, hostname, port, short_hostname=short_hostname)
        if insert:
            self.proxy_list.insert(0, proxy)
        else:
            self.proxy_list.append(proxy)
        self.proxy_dict[proxy.short_hostname] = proxy
        self._proxy_count += 1

    def add_socks5_proxy(self, hostname, port, short_hostname=None, insert=False):
        if short_hostname is None:
            short_hostname = 'lo%d' % self._proxy_count
        proxy = Socks5Proxy(self, hostname, port, short_hostname=short_hostname)
        if insert:
            self.proxy_list.insert(0, proxy)
        else:
            self.proxy_list.append(proxy)
        self.proxy_dict[proxy.short_hostname] = proxy
        self._proxy_count += 1

    def add_shadowsocks_proxys(self, *hostnames):
        for h in hostnames:
            self.add_shadowsocks_proxy(h)

    def add_shadowsocks_proxy(self, host, insert=False):
        ''' hostname: passord/method@us1.sss.tf:443 or us1.sss.tf '''
        h = host
        p1 = h.find('/')
        p2 = h.find('@')
        p3 = h.find(':')
        if 0 < p1 < p2 < p3:
            h, port = h.split(':')
            h, hostname = h.split('@')
            password, method = h.split('/')
            proxy_config = {
                'server_port': int(port),
                'password': password,
                'method': method
            }
        else:
            hostname = h
            json_file = hostname + '.json'
            proxy_config = {
                'json_config': json_file
            }
        proxy = ShadowsocksProxy(self, hostname, **proxy_config)
        if insert:
            self.proxy_list.insert(0, proxy)
        else:
            self.proxy_list.append(proxy)
        self.proxy_dict[proxy.short_hostname] = proxy
        self._proxy_count += 1

    def _test_proxy(self, proxy_name=None, test_url='http://www.google.com.hk/', reason=''):
        res = None
        try:
            if proxy_name:
                self.testing_proxy = proxy_name

            def async_request():
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) '
                                  'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.41 Safari/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Cache-Control': 'no-cache',
                    'Pragma': 'no-cache',
                    'Connection': 'close',
                    'Accept-Language': 'zh-CN,zh;q=0.8'
                    # 'Proxy-Name': '%s' % proxy_name
                }
                if proxy_name:
                    headers['Proxy-Name'] = proxy_name
                return requests.head(test_url, headers=headers, timeout=common.default_timeout+1, proxies={
                    "http": "http://127.0.0.1:%d" % (self._proxy_port-1),
                    "https": "http://127.0.0.1:%d" % (self._proxy_port-1)
                })
            res = yield from self._loop.run_in_executor(self.executor, async_request)
            logger.info('_test_proxy(%s, %s) status_code: %d', proxy_name, reason, res.status_code)
            local_ip = res.headers.get('Proxy-LocalIP', None)
            return res.status_code, local_ip
        except BaseException as ex:
            logger.warning('_test_proxy(%s, %s) %s: %s', proxy_name, reason, ex.__class__.__name__, ex)
        finally:
            self.testing_proxy = None
            if res:
                res.close()
        return 500, None

    def test_proxies(self, reason='regular check', *test_list):
        test_url = TEST_URLS.pop(0)
        TEST_URLS.append(test_url)

        if test_list is None or len(test_list) == 0:
            test_list = self.proxy_list
        futures = []
        for px in test_list:
            if not px.pause:
                futures.append(self._test_proxy(px.short_hostname, test_url, reason))
        # if len(test_list) > 1:
        #     futures.append(self._test_proxy(None, test_url, reason))

        network_is_ok = False
        if len(futures) > 1:
            done, pending = yield from asyncio.wait(futures, loop=self._loop)
            for f in done:
                status_code, local_ip = f.result()
                if 200 <= status_code < 400:
                    network_is_ok = True
                    if (time.time() - self.last_speed_test_time) > common.tp90_expired_time or self.local_ip is None or (local_ip is not None and self.local_ip != local_ip):
                        # speed value life time: 3 hours
                        # OR local/wan access changed
                        logger.info("LOCAL IP: %s", local_ip)
                        wan_ip = yield from self._loop.run_in_executor(self.executor, get_wan_ip)
                        if wan_ip is not None:
                            self.local_ip = local_ip
                        if (time.time() - self.last_speed_test_time) > common.tp90_expired_time or self.wan_ip is None or (wan_ip is not None and self.wan_ip != wan_ip):
                            # self.last_speed_test_time = time.time()
                            self.wan_ip = wan_ip
                            logger.info("WAN IP: %s", wan_ip)
                            yield from self.test_proxies_speed()
                        break

            logger.info("test_proxies done: %s", network_is_ok)
        return network_is_ok

    def _speed_test(self, proxy, url, speed_threshold=0, timeout=3, bytes_range=None):
        res = None
        MAX_BUF_SIZE_KB = 200
        self.testing_proxy = proxy.short_hostname
        try:
            logger.debug("going to test_proxies_speed %s speed", proxy.short_hostname)
            start = time.time()
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) '
                              'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.41 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Cache-Control': 'no-cache',
                'Pragma': 'no-cache',
                'Accept-Language': 'zh-CN,zh;q=0.8'
                # 'Proxy-Name': '%s' % proxy.short_hostname
            }
            if bytes_range:
                headers['Range'] = 'bytes=0-%d' % bytes_range
            res = requests.get(url, headers=headers, timeout=timeout, proxies={
                "http": "http://127.0.0.1:%d" % self._proxy_port,
                "https": "http://127.0.0.1:%d" % self._proxy_port
            }, stream=True)
            res_len = 0
            if 200 <= res.status_code < 400:
                con_len = res.headers.get('content-length', 0)
                kn = 10
                down_speed = 0
                while True:
                    buf = res.raw.read(kn * 1024)
                    if not buf:
                        break
                    res_len += len(buf)
                    end = time.time()
                    time_past = end - start
                    down_speed = res_len / time_past
                    logger.debug("_speed_test(%s) recved %d/%s, time_past: %.1f sec, speed: %sB/S", proxy.short_hostname, res_len, con_len, time_past,
                                 common.fmt_human_bytes(down_speed))
                    if (kn > 10 and down_speed < min(kn, 100)*1024) or (100 < kn and time_past > timeout) or down_speed > 1024*1024:
                        break
                    if kn < MAX_BUF_SIZE_KB:
                        kn *= 2
                    if kn > MAX_BUF_SIZE_KB:
                        kn = MAX_BUF_SIZE_KB
                logger.info('_speed_test(%s) status_code: %d, used %.1f sec, recv %d/%s bytes, speed: %sB/S',
                            proxy.short_hostname, res.status_code, time_past, res_len, con_len,
                            common.fmt_human_bytes(down_speed))
                if (time.time() - self.last_speed_test_time) > 600:
                    proxy.down_speed = down_speed
                else:
                    proxy.down_speed = (proxy.down_speed + down_speed)/2
            else:
                proxy.down_speed = 0
                logger.info('_speed_test(%s) status_code: %d', proxy.short_hostname, res.status_code)
        except BaseException as ex:
            proxy.down_speed = 0
            logger.warning('_speed_test(%s) %s: %s', proxy.short_hostname, ex.__class__.__name__, ex)
        finally:
            self.testing_proxy = None
            if res:
                res.close()
        return proxy.down_speed

    def test_proxies_speed(self, hosts=None, re_test=2, bytes_range=2133961,
                           url='https://vt.media.tumblr.com/tumblr_olmzaj26fj1qlmvfe_480.mp4'):
        # with self.rlock:
        if self.speed_testing:
            return
        self.speed_testing = True

        def async_test(_hosts=None):
            if _hosts is None:
                _hosts = hosts
            max_speed = 0
            if not _hosts or self.proxy_list[0].short_hostname in _hosts:
                max_speed = self._speed_test(self.proxy_list[0], url, bytes_range=bytes_range)
            # for proxy in sorted(self.proxy_list[1:], key=lambda p: p.tp90):
            for proxy in self.proxy_list[1:]:
                # err_c = proxy.error_count + 1
                # if proxy.error_time < common.retry_interval_on_error * err_c:
                #     logger.debug("test_proxies_speed %s skip for error", proxy.short_hostname)
                #     continue
                if _hosts and proxy.short_hostname not in _hosts:
                    continue
                if proxy.pause:  # or 0 == proxy.tp90 or proxy.tp90 > ProxyStat.calc_tp90()*1.2:
                    logger.debug("test_proxies_speed %s skip for tp90 %.1f or pause=%s", proxy.short_hostname, proxy.tp90, proxy.pause)
                    continue
                _speed = self._speed_test(proxy, url, speed_threshold=0 if _hosts else max_speed*0.5, bytes_range=bytes_range)
                if _speed > max_speed:
                    max_speed = _speed

        _may_the_head = sorted(self.proxy_list, key=lambda p: p.total_count, reverse=True)[0]
        logger.info("test_proxies_speed START (_may_the_head=%s)", _may_the_head)
        retried = 0
        _fix_top = self.fix_top
        self.fix_top = False
        head_proxy = self.head_proxy
        may_the_heads = [_may_the_head.hostname, head_proxy.hostname]
        while True:
            yield from self._loop.run_in_executor(self.executor, async_test)
            self.last_speed_test_time = time.time()
            self.proxy_list.sort(key=sort_proxies)
            if (self.head_proxy.down_speed < 100 * 1024 or self.head_proxy.hostname not in may_the_heads) and retried < re_test:
                retried += 1
                logger.info("test_proxies_speed RE-RUN #%d for head[%s] speed=%sB/S",
                            retried, self.head_proxy.short_hostname, common.fmt_human_bytes(self.head_proxy.down_speed))
                may_the_heads.append(self.head_proxy.hostname)
                continue
            break
        move_head = head_proxy.hostname != self.head_proxy.hostname
        if not move_head:
            self.fix_top = _fix_top
        else:
            self.head_proxy.reset_stat_info()
            self.head_proxy.head_time = time.time()
        self.speed_testing = False
        logger.info("test_proxies_speed DONE#%d for move_head=%s[%s]", retried, move_head, self.head_proxy)

    def find_proxy(self, hostname):
        if hostname.find('@') >= 0:
            _, hostname = hostname.split('@', 1)
        if hostname.find(':') >= 0:
            hostname, _ = hostname.split(':', 1)
        # if hostname.find('.') >= 0:
        #     hostname = common.hostname2short(hostname)
            # hostname, _ = hostname.split('.', 1)
        for i in range(0, self._proxy_count):
            p = self.proxy_list[i]
            if p.short_hostname == hostname or p.hostname == hostname:
                return p, i
        return None, -1

    def remove_proxy(self, proxy):
        self.proxy_list.remove(proxy)
        del self.proxy_dict[proxy.short_hostname]
        self._proxy_count -= 1

    def sort_proxies(self):
        if self.fix_top:
            return

        def connect_test(proxy):
            if proxy.pause:
                return
            hostname = proxy.hostname
            port = proxy.port
            ip = topendns.dns_query(hostname)
            if ip is None:
                logger.warning('dns_query(%s) fail', hostname)
                return
            start = time.time()
            used = 100
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.settimeout(5)
                sock.connect((ip, port))
                used = time.time() - start
                logger.info('connect to proxy %s/%s:%d used %.2f sec', hostname, ip, port, used)
            except (ConnectionError, socket.timeout) as conn_err:
                logger.warning('try to connect to proxy %s/%s:%d fail: %s', hostname, ip, port, conn_err)
            finally:
                sock.close()
            proxy.connected_used = used

        futures = []
        for px in self.proxy_list[1:]:
            if px.pause:
                continue
            futures.append(self.executor.submit(connect_test, px))

        connect_test(self.proxy_list[0])
        for f in futures:
            f.result()

        self.proxy_list.sort(key=lambda p: -p.down_speed if p.down_speed > 0 else p.tp90 if p.tp90 > 0 and p.connected_used < 100 else p.connected_used)

    def check(self, proxy, reason):
        if self.testing_proxy and self.testing_proxy == proxy.short_hostname:
            return
        self.proxy_check_queue.put_nowait((proxy, reason))

    def monitor_loop(self, loop=None):
        check_interval = common.default_timeout
        while not self.shutdowning:
            timeout = False
            checking_proxy = set()
            try:
                with common.Timeout(check_interval):
                    p, checking_reason = yield from self.proxy_check_queue.get()
                    checking_proxy.add(p)
                yield from asyncio.sleep(1, loop=loop)
                while not self.proxy_check_queue.empty():
                    p, r = self.proxy_check_queue.get_nowait()
                    checking_proxy.add(p)
                    if checking_reason.find(r) < 0:
                        checking_reason += ', %s' % r
            except asyncio.TimeoutError:
                timeout = True
            except CancelledError:
                break
            except BaseException as ex2:
                logger.exception('%s: %s', ex2.__class__.__name__, ex2)

            try:
                if timeout:
                    yield from self.test_proxies()
                    if self._proxy_check(timeout):
                        check_interval = common.proxys_check_timeout
                    else:
                        check_interval = common.default_timeout
                    logger.debug("check_interval=%d", check_interval)
                else:
                    for p in checking_proxy:
                        if p.fail_rate > common.fail_rate_threshold or p.error_count > 0:
                            self.notify_monitor('restart' if p.fail_rate < 0.9 or p.error_count > 0 else 'check', p)
                        # yield from self._test_proxy(p.short_hostname, reason=checking_reason)
                        yield from self.test_proxies(checking_reason, p)
                        self._proxy_check(timeout, p)
            except CancelledError:
                break
            except BaseException as ex1:
                logger.exception('_proxy_check() error: %s', ex1)
        # self.executor.shutdown()

    def notify_monitor(self, cmd, proxy):
        pass

    def _proxy_check(self, timeout, checking_proxy=None):
        should_move_tail = False
        move_tail = False
        move_head = False

        head_proxy = self.head_proxy
        if checking_proxy and checking_proxy.hostname != head_proxy.hostname:
            logger.log(5, 'checking %s is not the HEAD', checking_proxy)
            return True

        logger.info("========== %d(%.1f) ==========", self.hundred_c, ProxyStat.calc_tp90())

        # move head to tail condition:
        # 1: fail_rate > 210%
        # 2: tp90 > global_tp90 x 1.1
        # 3: tp90 increment >= 50%
        # 4: sort_key decrement >= 50%
        fail_rate = head_proxy.fail_rate
        global_tp90 = ProxyStat.calc_tp90()
        tp90_inc_percent, last_tp90, tp90_inc = head_proxy.tp90_increment
        sort_key_dec, last_sort_key = head_proxy.sort_key_decrement
        # logger.info('%s sort_key_decrement: %.1f%%', head_proxy, sort_key_dec*100)
        if fail_rate > common.fail_rate_threshold:
            # fail rate greater than 20%
            hp = self.try_select_head_proxy(force_to_head=True, only_select=True)
            if hp is not None:
                move_tail = True
                self.move_head_to_tail(head_proxy, logging.INFO, "fail_rate=%.1f%% > %.1f%%",
                                       fail_rate*100, common.fail_rate_threshold*100)
        elif global_tp90 > 0 and (head_proxy.tp90/global_tp90) > common.global_tp90_threshold:
            # when head proxy's tp90 greater than global tp90 1.1x, try cut it
            hp = self.try_select_head_proxy(force_to_head=True, only_select=True)
            if hp is not None:
                move_tail = True
                self.move_head_to_tail(head_proxy, logging.INFO, "tp90=%.1f > (global_tp90=%.1f[%s] x %d)",
                                       head_proxy.tp90, global_tp90, ProxyStat.get_global_tp90_inc(), common.global_tp90_threshold)
        elif tp90_inc_percent >= common.tp90_inc_threshold:
            # tp90 increment greater than 50%
            hp = self.try_select_head_proxy(only_select=True, tp90_factor=1.0)
            if hp is not None:
                move_tail = True
                self.move_head_to_tail(head_proxy, logging.INFO, "tp90_inc(%.1f->%.1f +%.1f%% > %.1f%%)",
                                       last_tp90, head_proxy.tp90, tp90_inc_percent*100, common.tp90_inc_threshold*100)
            # else:
            #     logger.info("HEAD%s tp90_inc(%.1f->%.1f +%.1f%% > %.1f%%), but no proxy(%s) little than it :(",
            #                 head_proxy, last_tp90, head_proxy.tp90, tp90_inc_percent*100, common.tp90_inc_threshold*100, hp)
        if sort_key_dec >= common.tp90_inc_threshold:
            # sort_key decrement >= 50%
            hp = self.try_select_head_proxy(only_select=True, tp90_factor=global_tp90/head_proxy.tp90)
            if hp is not None:
                move_tail = True
                self.move_head_to_tail(head_proxy, logging.INFO, "sort_key_dec(%d->%d -%.1f%% > %.1f%%)",
                                       last_sort_key, head_proxy.sort_key, sort_key_dec*100, common.tp90_inc_threshold*100)
        if move_tail:
            head_proxy = self.head_proxy
        # elif int(head_proxy.total_count/common.hundred) > self.hundred_c \
        #         or int(head_proxy.proxy_count/(common.hundred/(1 if head_proxy.tp90 <= global_tp90 else 2))) > 0:
        elif head_proxy.sess_count > common.hundred:
            # head proxy exceed the session proxy count

            move_head = self.try_select_head_proxy()

            # if not move_head:
            #     self.hundred_c += 1
            #     hundred_inc = True
            #     # pretend sort by tp90
            #     logger.info('sorting proxy proxys by tp90, hundred_c=%d, global_tp90=%.1f[%s]', self.hundred_c, global_tp90,
            #                 ProxyStat.get_global_tp90_inc())
            #     # with self.rlock:
            #     move_head = self.try_select_head_proxy()
            #
            if not move_head:
                head_proxy.reset_stat_info()
            else:
                head_proxy = self.head_proxy

        # if timeout and not move_head and not move_tail and not hundred_inc:
        #     # timeout and nothing happend above
        #     if (time.time() - head_proxy.head_time) > common.retry_interval_on_error:
        #         # with self.rlock:
        #         move_head = self.try_select_head_proxy(tp90_factor=1.0)
        #         if move_head:
        #             head_proxy = self.head_proxy
        for _proxy in self.proxy_list[1:]:
            if not _proxy.pause:
                if (_proxy.tp90 >= global_tp90*3 and _proxy.tp90_len > 10) or (_proxy.proxy_count > 10 and _proxy.fail_rate >= common.fail_rate_threshold*3):
                    _proxy.pause = True
                    self.auto_pause_list.add(_proxy.hostname)
                    logger.info("%s auto pause", _proxy)
            elif _proxy.hostname in self.auto_pause_list:
                if (_proxy.tp90 <= global_tp90 or _proxy.tp90_len <= 10) and (_proxy.proxy_count <= 10 or _proxy.fail_rate < common.fail_rate_threshold):
                    _proxy.pause = False
                    self.auto_pause_list.remove(_proxy.hostname)
                    logger.info("%s auto resume", _proxy)

        if head_proxy.tp90_len >= common.tp90_calc_count:
            return True
        # for i in range(0, self._proxy_count):
        #     proxy = self.proxy_list[i]
        #     if proxy.tp90_len >= common.tp90_calc_count:
        #         return True

        return False

    @property
    def psize(self):
        return self._proxy_count

    @property
    def head_proxy(self):
        return self.proxy_list[0]

    def move_to_head(self, proxy):
        # with self.rlock:
        self.proxy_list.remove(proxy)
        self.proxy_list.insert(0, proxy)

    def _move_head_to_tail(self):
        head_proxy = self.head_proxy
        self.proxy_list.remove(head_proxy)
        self.proxy_list.append(head_proxy)

    def move_head_to_tail(self, head_proxy, log_level=logging.INFO, mesg=None, *arg, **kwargs):
        if self._proxy_count <= 1:
            return False
        # with self.rlock:
        if head_proxy and head_proxy.hostname != self.head_proxy.hostname:
            logger.debug("move_head(%s)_to_TAIL() fail cause it's not the head", head_proxy)
            return False
        if not head_proxy:
            head_proxy = self.head_proxy
        if mesg:
            logger.log(log_level, "move_head(%s)_to_TAIL() cause " + mesg, head_proxy, *arg, **kwargs)
        self.fix_top = False
        head_proxy.error_time = time.time()
        self._move_head_to_tail()
        for i in range(1, self._proxy_count-1):
            if not self.head_proxy.pause:
                break
            self._move_head_to_tail()
        self.try_select_head_proxy(force_to_head=True)
        return True

    def try_select_head_proxy(self, force_to_head=False, only_select=False, tp90_factor=1.1):
        if self._proxy_count <= 1:
            return False
        if self.fix_top:
            return None if only_select else False
        if force_to_head and not only_select:
            select_from = 0
            select_end = self._proxy_count - 1
        else:
            select_from = 1
            select_end = self._proxy_count
        head_proxy = self.head_proxy
        for proxy in sorted(self.proxy_list[select_from:select_end], key=sort_proxies):
            if head_proxy.sort_key > proxy.sort_key and not force_to_head:
                logger.debug("try_select_head_proxy(): NOT move %s to HEAD cause sort_key[%.1f] > head.sort_key(%s)", proxy, proxy.sort_key, head_proxy.sort_key)
                break
            if not (proxy.tp90 <= head_proxy.tp90*tp90_factor or (force_to_head and proxy.fail_rate <= common.fail_rate_threshold)):
                logger.debug("try_select_head_proxy(): NOT move %s to HEAD cause proxy.tp90 > head_proxy.tp90[%.1f]*tp90_factor[%.1f]",
                             proxy, head_proxy.tp90, tp90_factor)
                continue
            if proxy.error_time < common.retry_interval_on_error * proxy.error_count:
                logger.debug("try_select_head_proxy(): NOT move %s to HEAD cause error_time=%.1f < %.1fx%d",
                             proxy, proxy.error_time, common.retry_interval_on_error, proxy.error_count)
                continue
            if proxy.pause or (proxy.tp90_len == 0 and proxy.total_count > 0):
                logger.debug("try_select_head_proxy(): NOT move %s to HEAD cause pause=%s", proxy, proxy.pause)
                continue
            if only_select:
                return proxy
            # factor = proxy.factor
            # if int(factor/common.hundred) <= self.hundred_c or force_to_head:
            # move the proxy to head
            proxy.reset_stat_info()
            if head_proxy.hostname != proxy.hostname:
                self.proxy_list.remove(proxy)
                self.proxy_list.insert(0, proxy)
                logger.info("try_select_HEAD_proxy(): select %s to HEAD {global_tp90=%.1f[%s]}[%d:%d] old_head=%s %s", proxy,
                            ProxyStat.calc_tp90(), ProxyStat.get_global_tp90_inc(),
                            select_from, select_end, head_proxy, "by force" if force_to_head else '')
            else:
                logger.info("try_select_HEAD_proxy(): select %s, but it is the HEAD", proxy)
            proxy.head_time = time.time()
            return True
        if force_to_head:
            logger.warning("try_select_HEAD_proxy(): sorry, we CAN NOT select head proxy [%d:%d] %s",
                           select_from, select_end, "by force" if force_to_head else '')
        return None if only_select else False


