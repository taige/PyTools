#!/usr/bin/env python3

import asyncio
import concurrent.futures
import logging
import os
import queue
import sys
import threading
import time
import traceback
import errno
from io import StringIO
from configparser import RawConfigParser
from urllib.parse import urlparse

from async_timeout import timeout

from tsproxy import lookup_conf_file, str_datetime, ts_print, __version__

logger = logging.getLogger(__name__)

KEY_FIRST_HTTP_REQUEST = 'FIRST_HTTP_REQUEST'
HTTPS_METHOD_CONNECT = 'CONNECT'
KEY_IP_CHANGED = 'KEY_ip_changed'

Timeout = timeout

apnic_latest_url = 'https://ftp.apnic.net/apnic/stats/apnic/delegated-apnic-latest'
apnic_expired_days = 30

# default timeout seconds for read and connect
default_timeout = 10

# real proxy proxy count per session
hundred = 100

# proxy timeout config
proxy_idle_sec = 5
# seconds of proxys check
proxys_check_timeout = 120
# next retry time(seconds) on error
retry_interval_on_error = 120
# close connection on idle 1/2 hour
close_on_idle_timeout = 600

# max times for fail_rate
max_times_fail_rate = 100
# tp90 increment percent Threshold
tp90_inc_threshold = 0.5
# when head proxy's tp90 greater than global tp90 2 times, cut it
global_tp90_threshold = 1.9
# fail rate threshold
fail_rate_threshold = 0.2
# auto pause fail rate threshold
auto_pause_fail_rate_threshold = 0.3
# response time expired after 3 hour on calc tp90
tp90_expired_time = 3600*3
# use recent 100 response time on calc tp90
tp90_calc_count = 100

# speed test result lifetime
speed_lifetime = 12 * 3600

speed_test_timeout = 5
speed_retry_count = 2
speed_average_threshold = 100
speed_index_url = 'https://www.tumblr.com/'
speed_urls = ['https://ve.media.tumblr.com/tumblr_pfwg4l558b1sq04bj_480.mp4', 'https://vtt.tumblr.com/tumblr_pf98cpiaq61x9cgqk.mp4', 'https://vt.media.tumblr.com/tumblr_olmzaj26fj1qlmvfe_480.mp4']
speed_domains = set()
speed_domain_mapping = {}

speed_hosts = set()
speed_black_hosts = set()

speed_hosts_file = None
speed_hosts_update = 0
speed_hosts_file_mod = 0

_network_errors = (errno.ENETDOWN, errno.ENETRESET, errno.ENETUNREACH, errno.EHOSTDOWN, errno.EHOSTUNREACH)
network_errors = set(_network_errors)


def load_tsproxy_conf(conf_file):
    global default_timeout
    global hundred
    # proxy timeout config
    global proxy_idle_sec
    # seconds of proxys check
    global proxys_check_timeout
    # next retry time(seconds) on error
    global retry_interval_on_error
    # close connection on idle 1/2 hour
    global close_on_idle_timeout
    # max times for fail_rate
    global max_times_fail_rate
    # tp90 increment percent Threshold
    global tp90_inc_threshold
    # when head proxy's tp90 greater than global tp90 2 times, cut it
    global global_tp90_threshold
    # fail rate threshold
    global fail_rate_threshold
    # auto pause fail rate threshold
    global auto_pause_fail_rate_threshold
    # response time expired after 3 hour on calc tp90
    global tp90_expired_time
    # use recent 100 response time on calc tp90
    global tp90_calc_count

    global apnic_latest_url
    global apnic_expired_days

    global _network_errors
    global network_errors

    global speed_lifetime
    global speed_test_timeout
    global speed_retry_count
    global speed_average_threshold
    global speed_index_url
    global speed_urls
    global speed_domains
    global speed_domain_mapping

    config = RawConfigParser(allow_no_value=True, delimiters=('='))
    config.optionxform = lambda option: option
    config.read(conf_file)

    def _common_conf_get(func, key, default, section="common", remove=False):
        try:
            if config.has_option(section, key):
                value = func(section, key)
                if remove:
                    config.remove_option(section, key)
                return value
        except Exception as ex:
            logging.exception('_common_conf_get(%s, %s) fail: %s', section, key, ex)
        return default

    if config.has_section('common'):
        logger.info('tsproxy.conf: [common] %s', config.items('common'))
    default_timeout = _common_conf_get(config.getint, "default_timeout", default_timeout)
    hundred = _common_conf_get(config.getint, "hundred", hundred)
    proxy_idle_sec = _common_conf_get(config.getint, "proxy_idle_sec", proxy_idle_sec)
    proxys_check_timeout = _common_conf_get(config.getint, "proxys_check_timeout", proxys_check_timeout)
    retry_interval_on_error = _common_conf_get(config.getint, "retry_interval_on_error", retry_interval_on_error)
    close_on_idle_timeout = _common_conf_get(config.getint, "close_on_idle_timeout", close_on_idle_timeout)
    max_times_fail_rate = _common_conf_get(config.getint, "max_times_fail_rate", max_times_fail_rate)
    tp90_inc_threshold = _common_conf_get(config.getfloat, "tp90_inc_threshold", tp90_inc_threshold)
    global_tp90_threshold = _common_conf_get(config.getfloat, "global_tp90_threshold", global_tp90_threshold)
    fail_rate_threshold = _common_conf_get(config.getfloat, "fail_rate_threshold", fail_rate_threshold)
    auto_pause_fail_rate_threshold = _common_conf_get(config.getfloat, "auto_pause_fail_rate_threshold", auto_pause_fail_rate_threshold)
    tp90_expired_time = _common_conf_get(config.getint, "tp90_expired_time", tp90_expired_time)
    tp90_calc_count = _common_conf_get(config.getint, "tp90_calc_count", tp90_calc_count)

    apnic_latest_url = _common_conf_get(config.get, "apnic_latest_url", apnic_latest_url)
    apnic_expired_days = _common_conf_get(config.getint, "apnic_expired_days", apnic_expired_days)

    __network_errors = _common_conf_get(config.get, "network_errors", None)
    if __network_errors:
        network_errors.clear()
        _buf = __network_errors.split(',')
        _buf.extend(_network_errors)
        for e in _buf:
            e = int(e)
            if e not in errno.errorcode:
                logger.info('unknow network error code: %d', e)
                continue
            network_errors.add(e)
            logger.info('NETWORK_ERROR: %d %s', e, errno.errorcode[e])

    if config.has_section("speed_test"):
        if config.has_section('speed_test'):
            logger.info('tsproxy.conf: [speed_test] %s', config.items('speed_test'))
        speed_lifetime = _common_conf_get(config.getint, "speed_lifetime", speed_lifetime, section="speed_test", remove=True)
        speed_retry_count = _common_conf_get(config.getint, "speed_retry_count", speed_retry_count, section="speed_test", remove=True)
        speed_test_timeout = _common_conf_get(config.getfloat, "speed_test_timeout", speed_test_timeout, section="speed_test", remove=True)
        speed_average_threshold = _common_conf_get(config.getint, "speed_average_threshold", speed_average_threshold, section="speed_test", remove=True)
        speed_index_url = _common_conf_get(config.get, "speed_index_url", speed_index_url, section="speed_test", remove=True)
        _speed_urls = []
        _speed_domain_map = {}
        for (k, v) in config.items("speed_test"):
            if k.startswith("http"):
                if v is not None:
                    k = '%s=%s' % (k, v)
                _speed_urls.append(k)
            elif v is not None:
                _speed_domain_map[k] = v
        if len(_speed_urls) > 0:
            speed_urls.clear()
            speed_urls.extend(_speed_urls)
        if len(_speed_domain_map) > 0:
            speed_domain_mapping.clear()
            speed_domain_mapping.update(_speed_domain_map)
    speed_domains.clear()
    for url in speed_urls:
        domain = urlparse(url).netloc
        speed_domains.add(domain)


def clazz_fullname(obj):
    _module = obj.__class__.__module__
    _name = obj.__class__.__name__
    if _module == 'builtins':
        return _name
    return '%s.%s' % (_module, _name)


def update_speed_hosts():
    global speed_hosts_update
    global speed_hosts_file_mod
    global speed_hosts_file
    global speed_hosts
    global speed_black_hosts

    if (time.time() - speed_hosts_update) < 1:
        return
    speed_hosts_update = time.time()
    if speed_hosts_file is None:
        speed_hosts_file = lookup_conf_file('speed_sites.conf')
    try:
        mtime = os.stat(speed_hosts_file).st_mtime
        if speed_hosts_file_mod < mtime:
            if speed_hosts_file_mod >= mtime:
                return
            speed_hosts.clear()
            speed_black_hosts.clear()
            with open(speed_hosts_file, 'r') as f:
                while True:
                    d = f.readline()
                    if not d:
                        break
                    d = d.strip()
                    if d and not d.startswith("#"):
                        if d.startswith("-"):
                            speed_black_hosts.add(d[1:])
                        else:
                            speed_hosts.add(d)
            speed_hosts_file_mod = mtime
            logger.info('%s reloaded', speed_hosts_file)
    except FileNotFoundError:
        speed_hosts_update = time.time() + 60
        logger.debug('speed test file: %s not found', speed_hosts_file)


def is_speed_host(host=None):
    update_speed_hosts()
    if host is None:
        return False
    for h in speed_black_hosts:
        if host.endswith(h):
            return False
    for h in speed_hosts:
        if host.endswith(h):
            return True
    return False


def forward_log(_logger, source_conn, dest_conn, data, loglevel=logging.NOTSET, max_len=80):
    if len(data) > max_len:
        _logger.log(loglevel, "forward %s to %s [%s ...%d]", source_conn, dest_conn, data[:max_len], len(data))
    else:
        _logger.log(loglevel, "forward %s to %s [%s]", source_conn, dest_conn, data)


def print_stack_trace(limit=5, out=sys.stdout, *threads):
    frames = sys._current_frames()
    if len(threads) == 0:
        threads = threading.enumerate()
    for th in threads:
        ts_print(th, file=out)
        traceback.print_stack(frames[th.ident], limit=limit, file=out)
        ts_print(file=out)


def errno_from_exception(e):
    """Provides the errno from an Exception object.

    There are cases that the errno attribute was not set so we pull
    the errno out of the args but if someone instatiates an Exception
    without any args you will get a tuple error. So this function
    abstracts all that behavior to give you a safe way to get the
    errno.
    """

    if hasattr(e, 'errno'):
        return e.errno
    elif e.args:
        return e.args[0]
    else:
        return None


def fmt_human_bytes(num):
    if num < 10000:
        return '%d' % num
    k = num / 1024
    if k > 1024:
        return '%.1fM' % (k/1024)
    else:
        return '%.1fK' % k


def fmt_human_time(t, unit='s'):
    if t is None:
        return 'unknown'
    if unit == 's':
        if t < 60:
            return '%02.1f' % t
        m = t / 60
        t %= 60
    else:
        m = t
        t = 0
    if m < 60:
        return '%02d:%02d' % (m, t)
    h = m / 60
    m %= 60
    return '%02d:%02d:%02d' % (h, m, t)


def hostname2short(hostname):
    idx1 = hostname.rfind('.')
    if idx1 > 0:
        idx2 = hostname.rfind('.', 0, idx1-1)
        if idx2 > 0:
            return hostname[:idx2]
        else:
            return hostname[:idx1]
    else:
        return hostname


class FIFOList(list):

    @staticmethod
    class TimedItem(dict):

        def __init__(self, item=None, key=None, **kwargs):
            self._key = key
            if item:
                super().__init__(_item_=item, _in_time_=time.time(), **kwargs)
            else:
                super().__init__(**kwargs)

        @property
        def _item(self):
            if self._key:
                return self._key(self['_item_'])
            return self['_item_']

        @property
        def _in_time(self):
            return self['_in_time_']

        def __hash__(self):
            return self._item.__hash__()

        def __eq__(self, other):
            return self._item.__eq__(other._item)

        def __ge__(self, other):
            return self._item.__ge__(other._item)

        def __gt__(self, other):
            return self._item.__gt__(other._item)

        def __le__(self, other):
            return self._item.__le__(other._item)

        def __lt__(self, other):
            return self._item.__lt__(other._item)

        def __ne__(self, other):
            return self._item.__ne__(other._item)

        def __cmp__(self, other):
            return self._item.__cmp__(other._item)

        def __float__(self):
            return float(self._item)

        def __str__(self):
            return self._item.__str__()

        def __repr__(self):
            return self._item.__repr__()

        def __call__(self):
            return self['_item_']

    def __init__(self, cache_timeout=3600, cache_count=100, item_key=None, *args):
        list.__init__(self)
        self._item_key = item_key
        for item in args:
            if item and isinstance(item, dict):
                list.append(self, FIFOList.TimedItem(item=None, key=self._item_key, **item))
        self._cache_timeout = cache_timeout
        self._cache_count = cache_count

    def _check_timeout_(self):
        try:
            size = list.__len__(self)
            while size > 0:
                head = list.__getitem__(self, 0)
                if (time.time() - head._in_time) > self._cache_timeout or (0 < self._cache_count < size):
                    self.pop(0)
                    size = list.__len__(self)
                else:
                    break
        except:
            pass

    def checkout(self, proxy_name):
        try:
            size = list.__len__(self)
            idx = 0
            while size > idx:
                t = list.__getitem__(self, idx)
                if not hasattr(t, '__call__'):
                    continue
                _t, _f, _n = t()
                if _n.startswith(proxy_name):
                    self.pop(idx)
                    size = list.__len__(self)
                else:
                    idx += 1
        except:
            pass

    def append(self, item):
        self._check_timeout_()
        if item:
            return list.append(self, FIFOList.TimedItem(item, key=self._item_key))

    def insert(self, index, item):
        self._check_timeout_()
        if item:
            return list.insert(self, index, FIFOList.TimedItem(item, key=self._item_key))

    def __iter__(self):
        self._check_timeout_()
        return list.__iter__(self)

    def __len__(self):
        self._check_timeout_()
        return list.__len__(self)

    def __getitem__(self, index):
        item = list.__getitem__(self, index)
        return item._item

    def __setitem__(self, index, item):
        self._check_timeout_()
        return list.__setitem__(self, index, FIFOList.TimedItem(item, key=self._item_key))


class FIFOCache(dict):

    def __init__(self, cache_timeout=1800, lru=False, **kwargs):
        dict.__init__(self, **kwargs)
        self.keys_in_time = {}
        self._lru = lru
        self.cache_timeout = cache_timeout

    def _check_timeout(self, key):
        if key in self.keys_in_time:
            in_time = self.keys_in_time[key]
            if (time.time() - in_time) >= self.cache_timeout:
                del self[key]
                return True
        return False

    def __contains__(self, key):
        c = dict.__contains__(self, key)
        if c:
            return not self._check_timeout(key)
        return c

    def __delitem__(self, key):
        dict.__delitem__(self, key)
        if key in self.keys_in_time:
            del self.keys_in_time[key]

    def __getitem__(self, key):
        value = dict.__getitem__(self, key)
        if self._lru:
            self.keys_in_time[key] = time.time()
        else:
            self._check_timeout(key)
        return value

    def __setitem__(self, key, value):
        dict.__setitem__(self, key, value)
        self.keys_in_time[key] = time.time()


class MyThreadPoolExecutor(concurrent.futures.ThreadPoolExecutor):

    def __init__(self, max_workers, pool_name=None, order_by_func=False):
        super().__init__(max_workers=max_workers)
        self._pool_name = pool_name
        self._pool_thread_id = 0
        self._func_map = {}
        self._rlock = threading.RLock()
        self._order_by_func = order_by_func

    def shutdown(self, timeout=default_timeout):
        for key in self._func_map:
            _func_queue = self._func_map[key]
            _func_queue.put('QUIT_by_shutdown')
        super().shutdown(wait=False)
        for t in self._threads:
            t.join(timeout=timeout)
            if t.is_alive():
                out = StringIO()
                print_stack_trace(10, out, t)
                logger.warning('%s QUIT timeout[%d]:\n%s', t.name, timeout, out.getvalue())
            # else:
            #     logger.debug('%s QUIT', t.name)

    def submit(self, func, *args, **kwargs):
        if 'thread_name' in kwargs:
            thread_name = kwargs['thread_name']
        else:
            thread_name = None

        if self._order_by_func:
            func_hash_key = hash('%s/%s/%s' % (func, args, kwargs)) % self._max_workers
            future = concurrent.futures.Future()
            with self._rlock:
                if func_hash_key in self._func_map:
                    func_queue = self._func_map[func_hash_key]
                    func_queue.put((func, args, kwargs, future))
                else:
                    func_queue = queue.Queue()
                    func_queue.put((func, args, kwargs, future))
                    self._func_map[func_hash_key] = func_queue
                    super().submit(self._func_wrapper, thread_name, func_queue, func_hash_key)
            return future
        else:
            return super().submit(self._func_wrapper, thread_name, None, None, func, *args, **kwargs)

    def _func_wrapper(self, thread_name, func_queue, func_hash_key, func=None, *args, **kwargs):
        if thread_name:
            threading.current_thread().name = thread_name
        elif self._pool_name:
            if not threading.current_thread().name.startswith(self._pool_name):
                if self._max_workers == 1:
                    threading.current_thread().name = '%s' % self._pool_name
                else:
                    with self._rlock:
                        threading.current_thread().name = '%s#%d' % (self._pool_name, self._pool_thread_id)
                        self._pool_thread_id += 1
        logger.log(5, '%s START', threading.current_thread().name)
        if self._order_by_func:
            self._func_wrapper_by_order(func_queue, func_hash_key)
            logger.log(5, '%s STOP', threading.current_thread().name)
        else:
            try:
                result = func(*args, **kwargs)
                logger.log(5, '%s STOP', threading.current_thread().name)
                return result
            except BaseException as ex:
                logger.info('%s STOP with %s: %s', threading.current_thread().name, clazz_fullname(ex), ex)
                raise ex

    def _func_wrapper_by_order(self, func_queue, func_hash_key):
        while True:
            _future = None
            try:
                _work = func_queue.get(timeout=1)
                if isinstance(_work, tuple):
                    _fn, _args, _kwargs, _future = _work
                else:
                    logger.info('_work=%s', _work)
                    break
                logger.log(5, '%s WORKING', threading.current_thread().name)
                result = _fn(*_args, **_kwargs)
                _future.set_result(result)
                logger.log(5, '%s WORK DONE', threading.current_thread().name)
            except queue.Empty:
                with self._rlock:
                    if func_queue.qsize() == 0:
                        del self._func_map[func_hash_key]
                        break
                    else:
                        continue
            except BaseException as ex:
                if _future:
                    _future.set_exception(ex)
                logger.info('%s WORK DONE with %s: %s', threading.current_thread().name, clazz_fullname(ex), ex)


def forward_forever(connection, peer_conn, is_responsed=False, stop_func=None, on_data_recv=None, on_idle=None) -> (bytes, float):
    idle_count = 0
    # idle_start = time.time()
    first_response_time = None
    while True:
        data = None  # type: bytes
        try:
            data = yield from connection.reader.read(read_timeout=1)
            if data and not peer_conn.is_closing and (stop_func is None or not stop_func(data)):
                if on_data_recv:
                    on_data_recv(data)
                if first_response_time is None:
                    first_response_time = time.time()
                is_responsed = True
                peer_conn.writer.write(data)
                yield from peer_conn.writer.drain()
                forward_log(logger, connection, peer_conn, data)
                # idle_start = time.time()
                idle_count = 0
            else:
                break
        except asyncio.TimeoutError:
            idle_time = connection.idle_time  # time.time() - idle_start
            if idle_time > close_on_idle_timeout:
                logger.debug("%s going to close for idle#%d timeout %.0f seconds", connection, idle_count, idle_time)
                break
            if peer_conn.is_closing:
                logger.debug("%s going to close for peer %s is closing", connection, peer_conn)
                break
            _idle_count = int(idle_time) % default_timeout
            if _idle_count != 0 or idle_time < 1:  # idle_time 接近1但<1时，int()直接截成0，使 _idle_count == 0，故增加条件 idle_time < 1
                # logger.log(5, "%s idle#%d.%d for %.1f second", connection, idle_count, _idle_count, idle_time)
                continue
            if not is_responsed:
                logger.info("%s response timeout %f seconds [#%d]", connection, idle_time, _idle_count)
                connection.response_timeout = True
                break
            else:
                if on_idle is None or on_idle(connection, peer_conn, is_responsed):
                    idle_count += 1
                    logger.debug("%s IDLE#%d for %.0f seconds", connection, idle_count, idle_time)
                else:
                    logger.debug("%s IDLE#%d for %.0f seconds and going to close", connection, idle_count, idle_time)
                    break
        except ConnectionError as conn_err:
            logger.info("forward_forever(%s) %s: %s", connection, clazz_fullname(conn_err), conn_err)
            break
        except OSError as ex:
            logger.warning("forward_forever(%s) %s: %s", connection, clazz_fullname(ex), ex)
            break
        except Exception as ex:
            logger.error("forward_forever(%s) %s: %s", connection, clazz_fullname(ex), ex)
            break
        except BaseException as ex:
            logger.exception("forward_forever(%s) %s: %s", connection, clazz_fullname(ex), ex)
            break
    if not is_responsed:
        peer_conn.close()
    return data, first_response_time


