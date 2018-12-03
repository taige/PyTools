import asyncio
import json
import logging
import os
import socket
import time
from io import BytesIO

try:
    from shadowsocks.cryptor import Cryptor
except ImportError:
    from shadowsocks.encrypt import Encryptor as Cryptor

from tsproxy import httphelper2 as httphelper
from tsproxy import common, streams, topendns, str_datetime

logger = logging.getLogger(__name__)

PEER_CONNECTION = 'proxy.PEER_CONNECTION'
PROXY_NAME = 'proxy.PROXY_NAME'


class ProxyStat(dict):
    global_resp_time = None
    global_tp90_len = 0
    global_resp_count = 0
    global_tp90_cache = 0
    global_tp90_cache_time = 0

    global_tp90_inc = 0.0
    global_last_tp90 = 0.0
    global_tp90_inc_time = 0

    def __init__(self, proxy_monitor=None, **kwargs):
        if 'resp_time' in kwargs:
            del kwargs['resp_time']
        if 'proxy_count' in kwargs:
            del kwargs['proxy_count']
        super().__init__(**kwargs)
        self.proxy_monitor = proxy_monitor
        self._tp90_len = 0
        self._tp90_cache = 0
        self._tp90_cached_time = 0
        self.connected_used = 100
        self._resp_time = []
        self._resp_cache_time = 0
        self._pc_cache = []
        self._pc_cache_time = 0
        self._proxy_fail_stat = common.FIFOCache(cache_timeout=300)
        self._proxy_timeout_stat = common.FIFOCache(cache_timeout=300)

    def _name(self):
        raise NotImplementedError()

    def _id_str_(self):
        return '%d/%d/%d/%d/%d/%d/%f/%f/%s' \
               % (self.proxy_count, self.total_count, self.fail_count, self.total_fail,
                  self.last_tp90, self.error_count, self._error_time, self.head_time, self.resp_time)

    def __eq__(self, other):
        if not isinstance(other, ProxyStat):
            return False
        return self._id_str_() == other._id_str_()

    def __hash__(self):
        return hash(self._id_str_())

    # convert ProxyStat to dict properties:

    @property
    def down_speed(self):
        if 'down_speed' not in self or 'down_speed_settime' not in self or (time.time() - self['down_speed_settime']) > common.speed_lifetime:
            self['down_speed'] = 0
        return self['down_speed']

    @property
    def down_speed_settime(self):
        return self['down_speed_settime'] if 'down_speed_settime' in self else 0

    @property
    def realtime_speed(self):
        if 'realtime_speed' not in self:
            self['realtime_speed'] = 0
        #     self['realtime_speed_time'] = time.time()
        # if (time.time() - self['realtime_speed_time']) > common.speed_lifetime:
        #     return 0
        return self['realtime_speed']

    @realtime_speed.setter
    def realtime_speed(self, r_speed):
        _now = int(time.time())
        _s = 0 if 'realtime_speed' not in self else self['realtime_speed']
        if 'realtime_speed_time' in self and self['realtime_speed_time'] == _now:
            self['realtime_speed'] = _s + r_speed
        else:
            self['realtime_speed'] = r_speed
        self['realtime_speed_time'] = _now

    def set_realtime_speed(self, r_speed):
        self['realtime_speed'] = r_speed
        self['realtime_speed_time'] = int(time.time())

    @down_speed.setter
    def down_speed(self, d_speed):
        # 10 分钟内的速度取平均值
        if d_speed < 0 or 'down_speed_settime' not in self or (time.time() - self['down_speed_settime']) > 600:
            self['down_speed'] = d_speed
        else:
            self['down_speed'] = (self.down_speed + d_speed)/2
        self['down_speed_settime'] = time.time()

    @property
    def pause(self):
        if 'pause' not in self:
            self['pause'] = False
        return self['pause']

    @pause.setter
    def pause(self, c):
        self['pause'] = c
        logger.info("%s %s", self, 'paused' if c else 'resumed')

    @property
    def sess_count(self):
        if 'sess_count' not in self:
            self['sess_count'] = 0
        return self['sess_count']

    @sess_count.setter
    def sess_count(self, c):
        self['sess_count'] = c

    def _proxy_count_cache(self):
        if time.time() - self._pc_cache_time > 1:
            self._pc_cache = []
            for t in ProxyStat.global_resp_time:
                if not hasattr(t, '__call__'):
                    continue
                _t, _f, _n = t()
                if _n.startswith(self._name()):
                    self._pc_cache.append(_f)
            self._pc_cache_time = time.time()

    @property
    def proxy_count(self):
        self._proxy_count_cache()
        return len(self._pc_cache)

    @property
    def total_count(self):
        if 'total_count' not in self:
            self['total_count'] = {}
        if isinstance(self['total_count'], int):
            if 'resolved_addr' in self:
                addr = self['resolved_addr'][0]
                if isinstance(addr, list):
                    self['total_count'] = {self['resolved_addr'][0][0]: self['total_count']}
                else:
                    self['total_count'] = {self['resolved_addr'][0]: self['total_count']}
            else:
                return self['total_count']
        c = 0
        for k in self['total_count']:
            c += self['total_count'][k]
        return c

    # @total_count.setter
    # def total_count(self, c):
    #     if 'total_count' not in self:
    #         self.sess_count = c
    #     else:
    #         _sc = c - self['total_count']
    #         self.sess_count += _sc
    #     self['total_count'] = c

    @property
    def fail_count(self):
        self._proxy_count_cache()
        _fail_c = 0
        for f in self._pc_cache:
            if f:
                _fail_c += 1
        return _fail_c

    @property
    def total_fail(self):
        if 'total_fail' not in self:
            self['total_fail'] = {}
        if isinstance(self['total_fail'], int):
            if 'resolved_addr' in self:
                addr = self['resolved_addr'][0]
                if isinstance(addr, list):
                    self['total_fail'] = {self['resolved_addr'][0][0]: self['total_fail']}
                else:
                    self['total_fail'] = {self['resolved_addr'][0]: self['total_fail']}
            else:
                return self['total_fail']
        c = 0
        for k in self['total_fail']:
            c += self['total_fail'][k]
        return c

    # @total_fail.setter
    # def total_fail(self, c):
    #     self['total_fail'] = c

    @property
    def error_count(self):
        if 'error_count' not in self:
            self['error_count'] = 0
        return self['error_count']

    @error_count.setter
    def error_count(self, c):
        self['error_count'] = c

    @property
    def _error_time(self):
        if '_error_time' not in self:
            self['_error_time'] = 0
        return self['_error_time']

    @_error_time.setter
    def _error_time(self, c):
        self['_error_time'] = c

    @property
    def head_time(self):
        if 'head_time' not in self:
            self['head_time'] = 0
        return self['head_time']

    @head_time.setter
    def head_time(self, c):
        self['head_time'] = c
        self['sort_key_onhead'] = self.sort_key

    @property
    def sort_key_decrement(self):
        if 'sort_key_onhead' not in self:
            self['sort_key_onhead'] = self.sort_key
        dec = self['sort_key_onhead'] - self.sort_key
        if self['sort_key_onhead'] == 0:
            return 0, 0
        else:
            return (dec / self['sort_key_onhead']), self['sort_key_onhead']

    @property
    def sort_key(self):
        p = self
        if 'sort_key_time' in p and 'sort_key' in p:
            if time.time() - p['sort_key_time'] < 1:
                return p['sort_key']
        global_tp90 = round(ProxyStat.calc_tp90(), 1)
        if global_tp90 == 0:
            return 0
        if p.down_speed > 0:
            # f1 成功率^3
            f1 = 1 - (p.fail_rate if p.tp90_len == 0 or p.proxy_count >= common.tp90_calc_count*0.9 else p.total_fail_rate)
            f1 *= f1 * f1
            # f2 连接速度偏差 率
            f2 = (global_tp90 - round(p.tp90, 1)) / global_tp90 + (0.9 ** 4)
            if f2 > 0:
                f2 **= 0.25
            else:
                f2 = 0
            # f2 = p.tp90 / global_tp90
            # f2 *= f2
            # f2 = 1 - f2
            p['sort_key'] = round(p.down_speed/102400) * f1 * f2 * 10
        else:
            p['sort_key'] = 0
        p['sort_key_time'] = time.time()
        return p['sort_key']

    @property
    def resp_time(self):
        if time.time() - self._resp_cache_time > 0.5:
            self._resp_time = []
            for t in ProxyStat.global_resp_time:
                if not hasattr(t, '__call__'):
                    continue
                _t, _f, _n = t()
                if _n.startswith(self._name()) and _t >= 0:
                    self._resp_time.append(_t)
            self._resp_cache_time = time.time()
        return self._resp_time

    @property
    def last_tp90(self):
        if 'last_tp90' not in self:
            self['last_tp90'] = 0
        return self['last_tp90']

    @last_tp90.setter
    def last_tp90(self, c):
        self['last_tp90'] = c

    # END of ProxyStat to dict properties convert

    @staticmethod
    def get_global_tp90_inc():
        if (time.time() - ProxyStat.global_tp90_inc_time) < 0.5 and ProxyStat.global_last_tp90 > 0:
            return '%s%.1f' % ('+' if ProxyStat.global_tp90_inc >= 0 else '', ProxyStat.global_tp90_inc)
        ProxyStat.global_tp90_inc = ProxyStat.calc_tp90() - ProxyStat.global_last_tp90
        ProxyStat.global_last_tp90 = ProxyStat.calc_tp90()
        ProxyStat.global_tp90_inc_time = time.time()
        return '%s%.1f' % ('+' if ProxyStat.global_tp90_inc >= 0 else '', ProxyStat.global_tp90_inc)

    @staticmethod
    def calc_tp90(time_list=None, time_count=None):
        is_global = False
        if time_list is None:
            if (time.time() - ProxyStat.global_tp90_cache_time) < 0.5 and ProxyStat.global_tp90_cache > 0:
                return ProxyStat.global_tp90_cache
            is_global = True
            time_list = []
            for t in ProxyStat.global_resp_time:
                if not hasattr(t, '__call__'):
                    continue
                _t, _f, _n = t()
                if _t >= 0:
                    time_list.append(_t)
            time_count = len(time_list)
        c90 = int(time_count * 0.1)
        count = 0
        tp90 = 0.0
        for t in sorted(time_list, reverse=True):
            count += 1
            if count > c90:
                tp90 = float(t)
                # logger.debug("count = %d, c90 = %d, tp90 = %.1f", count, c90, tp90)
                break
        if is_global:
            if tp90 < 0.1 and ProxyStat.global_tp90_cache > 0:
                ProxyStat.global_tp90_cache_time = time.time()
                return ProxyStat.global_tp90_cache
            ProxyStat.global_tp90_cache = tp90
            ProxyStat.global_tp90_len = time_count
            ProxyStat.global_resp_count = len(ProxyStat.global_resp_time)
            ProxyStat.global_tp90_cache_time = time.time()
        return tp90

    @property
    def fail_rate(self):
        if self.total_count > 10:
            # total count > 10 and all failed
            if self.total_count == self.total_fail:
                return 1.0
        if self.proxy_count <= 10:
            # try count <= 10, fail_rate can't calculate by fail/count, may try more
            if self.fail_count >= 5:
                # fail count >= 5, fail_rate too high
                return self.fail_count/self.proxy_count
        return self.fail_count / self.proxy_count if self.proxy_count > 10 else 0.0

    @property
    def total_fail_rate(self):
        if self.total_count <= 10:
            if self.total_fail >= 5:
                return self.total_fail/self.total_count
        return self.total_fail / self.total_count if self.total_count > 10 else 0.0

    @property
    def tp90_increment(self):
        inc = self.tp90 - self.last_tp90
        if self.last_tp90 < 0.1:
            return 0.0, self.last_tp90, inc
        else:
            return (inc / self.last_tp90), self.last_tp90, inc

    @property
    def _tp90(self):
        return self._tp90_cache

    @_tp90.setter
    def _tp90(self, _tp90):
        self._tp90_cache = _tp90
        self._tp90_cached_time = time.time()

    @property
    def error_time(self):
        return time.time() - self._error_time

    @error_time.setter
    def error_time(self, err_time):
        # self.error_count += 1
        self._error_time = err_time

    def _tp90_cache_(self):
        if (time.time() - self._tp90_cached_time) < 0.5 and self._tp90 > 0:
            return
        dict_time = self.resp_time
        count = len(self.resp_time)
        t = ProxyStat.calc_tp90(dict_time, count)
        if t < 0.1 and self._tp90 > 0:
            return
        self._tp90 = t
        self._tp90_len = count

    @property
    def tp90_len(self):
        self._tp90_cache_()
        return self._tp90_len

    @property
    def tp90(self):
        self._tp90_cache_()
        return self._tp90

    @property
    def factor(self):
        total_fail_rate = self.total_fail_rate
        times_fail_rate = common.max_times_fail_rate if total_fail_rate >= 1 else 1 / (1 - total_fail_rate)
        if times_fail_rate > common.max_times_fail_rate:
            times_fail_rate = common.max_times_fail_rate
        if self.total_count == self.total_fail:
            times_fail_rate = common.max_times_fail_rate * 10
        return self.total_count * times_fail_rate

    def save_last_tp90(self):
        self.last_tp90 = self.tp90

    def reset_stat_info(self):
        self.save_last_tp90()
        self.sess_count = 0

    def update_proxy_stat(self, connection, resp_time, target_host=None, proxy_ip=None, loginfo="FIRST response", proxy_fail=False, proxy_timeout=False, proxy_name=None, **kwargs):
        if connection is not None:
            target_host = connection.target_host
            proxy_ip = connection.raddr
        logger.debug('proxy for %s %s use %.2f sec', target_host, loginfo, resp_time)
        self._update_stat_info(target_host, resp_time, self_ip=proxy_ip, proxy_fail=proxy_fail, proxy_timeout=proxy_timeout, proxy_name=proxy_name)

    def get_proxy_stat(self, target_host, proxy_fail=False, proxy_timeout=False, resp_time=False):
        """ 如果代理响应超时或无响应，返回True """
        # TODO 先简单实现
        return self._proxy_fail_stat.get(target_host, proxy_fail) or self._proxy_timeout_stat.get(target_host, proxy_timeout)

    def _update_stat_info(self, target_host, resp_time, self_ip, proxy_fail=False, proxy_timeout=False, proxy_name=None):
        # with self.rlock:
        self._proxy_fail_stat[target_host] = proxy_fail
        self._proxy_timeout_stat[target_host] = proxy_timeout
        if not self_ip:
            if 'resolved_addr' in self:
                self_ip = self['resolved_addr'][0][0]
            else:
                logger.warning("'resolved_addr' not in %s and self_ip is None", self)
                self_ip = '0.0.0.0'
        if proxy_fail or proxy_timeout:
            if self_ip in self['total_fail']:
                self['total_fail'][self_ip] += 1
            else:
                self['total_fail'][self_ip] = 1
        if self_ip in self['total_count']:
            self['total_count'][self_ip] += 1
        else:
            self['total_count'][self_ip] = 1

        if not proxy_fail:
            t = resp_time
        else:
            t = -1
        ProxyStat.global_resp_time.append([t, proxy_fail or proxy_timeout, '%s/%s' % (self._name(), self_ip)])
        count = self.proxy_count
        # end of self.rlock

        if self.proxy_monitor and proxy_name is None:
            if (count % int(common.hundred/10)) == 0:
                self.proxy_monitor.check(self, '(count[%d] %% int(%d/10)) == 0' % (count, common.hundred))
                # pass
            elif self.fail_rate > common.fail_rate_threshold:
                self.proxy_monitor.check(self, 'self.fail_rate[%.1f] > common.fail_rate_threshold[%.1f]' % (self.fail_rate, common.fail_rate_threshold))
            elif (proxy_fail or proxy_timeout) and self.total_count == self.total_fail:
                self.proxy_monitor.check(self, '(proxy_fail[%s] or proxy_timeout[%s]) and self.total_count[%d] == self.total_fail[%d]'
                                         % (proxy_fail, proxy_timeout, self.total_count, self.total_fail))


class Proxy(ProxyStat):
    # conn.flag
    # 1 - 4: sub-class define
    # 5: first request sent
    # 6: first response recv

    def __init__(self, proxy_monitor, hostname, server_port=0, short_hostname=None, json_config=None, **kwargs):
        self.hostname = hostname
        ProxyStat.__init__(self, proxy_monitor, **kwargs)
        self['__class__'] = self.__class__.__name__
        self.port = server_port
        self.short_hostname = short_hostname  # if short_hostname else hostname.split('.', 1)[0]

        self.json_file_mod = 0
        self.last_json_check = 0
        if json_config:
            self.json_config = json_config
            self._json_config_full_path = common.lookup_conf_file(json_config)
            self.update_json_config(raise_on_fnfe=True)
        else:
            self.json_config = None

        if 'short_hostname' not in self or not self['short_hostname']:
            self.short_hostname = common.hostname2short(hostname)

    def _name(self):
        return self.hostname

    def __repr__(self):
        s = '<%s://%s/%d tp90:%.1f/%d count:%d/f%d//%d/f%d%s>' % \
            (self.protocol, self.short_hostname, self.port, self.tp90, self._tp90_len, self.total_count, self.total_fail,
             self.proxy_count, self.fail_count,
             '%s%s%s' % ('' if self.down_speed <= 0 else ' speed=%sB/S' % common.fmt_human_bytes(self.down_speed),
                         ' %s' % format(int(self.sort_key), ',') if 'sort_key' in self else '', '=' if self.pause else '>'))
        return s

    def _id_str_(self):
        return '%s/%s/%d' % (super()._id_str_(), self.hostname, self.port)

    @property
    def hostname(self):
        if 'hostname' not in self:
            return None
        return self['hostname']

    @hostname.setter
    def hostname(self, c):
        self['hostname'] = c

    @property
    def port(self):
        if 'server_port' not in self:
            return 0
        return self['server_port']

    @port.setter
    def port(self, c):
        self['server_port'] = c

    @property
    def short_hostname(self):
        if 'short_hostname' not in self or not self['short_hostname']:
            return common.hostname2short(self.hostname)
        return self['short_hostname']

    @short_hostname.setter
    def short_hostname(self, c):
        self['short_hostname'] = c

    @property
    def json_config(self):
        if 'json_config' not in self:
            return None
        return self['json_config']

    @json_config.setter
    def json_config(self, c):
        self['json_config'] = c

    @property
    def protocol(self):
        raise NotImplementedError()

    @asyncio.coroutine
    def init_connection(self, connection, host, port, **kwargs):
        pass

    def print_info(self, index=None, force_print=True, out=None, high_light=False, max_total_count=6, max_sess_count=3):
        if self.total_count > 0 or self.proxy_count > 0 or force_print:
            fr1 = 100*(0 if self.total_count == 0 else self.total_fail/self.total_count)
            fr2 = 100*(0 if self.proxy_count == 0 else self.fail_count/self.proxy_count)
            count_fmt1 = '%%%ds' % len(format(max_total_count, ','))
            count_fmt2 = '%%%ds' % len(format(max_sess_count, ','))
            output = "PROXY%4s %s %-20s %-14s count=%s/%s|%s/%s" % \
                     ('' if index is None else '[%2d]' % index,
                      '~' if self.pause and self.hostname in self.proxy_monitor.auto_pause_list else '=' if self.pause else '>',
                      '%s://%s:%d' % (self.protocol, self.short_hostname, self.port),
                      'tp90=%.1fs/%d' % (self.tp90, self.tp90_len),
                      count_fmt1 % format(self.proxy_count if self.total_count == 0 else self.total_count, ','),
                      ('f%.0f.%%' if fr1 > 9.95 else 'f%.1f%%') % fr1,
                      count_fmt2 % format(self.proxy_count, ','),
                      ('f%.0f.%%' if fr2 > 9.95 else 'f%.1f%%') % fr2
                      )
            if 'down_speed_settime' in self and (time.time() - self['down_speed_settime']) < 24*3600:
                output += ' S=%s' % str_datetime(timestamp=self['down_speed_settime'], fmt='%H:%M:%S,%f', end=12)
            # if (time.time() - self.head_time) < 24*3600 or index == 0:
            #     output += ' H=%s' % str_datetime(timestamp=self.head_time, fmt='%H:%M:%S,%f', end=12)
            if self.down_speed > 0:
                output += ' speed=%sB/S %s' % (common.fmt_human_bytes(self.down_speed), format(int(self.sort_key), ',') if 'sort_key' in self else '')
            if high_light:
                output = '\x1b[1;31;48m%s\x1b[0m' % output
            if out:
                out.write('%s\r\n' % output)
            else:
                logger.info(output)
            return 1
        else:
            return 0

    @staticmethod
    def load_json_config(json_file):
        with open(json_file, 'r') as f:
            return json.load(f)

    def update_json_config(self, raise_on_fnfe=False):
        if self.json_config is None:
            return
        if (time.time() - self.last_json_check) < 1:
            return
        self.last_json_check = time.time()
        try:
            mtime = os.stat(self._json_config_full_path).st_mtime
            if self.json_file_mod < mtime:
                # with self.rlock:
                if self.json_file_mod >= mtime:
                    return
                config = self.load_json_config(self._json_config_full_path)
                self.read_json_config(config)
                self.json_file_mod = mtime
                logger.info('%s reloaded', self._json_config_full_path)
        except FileNotFoundError as fnfe:
            logger.warning('config file: %s not found', self._json_config_full_path)
            if raise_on_fnfe:
                raise fnfe

    def read_json_config(self, config):
        pass

    @property
    def addr(self):
        self.update_json_config()
        return self.hostname, self.port

    @property
    def resolved_addr(self):
        if 'resolved_addr' in self:
            if not isinstance(self['resolved_addr'][0], list):
                self['resolved_addr'][0] = [self['resolved_addr'][0]]
            return self['resolved_addr']
        else:
            return [self.hostname], self.port

    @resolved_addr.setter
    def resolved_addr(self, addr):
        if 'resolved_addr' in self:
            if sorted(addr[0]) != sorted(self.resolved_addr[0]):  # ip 有变更
                _old_info = '%s' % self
                # remove domain speed map data
                for ip in self.resolved_addr[0]:
                    if ip not in addr[0]:
                        # 删除speedup数据
                        self.proxy_monitor.remove_proxy_from_domain_speed(self, ip)
                        # 删除resp_time数据
                        ProxyStat.global_resp_time.checkout('%s/%s' % (self.hostname, ip))
                        # 重置统计数据
                        self['total_count'].pop(ip, None)
                        self['total_fail'].pop(ip, None)
                logger.info('proxy(%s) ip changed, from %s/%s to %s/%s, ', self.short_hostname, _old_info, self.resolved_addr[0], self, addr[0])
        self['resolved_addr'] = addr

    @property
    def forward_flag(self):
        return 5

    def on_idle(self, connection, peer_conn, is_responsed):
        return 'Proxy-Name' in connection or self.proxy_monitor is None or self.proxy_monitor.head_proxy.hostname == self.hostname

    def __call__(self, connection, peer_conn):
        logger.info("forward-%s(%s) ESTABLISHED", self.protocol, connection)
        if True or common.is_speed_host(connection.target_host):
            # experimental code
            # _mutable_data_count: [0]recv_bytes,  [1]written_time(starting_time), [2]recv_used_time,
            #                      [3]last_timing, [4]speed output time
            _mutable_data_count = [0, connection.written_time, 0, 0, int(time.time()), 0, 0]
            # logger.debug('%s _mutable_data_count: %s', connection, _mutable_data_count[:2])

            def _log_speed(_data):
                _data_len = len(_data)
                if connection.written_time != _mutable_data_count[1]:
                    _mutable_data_count[1] = connection.written_time
                    _mutable_data_count[2] += _mutable_data_count[3]
                    _mutable_data_count[6] = _mutable_data_count[0]
                    # logger.debug('%s _mutable_data_count: %s', connection, _mutable_data_count[:4])
                _mutable_data_count[0] += _data_len
                _mutable_data_count[3] = time.time() - connection.written_time
                connection['_realtime_speed_'] = _mutable_data_count[0] / (_mutable_data_count[2] + _mutable_data_count[3])
                if int(time.time()) != _mutable_data_count[4]:  # and '_realtime_speed_' in connection:
                    _mutable_data_count[4] = int(time.time())
                    _mutable_data_count[5] = connection['_realtime_speed_']
                    self.realtime_speed = _mutable_data_count[5]
                    logger.debug('%s realtime_speed: %sB/S %sB/S %s %s', connection, common.fmt_human_bytes(connection['_realtime_speed_']), common.fmt_human_bytes(self.realtime_speed),
                                 common.fmt_human_bytes(_mutable_data_count[0]), common.fmt_human_bytes(_mutable_data_count[0]-_mutable_data_count[6]))
                # else:
                #     logger.debug('%s realtime_speed %d/%.1f: %sB/S', connection, _data_len, _mutable_data_count[3], common.fmt_human_bytes(connection['_realtime_speed_']))
        else:
            _log_speed = None
        _, first_res_time = yield from common.forward_forever(connection, peer_conn, on_data_recv=_log_speed, on_idle=self.on_idle)
        if connection.response_timeout:
            self.update_proxy_stat(connection, time.time() - connection.create_time, loginfo="response timeout", proxy_timeout=True)
        elif not first_res_time:
            self.update_proxy_stat(connection, time.time() - connection.create_time, loginfo="be closed with no response", proxy_fail=True)
        else:
            self.update_proxy_stat(connection, first_res_time - connection.create_time)
            if _log_speed is not None:
                connection['_realtime_speed_'] = _mutable_data_count[0] / (_mutable_data_count[2] + _mutable_data_count[3])
                if int(time.time()) != _mutable_data_count[4]:
                    _mutable_data_count[4] = int(time.time())
                    self.realtime_speed = connection['_realtime_speed_']
                else:
                    self.realtime_speed = connection['_realtime_speed_'] - _mutable_data_count[5]
                # if connection['_realtime_speed_'] > self.down_speed or (time.time() - self['down_speed_settime']) > common.default_timeout:
                #     self.down_speed = connection['_realtime_speed_']
                # if self.down_speed == 0:
                #     self.down_speed = self.realtime_speed
                # self.down_speed = (self.down_speed + self.realtime_speed) / 2
                logger.debug('%s realtime_speed2: %sB/S %sB/S %s %s', connection, common.fmt_human_bytes(connection['_realtime_speed_']), common.fmt_human_bytes(self.realtime_speed),
                             common.fmt_human_bytes(_mutable_data_count[0]), common.fmt_human_bytes(_mutable_data_count[0]-_mutable_data_count[6]))
        logger.info("forward-%s(%s) DONE", self.protocol, connection)


class DirectForward(Proxy):

    def __init__(self):
        super().__init__(None, 'D')

    def _update_stat_info(self, target_host, resp_time, self_ip, proxy_fail=False, proxy_timeout=False, proxy_name=None):
        pass

    @property
    def protocol(self):
        return 'direct'


def comps_connect_request(addr, port, socks5=False):
    h = b'\x05\x01\x00' if socks5 else b''
    if topendns.is_ipv4(addr):
        h += b'\x01' + socket.inet_aton(addr)
    elif topendns.is_ipv6(addr):
        h += b'\x04' + socket.inet_pton(socket.AF_INET6, addr)
    else:
        h += b'\x03' + len(addr).to_bytes(1, byteorder='big') + addr.encode()
    return h + port.to_bytes(2, byteorder='big')


class ProxyConnectInitError(Exception):

    def __init__(self, flag, message=None):
        self.flag = flag
        self.message = message


class Socks5Proxy(Proxy):
    # conn.flag
    # 1: socks5 hello request sent
    # 2: socks5 hello response recv
    # 3: socks5 connect request sent
    # 4: socks5 connect response recv
    # 5: first request sent
    # 6: first response recv

    SOCKS5_CONN_REP = {
        0x00: 'succeeded',
        0x01: 'general SOCKS server failure',
        0x02: 'connection not allowed by ruleset',
        0x03: 'Network unreachable',
        0x04: 'Host unreachable',
        0x05: 'Connection refused',
        0x06: 'TTL expired',
        0x07: 'Command not supported',
        0x08: 'Address type not supported'
    }

    def __init__(self, proxy_monitor, hostname, server_port, short_hostname=None, **kwargs):
        super().__init__(proxy_monitor, hostname, server_port, short_hostname=short_hostname, **kwargs)

    @property
    def protocol(self):
        return 'socks5'

    def init_connection(self, connection, host, port, **kwargs):
        flag = 0
        try:
            hello_req = b'\x05\x01\x00'
            connection.writer.write(hello_req)
            # yield from connection.writer.drain()
            flag = 1
            yield from connection.reader.read_bytes(size=2, exactly=True)
            flag = 2
            conn_req = comps_connect_request(host, port, socks5=True)
            connection.writer.write(conn_req)
            # yield from connection.writer.drain()
            flag = 3
            conn_res_header = yield from connection.reader.read_bytes(size=5, exactly=True)
            rep = conn_res_header[1]
            if rep != 0x00:
                err = 'unknown(%x)' % rep if rep not in self.SOCKS5_CONN_REP else self.SOCKS5_CONN_REP[rep]
                logger.warning('%s socks5 proxy connect fail: %s', connection, err)
                raise ProxyConnectInitError(flag, 'connect response error: %s' % err)
            if conn_res_header[3] == 0x01:  # ipv4
                l = 3
            elif conn_res_header[3] == 0x04:  # ipv6
                l = 15
            elif conn_res_header[3] == 0x03:  # domain-name
                l = int.from_bytes(conn_res_header[4], byteorder='big')
            else:
                raise ProxyConnectInitError(flag, 'unknown ATYP=%x' % conn_res_header[3])
            yield from connection.reader.read_bytes(size=l+2, exactly=True)
            logger.debug('%s socks5 proxy connected, use %.3f sec', connection, time.time() - connection.create_time)
            flag = 4
        except ProxyConnectInitError:
            raise
        except Exception as ex:
            logger.exception('%s init socks5 connect fail@%d %s: %s', connection, flag, common.clazz_fullname(ex), ex)
            raise ProxyConnectInitError(flag, 'init socks5 connect fail@%d' % flag)


class ShadowsocksProxy(Proxy):
    # conn.flag
    # 3: connected
    # 4: 'CONNECT' command sent
    # 5: first request sent
    # 6: first response recv

    ENCRYPTOR = 'ShadowsocksProxy.ENCRYPTOR'

    def __init__(self, proxy_monitor, hostname, **kwargs):
        self.password = None
        self.method = None
        if 'password' in kwargs:
            self.password = kwargs['password']
        if 'method' in kwargs:
            self.method = kwargs['method']
        super().__init__(proxy_monitor, hostname=hostname, **kwargs)
        if not self.hostname:
            raise ValueError('hostname not set')
        if not self.port:
            raise ValueError('server_port not set')
        if not self.password:
            raise ValueError('password not set')
        if not self.method:
            raise ValueError('method not set')
        if self.new_encryptor() is None:
            raise Exception('new_encryptor fail')
        self.encoder = ShadowsocksEncoder(self)
        self.decoder = ShadowsocksDecoder(self)

    def _id_str_(self):
        return '%s/%s/%s' % (super()._id_str_(), self.password, self.method)

    @property
    def password(self):
        if 'password' not in self:
            return None
        return self['password']

    @password.setter
    def password(self, c):
        self['password'] = c

    @property
    def method(self):
        if 'method' not in self:
            return None
        return self['method']

    @method.setter
    def method(self, c):
        self['method'] = c

    @property
    def protocol(self):
        return 'shadws'

    def read_json_config(self, config):
        self.port = int(config['server_port'])
        self.password = config['password']
        self.method = config['method']
        if 'server' in config:
            self.hostname = config['server']

    def init_connection(self, connection, host, port, **kwargs):
        flag = 3
        try:
            conn_req = comps_connect_request(host, port)
            connection.writer.write(conn_req)
            yield from connection.writer.drain()
            logger.debug('%s shadowsocks proxy connected, use %.3f sec', connection, time.time() - connection.create_time)
            flag = 4
        except ProxyConnectInitError:
            raise
        except Exception as ex:
            logger.exception('%s init socks5 connect fail@%d %s: %s', connection, flag, common.clazz_fullname(ex), ex)
            raise ProxyConnectInitError(flag, 'init socks5 connect fail@%d' % flag)

    def new_encryptor(self):
        self.update_json_config()
        if self.password and self.method:
            try:
                return Cryptor(self.password, self.method)
            except SystemExit:
                raise Exception('method %s not supported' % self.method)
        else:
            return None

    def get_encryptor(self, conn):
        encryptor = conn.get_attr(self.ENCRYPTOR)
        if encryptor is None:
            encryptor = self.new_encryptor()
            conn.set_attr(self.ENCRYPTOR, encryptor)
            logger.log(5, "new Encryptor for %s", conn)
        return encryptor

    def do_encrypt(self, conn, data):
        encryptor = self.get_encryptor(conn)
        if encryptor:
            # logger.log(5, "%s encrypting %s", conn, data[:10])
            return encryptor.encrypt(data)
        else:
            return data

    def do_decrypt(self, conn, data):
        encryptor = self.get_encryptor(conn)
        if encryptor:
            dec = encryptor.decrypt(data)
            # logger.log(5, "%s decrypt %s => %s", conn, data[:10], dec[:10])
            return dec
        else:
            return data


class ShadowsocksEncoder(streams.Encoder):

    def __init__(self, shadws_proxy):
        self._shadws_proxy = shadws_proxy

    def __call__(self, data, connection):
        enc = self._shadws_proxy.do_encrypt(connection, data)
        logger.log(5, 'do_encrypt %s -> %s', data[:40], enc[:40])
        return enc


class ShadowsocksDecoder(streams.Decoder):

    def __init__(self, shadws_proxy):
        self._shadws_proxy = shadws_proxy

    def __call__(self, connection, read_timeout):
        try:
            data = yield from connection.reader.read_bytes(read_timeout=read_timeout)
            if not data:
                return None
        except (TimeoutError, asyncio.TimeoutError):
            raise
        except Exception as ex:
            logger.exception("%s shadowsocks read fail: %s(%s)", connection, common.clazz_fullname(ex), ex)
            return None
        dec = self._shadws_proxy.do_decrypt(connection, data)
        logger.log(5, 'do_decrypt %s -> %s', data[:40], dec[:40])
        return dec


class HttpRequestRewriter:

    def __call__(self, data, connection):
        if common.KEY_FIRST_HTTP_REQUEST in connection:
            enc = rewrite_http_request(connection.pop(common.KEY_FIRST_HTTP_REQUEST))
            return enc
        return data


class HttpProxy(Proxy):

    def __init__(self, proxy_monitor, hostname, server_port, short_hostname=None, **kwargs):
        super().__init__(proxy_monitor, hostname, server_port, short_hostname=short_hostname, **kwargs)
        self.encoder = HttpRequestRewriter()

    @property
    def protocol(self):
        return 'http'

    def init_connection(self, connection, host, port, request=None, **kwargs):
        if not request:
            return
        flag = 0
        try:
            if request.method != common.HTTPS_METHOD_CONNECT:
                # http request, save the request and do encode at write() phase
                connection[common.KEY_FIRST_HTTP_REQUEST] = request
                return
            else:
                # https request, connect to downstream proxy
                hello_req = rewrite_http_request(request)
            connection.writer.write(hello_req)
            # yield from connection.writer.drain()
            flag = 1
            hello_res = yield from connection.reader.readuntil(b'\r\n\r\n')
            flag = 2
            _http_parser = httphelper.HttpResponseParser()
            response, _ = _http_parser.parse_response(hello_res, common.HTTPS_METHOD_CONNECT)
            flag = 3
            if response.code != 200:
                raise ProxyConnectInitError(flag, 'https proxy res.code=%d/%s' % (response.code, response.reason))
            logger.debug('%s https proxy connected, use %.3f sec', connection, time.time() - connection.create_time)
            flag = 4
        except ProxyConnectInitError:
            raise
        except Exception as ex:
            logger.exception('%s init https connection fail@%d %s: %s', connection, flag, common.clazz_fullname(ex), ex)
            raise ProxyConnectInitError(flag, 'init https connection fail@%d' % flag)


def rewrite_http_request(request):
    buf = BytesIO()
    if request.method == common.HTTPS_METHOD_CONNECT:
        buf.write(('CONNECT %s:%d HTTP/1.1\r\n' % (request.url.hostname, request.url.port)).encode())
    else:
        buf.write(request.request_line.encode() + b'\r\n')
    for key in request.headers:
        if key == 'Proxy-Name':
            continue
        buf.write(key.encode() + b': ' + request.headers[key].encode() + b'\r\n')
    buf.write(b'\r\n')
    buf.write(request.body)
    return buf.getvalue()
