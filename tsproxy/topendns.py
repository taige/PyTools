import logging
import os
import re
import socket
import time

from dns.exception import Timeout
from dns.resolver import NXDOMAIN
from dns.resolver import NoAnswer
from dns.resolver import Resolver

from tsproxy.common import FIFOCache, MyThreadPoolExecutor, lookup_conf_file

logger = logging.getLogger(__name__)

cn_ip_list = []
cn_ip_last_mod = 0

local_dns_query = False

resolver = Resolver('/dev/null')
resolver.nameservers.clear()
resolver.nameservers.append('208.67.220.220')
resolver.nameservers.append('208.67.222.222')
resolver.lifetime = 2
resolver.port = 443

cn_addr_cache = {}
dns_cache = FIFOCache()

ip_regex = re.compile(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$')

ipv6_regex = re.compile(r'(\A([0-9a-f]{1,4}:){1,1}(:[0-9a-f]{1,4}){1,6}\Z)|'
                        r'(\A([0-9a-f]{1,4}:){1,2}(:[0-9a-f]{1,4}){1,5}\Z)|'
                        r'(\A([0-9a-f]{1,4}:){1,3}(:[0-9a-f]{1,4}){1,4}\Z)|'
                        r'(\A([0-9a-f]{1,4}:){1,4}(:[0-9a-f]{1,4}){1,3}\Z)|'
                        r'(\A([0-9a-f]{1,4}:){1,5}(:[0-9a-f]{1,4}){1,2}\Z)|'
                        r'(\A([0-9a-f]{1,4}:){1,6}(:[0-9a-f]{1,4}){1,1}\Z)|'
                        r'(\A(([0-9a-f]{1,4}:){1,7}|:):\Z)|(\A:(:[0-9a-f]{1,4})'
                        r'{1,7}\Z)|(\A((([0-9a-f]{1,4}:){6})(25[0-5]|2[0-4]\d|[0-1]'
                        r'?\d?\d)(\.(25[0-5]|2[0-4]\d|[0-1]?\d?\d)){3})\Z)|'
                        r'(\A(([0-9a-f]{1,4}:){5}[0-9a-f]{1,4}:(25[0-5]|2[0-4]\d|'
                        r'[0-1]?\d?\d)(\.(25[0-5]|2[0-4]\d|[0-1]?\d?\d)){3})\Z)|'
                        r'(\A([0-9a-f]{1,4}:){5}:[0-9a-f]{1,4}:(25[0-5]|2[0-4]\d|'
                        r'[0-1]?\d?\d)(\.(25[0-5]|2[0-4]\d|[0-1]?\d?\d)){3}\Z)|'
                        r'(\A([0-9a-f]{1,4}:){1,1}(:[0-9a-f]{1,4}){1,4}:(25[0-5]|'
                        r'2[0-4]\d|[0-1]?\d?\d)(\.(25[0-5]|2[0-4]\d|[0-1]?\d?\d))'
                        r'{3}\Z)|(\A([0-9a-f]{1,4}:){1,2}(:[0-9a-f]{1,4}){1,3}:'
                        r'(25[0-5]|2[0-4]\d|[0-1]?\d?\d)(\.(25[0-5]|2[0-4]\d|[0-1]?'
                        r'\d?\d)){3}\Z)|(\A([0-9a-f]{1,4}:){1,3}(:[0-9a-f]{1,4})'
                        r'{1,2}:(25[0-5]|2[0-4]\d|[0-1]?\d?\d)(\.(25[0-5]|2[0-4]\d|'
                        r'[0-1]?\d?\d)){3}\Z)|(\A([0-9a-f]{1,4}:){1,4}(:[0-9a-f]'
                        r'{1,4}){1,1}:(25[0-5]|2[0-4]\d|[0-1]?\d?\d)(\.(25[0-5]|'
                        r'2[0-4]\d|[0-1]?\d?\d)){3}\Z)|(\A(([0-9a-f]{1,4}:){1,5}|:):'
                        r'(25[0-5]|2[0-4]\d|[0-1]?\d?\d)(\.(25[0-5]|2[0-4]\d|[0-1]?'
                        r'\d?\d)){3}\Z)|(\A:(:[0-9a-f]{1,4}){1,5}:(25[0-5]|2[0-4]\d|'
                        r'[0-1]?\d?\d)(\.(25[0-5]|2[0-4]\d|[0-1]?\d?\d)){3}\Z)')

local_ip_list = (
    ('127.0.0.0', 0xff000000),
    ('10.0.0.0', 0xff000000),
    ('192.168.0.0', 0xffff0000),
    ('172.16.0.0', 0xffff0000),
    ('172.17.0.0', 0xffff0000),
    ('172.18.0.0', 0xffff0000),
    ('172.19.0.0', 0xffff0000),
    ('172.20.0.0', 0xffff0000),
    ('172.21.0.0', 0xffff0000),
    ('172.22.0.0', 0xffff0000),
    ('172.23.0.0', 0xffff0000),
    ('172.24.0.0', 0xffff0000),
    ('172.25.0.0', 0xffff0000),
    ('172.26.0.0', 0xffff0000),
    ('172.27.0.0', 0xffff0000),
    ('172.28.0.0', 0xffff0000),
    ('172.29.0.0', 0xffff0000),
    ('172.30.0.0', 0xffff0000),
    ('172.31.0.0', 0xffff0000)
)

_cn_domain_list = {
    'localhost',
    '.cn',
    '.baidu.com',
    '.jd.com',
    '.360buyimg.com',
    '.taobao.com',
    '.alicdn.com',
    '.tanx.com',
    '.bdimg.com',
    '.qq.com'
}

_foreign_domain_list = {
    'google.com',
    'facebook.com',
    'twitter.com',
    'tumblr.com'
}

_hosts = {}

cn_domain_list = set()
foreign_domain_list = set()

cn_domain_file = lookup_conf_file('cn_domain.conf')
cn_domain_update = 0
cn_domain_file_mod = 0

hosts_update_time = 0
hosts_file_mod = 0
hosts_file = '/etc/hosts'
if 'WINDIR' in os.environ:
    hosts_file = os.environ['WINDIR'] + '/system32/drivers/etc/hosts'

# rlock = threading.RLock()

dns_executor = MyThreadPoolExecutor(max_workers=os.cpu_count()+1, pool_name='DnsWorker', order_by_func=True)


def update_hosts():
    global hosts_update_time
    global hosts_file_mod
    global hosts_file

    if (time.time() - hosts_update_time) < 1:
        return
    hosts_update_time = time.time()

    try:
        mtime = os.stat(hosts_file).st_mtime
        if hosts_file_mod < mtime:
            # with rlock:
            if hosts_file_mod >= mtime:
                return
            _hosts.clear()
            with open(hosts_file, 'r') as f:
                for line in f.readlines():
                    line = line.strip()
                    parts = line.split()
                    if line.startswith("#"):
                        continue
                    if len(parts) >= 2:
                        ip = parts[0]
                        if is_ipv4(ip):
                            for i in range(1, len(parts)):
                                hostname = parts[i]
                                if hostname and hostname.startswith('#'):
                                    break
                                if hostname:
                                    _hosts[hostname] = ip
            hosts_file_mod = mtime
            logger.info('%s reloaded', hosts_file)
    except BaseException:
        hosts_update_time = time.time() + 10 * 60
        _hosts['localhost'] = '127.0.0.1'


def update_cn_domain():
    global cn_domain_update
    global cn_domain_file_mod
    global cn_domain_file
    global _cn_domain_list
    global cn_domain_list

    if (time.time() - cn_domain_update) < 1:
        return
    cn_domain_update = time.time()
    try:
        mtime = os.stat(cn_domain_file).st_mtime
        if cn_domain_file_mod < mtime:
            # with rlock:
            if cn_domain_file_mod >= mtime:
                return
            cn_domain_list.clear()
            cn_domain_list.update(_cn_domain_list)
            foreign_domain_list.clear()
            foreign_domain_list.update(_foreign_domain_list)
            with open(cn_domain_file, 'r') as f:
                while True:
                    d = f.readline()
                    if not d:
                        break
                    d = d.strip()
                    if d and not d.startswith("#"):
                        if d.startswith("-"):
                            foreign_domain_list.add(d[1:])
                        else:
                            cn_domain_list.add(d)
            cn_domain_file_mod = mtime
            logger.info('%s reloaded', cn_domain_file)
    except FileNotFoundError:
        cn_domain_update = time.time() + 60
        logger.debug('china domain file: %s not found', cn_domain_file)


def is_foreign_domain(addr):
    update_cn_domain()
    for domain in foreign_domain_list:
        if addr.endswith(domain):
            logger.log(5, '%s match foreign_domain %s', addr, domain)
            return True
    return False


def is_cn_domain(addr):
    update_cn_domain()
    for domain in cn_domain_list:
        if addr.endswith(domain):
            logger.log(5, '%s match cn_domain %s', addr, domain)
            return True
    return False


def is_ipv4(addr):
    return ip_regex.match(addr)


def is_ipv6(addr):
    return ipv6_regex.match(addr)


def del_cache(addr):
    if addr in dns_cache:
        del dns_cache[addr]
    if addr in cn_addr_cache:
        del cn_addr_cache[addr]


def async_dns_query(qname, raise_on_fail=False, local_dns=False, loop=None):
    import asyncio

    if loop is None:
        loop = asyncio.get_event_loop()
    update_hosts()
    ip = dns_query(qname, in_cache=True)
    if not ip:
        ip = yield from loop.run_in_executor(dns_executor, dns_query, qname, raise_on_fail, local_dns)

    return ip


def dns_query(qname, raise_on_fail=False, local_dns=False, in_cache=False):
    global dns_cache
    global resolver
    global local_dns_query
    if is_ipv4(qname):
        return qname
    if is_ipv6(qname):
        return qname
    if qname in dns_cache:
        return dns_cache[qname]
    # update_hosts()
    if qname in _hosts:
        return _hosts[qname]
    if in_cache:
        return None
    ex = None
    query_start = time.time()
    logger.log(logging.DEBUG, 'dns lookup %s ...', qname)
    if not local_dns_query and not local_dns:
        try:
            answers = resolver.query(qname)
            for a in answers:
                ipv4 = a.to_text()
                used = time.time() - query_start
                logger.log(logging.INFO if used < 1 else logging.WARN, 'opendns lookup %s => %s used %.2f sec', qname, ipv4, used)
                dns_cache[qname] = ipv4
                return ipv4
        except (NoAnswer, NXDOMAIN, Timeout) as noa:
            ex = noa
        logger.log(logging.WARN, 'opendns lookup %s failed, try local lookup (used %.2f sec)', qname, (time.time() - query_start))
    try:
        ipv4 = socket.gethostbyname(qname)
        dns_cache[qname] = ipv4
        if ex is not None:
            logger.warning('local lookup result: %s => %s for (%s:%s)', qname, ipv4, ex.__class__.__name__, ex)
        else:
            used = time.time() - query_start
            logger.log(logging.INFO if used < 1 else logging.WARN, 'local lookup %s => %s used %.2f sec', qname, ipv4,
                       used)
        return ipv4
    except BaseException as ex:
        logger.warning('%s => DNS lookup FAIL(%s:%s), raise_on_fail=%s', qname, ex.__class__.__name__, ex, raise_on_fail)
        if raise_on_fail:
            raise ex
    return None


def is_subnet(ipv4, netlist):
    '''
    :param ipv4:
    :param netlist: (('10.0.0.0', 0xffffff00), (...), ...)
    :return:
    '''
    ipn = int.from_bytes(socket.inet_pton(socket.AF_INET, ipv4), byteorder='big')
    for ip, mask in netlist:
        if ip == (ipn & mask):
            return True
    return False


def is_cn_ip(atype, addr, return_country=False):
    global cn_ip_list
    global cn_addr_cache
    load_cn_list()
    if addr in cn_addr_cache:
        cn = cn_addr_cache[addr]
        logger.log(5, '%s => %s', addr, cn)
        return cn.upper() == 'CN' if not return_country else cn
    ip = dns_query(addr) if atype == 0x03 else addr
    if atype == 0x04:
        if addr.startswith('::ffff:'):
            ip = addr[7:]
            atype = 0x01
    if ip is None:
        return False if not return_country else 'FOREIGN'
    if ip == '202.106.1.2' or ip == '211.94.66.147' or ip == '180.168.41.175':
        logger.warn('GFW IP: %s[%s]', ip, addr)
        cn_addr_cache[addr] = 'FOREIGN'
        return False if not return_country else 'FOREIGN'
    ipn = int.from_bytes(socket.inet_pton(socket.AF_INET6 if atype == 0x04 else socket.AF_INET, ip), byteorder='big')
    for ip, mask, country in cn_ip_list:
        if ip == (ipn & mask):
            if country.upper() == 'CN':
                logger.log(logging.DEBUG, '%s[%s] => %s', addr, ip, country)
                cn_addr_cache[addr] = country
                return True if not return_country else country
            else:
                logger.log(logging.DEBUG, '%s[%s] => %s', addr, ip, country)
                cn_addr_cache[addr] = country
                return False if not return_country else country
    logger.log(logging.DEBUG, '%s[%s] => FOREIGN', addr, ip)
    cn_addr_cache[addr] = 'FOREIGN'
    return False if not return_country else 'FOREIGN'


def load_cn_list(only_cn=True):
    """http://ftp.apnic.net/apnic/stats/apnic/delegated-apnic-latest"""

    global cn_ip_last_mod
    global cn_ip_list

    if cn_ip_last_mod > 0:
        return

    try:
        cn_ip_list.clear()

        for local in local_ip_list:
            starting_ip = int.from_bytes(socket.inet_aton(local[0]), byteorder='big')
            imask = local[1]
            cn_ip_list.append((starting_ip, imask, 'CN'))

        cn_ip_last_mod = os.stat('apnic-latest').st_mtime
        with open('apnic-latest', 'r') as f:
            data = f.read()

        if only_cn:
            regex = re.compile(r'apnic\|cn\|ipv[46]\|[0-9a-f\.:]+\|[0-9]+\|[0-9]+\|a.*', re.IGNORECASE)
        else:
            regex = re.compile(r'apnic\|..\|ipv[46]\|[0-9a-f\.:]+\|[0-9]+\|[0-9]+\|a.*', re.IGNORECASE)
        cndata = regex.findall(data)

        for item in cndata:
            unit_items = item.split('|')
            country = unit_items[1]
            num_ip = int(unit_items[4])
            if unit_items[2] == 'ipv6':
                starting_ip = int.from_bytes(socket.inet_pton(socket.AF_INET6, unit_items[3]), byteorder='big')
                imask = 0xffffffffffffffffffffffffffffffff ^ (num_ip - 1)
            else:
                starting_ip = int.from_bytes(socket.inet_aton(unit_items[3]), byteorder='big')
                imask = 0xffffffff ^ (num_ip-1)
            cn_ip_list.append((starting_ip, imask, country))

        logger.info('apnic-latest loaded')
    except FileNotFoundError:
        cn_ip_last_mod = time.time() + 60
        logger.debug('file not found: apnic-latest')
    except BaseException as ex_apnic:
        logging.exception('load_cn_list(only_cn=%s) fail: %s', only_cn, ex_apnic)


if __name__ == '__main__':
    import signal
    import sys
    from tsproxy import print_stack_trace

    def term_handler(signum, _):
        if signum == signal.SIGQUIT:
            print_stack_trace()
            return
        logger.info('received %s, do graceful shutdowning ...',
                    'SIGTERM' if signum == signal.SIGTERM else
                    'SIGINT' if signum == signal.SIGINT else '%d' % signum)
        try:
            print_stack_trace()
        except FileNotFoundError:
            pass
        except BaseException as ex1:
            logger.warn('term_handler error: %s(%s)', ex1.__class__.__name__, ex1)
        finally:
            sys.exit(0)

    signal.signal(signal.SIGTERM, print_stack_trace)
    signal.signal(signal.SIGINT, print_stack_trace)
    signal.signal(signal.SIGQUIT, print_stack_trace)

    ipv4 = dns_query('jp01.sss.tf')
