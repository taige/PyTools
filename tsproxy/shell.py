#!/usr/bin/env python3

import argparse
import asyncio
import json
import logging
import logging.config
import os
import signal
from concurrent.futures import CancelledError

import uvloop

from tsproxy.common import print_stack_trace, lookup_conf_file
from tsproxy.connector import RouterableConnector, CheckConnector
from tsproxy.listener import ManageableHttpListener, HttpListener
from tsproxy.proxyholder import ProxyHolder
from tsproxy import topendns

logger = logging.getLogger(__name__)

logger_conf_mod = 0


def args_parse(args=None):
    parser = argparse.ArgumentParser(description="Smart Socks5 and Http Proxy with multi background shadowsocks/socks5 proxy",
                                     formatter_class=argparse.RawTextHelpFormatter)
    group = parser.add_argument_group("query ip/dname's country")
    group.add_argument('--ip', nargs='+',
                       help="query ip(s)'s country")
    group.add_argument('--qname', nargs='+', metavar='DOMAIN-NAME',
                       help="query domain-name(s)'s country. use 'opendns.com' to DNS query.")
    group.add_argument('--qnamel', nargs='+', metavar='DOMAIN-NAME',
                       help="query domain-name(s)'s country. use 'local system' to DNS query.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--proxy-all', '-a', action='store_const', dest='smart_mode', const=2,
                       help="proxy all request.")
    group.add_argument('--smart', '-s', action='store_const', dest='smart_mode', const=1, default=1,
                       help="run as a smart proxy, "
                            "ie. proxy foreign ip/domain-name and CN's connect directly. \n"
                            "this is default mode.")
    group.add_argument('--no-proxy', '-n', action='store_const', dest='smart_mode', const=0,
                       help="no proxy, all request connect direct \n"
                            "if run in this mode, socks5_hostname is required \n"
                            " and format is 'passord/method@hostname:port'.")
    parser.add_argument('--http-port', '-p', dest='http_port', type=int, default=8080,
                        help="http(s) proxy listening port.\ndefault is 8080.")
    parser.add_argument('--http-address', dest='http_address', default='0.0.0.0',
                        help="http(s) proxy listening address.\ndefault is '0.0.0.0'.")
    parser.add_argument('--socks5-port', metavar='SOCKS5_PORT', dest='socks5_port', type=int, default=7070,
                        help="socks5 proxy listening port.\ndefault is 7070")
    parser.add_argument('--pid-file', dest='pid_file', default='.ss-proxy.pid',
                        help="file to store the process id.\ndefault filename is '.ss-proxy.pid'.")
    parser.add_argument('--proxy-file', '-f', dest='proxy_file', default='proxies.json',
                        help="dump proxies information to this file on exit,\n"
                             "or load proxies from this file on no proxy hostnames input.\n"
                             "default filename is 'proxies.json'.")
    parser.add_argument('--router', dest='router_conf', default='router.yaml',
                        help="router config file using YAML format, default is 'router.yaml'.")
    parser.add_argument('--logger_conf', dest='logger_conf',
                        help="logger config file, default is 'ss-proxy-logging.conf'.")
    parser.add_argument('hostnames', metavar='proxy', nargs='*',
                        help='use "hostname:port" to define a socks5 proxy, \n'
                             '"password/method@hostname:server_port" or "hostname" as a shadowsocks proxy.\n'
                             'hostname is a valid shadowsocks proxy host, \n'
                             'and server_port/password/method in the config file "{hostname}.json".')

    kwargs = vars(parser.parse_args(None if args is None else args.split()))
    hostnames = kwargs['hostnames']
    del kwargs['hostnames']
    if kwargs['smart_mode'] is None:
        kwargs['smart_mode'] = 1
    return kwargs, hostnames


async def update_log_conf(logger_conf_file, update_interval=10, loop=None):
    global logger_conf_mod
    if logger_conf_mod > 0:
        while True:
            await asyncio.sleep(update_interval, loop=loop)
            try:
                mtime = os.stat(logger_conf_file).st_mtime
                if mtime > logger_conf_mod:
                    logging.config.fileConfig(logger_conf_file, disable_existing_loggers=False)
                    logger_conf_mod = mtime
                    logger.info('%s reloaded', logger_conf_file)
            except BaseException as ex_log1:
                logging.exception('update_log_conf(%s) fail: %s', logger_conf_file, ex_log1)


async def update_apnic(inital_wait, loop=None):
    wait_to_next = inital_wait
    while True:
        await asyncio.sleep(wait_to_next)
        try:
            wait_to_next = await topendns.update_apnic_latest(loop=loop)
        except CancelledError:
            break
        except BaseException as ex:
            logging.exception('update_apnic fail: %s', ex)


is_shutdown = False


def startup(*proxies, http_port=8080, http_address='127.0.0.1', proxy_file='proxies.json', pid_file='.ss-proxy.pid', smart_mode=1, **kwargs):
    global logger_conf_mod
    _startup = False

    logger_conf_file = 'ss-proxy-logging.conf'
    if 'logger_conf' in kwargs and kwargs['logger_conf']:
        logger_conf_file = kwargs.pop('logger_conf')
    logger_conf_file = lookup_conf_file(logger_conf_file)
    asyncio.set_event_loop(uvloop.new_event_loop())
    try:
        logger_conf_mod = os.stat(logger_conf_file).st_mtime
        logging.config.fileConfig(logger_conf_file, disable_existing_loggers=False)
    except BaseException as ex_log:
        logging.basicConfig(format='%(asctime)s %(levelname)-5s [%(threadName)-14s] %(name)-16s - %(message)s', level=logging.DEBUG)
        logging.exception('fileConfig(%s) fail: %s', logger_conf_file, ex_log)

    try:
        with open(proxy_file, 'r') as f:
            j_in = json.load(f)
    except FileNotFoundError:
        logger.warning('proxies not config, and proxy config file %s not found' % proxy_file)
        j_in = {}

    loop = asyncio.get_event_loop()
    proxy_holder = ProxyHolder(http_port+1, proxy_file=proxy_file)
    if not proxies:
        proxy_holder.load_proxies(j_in)
    else:
        proxy_holder.add_proxies(proxies)

    if proxy_holder.psize <= 0:
        raise Exception('no proxy config found')
    for i in range(0, proxy_holder.psize):
        proxy_info = proxy_holder.proxy_list[i]
        proxy_info.print_info(i)

    apnic_update_task = loop.create_task(topendns.update_apnic_latest(raise_on_fail=True, loop=loop))

    def term_handler(sig_num):
        global is_shutdown
        if sig_num == signal.SIGQUIT:
            print_stack_trace()
            return
        logger.debug('received %s, do graceful closing ...',
                     'SIGTERM' if sig_num == signal.SIGTERM else
                     'SIGINT' if sig_num == signal.SIGINT else '%d' % sig_num)
        try:
            for t in asyncio.Task.all_tasks():
                t.cancel()
        except BaseException as ex1:
            logger.exception('term_handler error: %s(%s)', ex1.__class__.__name__, ex1)
        finally:
            is_shutdown = True
            if _startup:
                loop.stop()

    for signame in ('SIGINT', 'SIGTERM', 'SIGQUIT'):
        signum = getattr(signal, signame)
        loop.add_signal_handler(signum, term_handler, signum)

    next_update_apnic = loop.run_until_complete(apnic_update_task)

    def dump_config():
        j_dump = {}
        http_proxy.dump_acl(j_dump)
        proxy_holder.dump_proxys(j_dump)
        with open(proxy_file + '.ing', 'w') as pf:
            json.dump(j_dump, pf, indent=2, sort_keys=True)
        os.rename(proxy_file + '.ing', proxy_file)

    http_proxy = ManageableHttpListener(listen_addr=(http_address, http_port),
                                        connector=RouterableConnector(proxy_holder, smart_mode, loop, **kwargs),
                                        proxy_holder=proxy_holder,
                                        dump_config=dump_config,
                                        loop=loop)
    check_proxy = HttpListener(listen_addr=('127.0.0.1', http_port+1),
                               connector=CheckConnector(proxy_holder, loop))
    # https_proxy = HttpsListener(listen_addr=('127.0.0.1', http_port - 1),
    #                             connector=SmartConnector(proxy_holder, smart_mode, loop))

    http_proxy.load_acl(j_in)
    server = loop.run_until_complete(http_proxy.start())
    # https_server = loop.run_until_complete(https_proxy.start())
    check_server = loop.run_until_complete(check_proxy.start())

    with open(pid_file, 'w') as f:
        f.write('%d' % os.getpid())

    try:
        if not is_shutdown:
            loop.create_task(update_log_conf(logger_conf_file, loop=loop))
            loop.create_task(update_apnic(next_update_apnic, loop=loop))
            loop.create_task(proxy_holder.monitor_loop(loop=loop))

            logger.info('TSProxy Startup')
            _startup = True
            loop.run_forever()
        server.close()
        # https_server.close()
        check_server.close()
        loop.run_until_complete(server.wait_closed())
        # loop.run_until_complete(https_server.wait_closed())
        loop.run_until_complete(check_server.wait_closed())
        loop.close()
        dump_config()
    finally:
        os.remove(pid_file)
        logger.info('TSProxy Closed')


def main():
    kwargs, hostnames = args_parse()
    pid = os.fork()
    if pid == 0:
        os.setsid()
        startup(*hostnames, **kwargs)


if __name__ == '__main__':
    main()
