#!/usr/bin/env python3

import argparse
import asyncio
import logging
import os
import signal
import sys
import platform
from concurrent.futures import CancelledError

import uvloop
from aiohttp import formdata

from tsproxy.common import MyThreadPoolExecutor
from tsproxy.common import fmt_human_bytes as _fmt_human_bytes
from tsproxy.common import fmt_human_time as _fmt_human_time

from tsproxy.version import version

__version__ = version

logger = logging.getLogger(__name__)

md5_executor = None


def fmt_human_bytes(num, status=None, key=None):
    s = _fmt_human_bytes(num)
    if status is None:
        return s
    if key not in status or status[key] < len(s):
        status[key] = len(s)
    return ('%'+str(status[key])+'s') % s


def fmt_human_time(t, status=None, key=None):
    s = _fmt_human_time(t)
    if status is None:
        return s
    if key not in status or status[key] < len(s):
        status[key] = len(s)
    return ('%'+str(status[key])+'s') % s


def MD5(filename, block_size=10240):
    """Returns MD% checksum for given file.
    """
    import hashlib

    md5 = hashlib.md5()
    with open(filename, 'rb') as file:
        while True:
            data = file.read(block_size)
            if not data:
                break
            md5.update(data)
    return md5.hexdigest()


async def async_MD5(*args, loop=None):
    global md5_executor
    if loop is None:
        loop = asyncio.get_event_loop()
    if md5_executor is None:
        md5_executor = MyThreadPoolExecutor(max_workers=os.cpu_count(), pool_name='md5-helper')
    _md5 = await loop.run_in_executor(md5_executor, MD5, *args)
    return _md5


def main_entry(*headers, urls: list, baidu=False, user_agent=None, http_proxy=None, n=0, conc=False, loop=None, **kwargs):
    from pyclda.aio_downloader import AioDownloader
    from pyclda.baidu_downloader import BaiduDownloader
    dict_headers = {}
    if user_agent is not None:
        dict_headers['User-Agent'] = user_agent.strip()
    for h in headers:
        if ':' in h:
            k, w = h.split(':', 1)
            dict_headers[k.strip()] = w.strip()
    if 'Host' in dict_headers:
        del dict_headers['Host']
    if http_proxy:
        http_proxy = http_proxy.strip()

    async def _d(_url, post_data=None, **kw_args):
        if baidu:
            downloader = BaiduDownloader
        else:
            downloader = AioDownloader
        data = None
        if post_data is not None:
            data = formdata.FormData()
            for _data in post_data.split('&'):
                _kv = _data.split('=', 1)
                if len(_kv) == 2:
                    data.add_field(_kv[0], _kv[1])
        ad = downloader(
            _url,
            n=min(50, n),
            loop=loop,
            headers=dict_headers,
            data=data,
            proxy=http_proxy,
            skip_auto_headers=('Accept-Encoding', 'Accept'),
            **kw_args
        )
        done, _out_file, cancelled_err = await ad.download()
        os.system('tput -Txterm bel')
        return done, _out_file, cancelled_err

    def term_handler(_signum):
        if _signum == signal.SIGQUIT:
            return
        logger.log(6, 'received %s, do graceful closing ...',
                   'SIGTERM' if _signum == signal.SIGTERM else
                   'SIGINT' if _signum == signal.SIGINT else '%d' % _signum)
        try:
            for t in asyncio.Task.all_tasks():
                t.cancel()
        except BaseException as ex1:
            logger.exception('term_handler error: %s(%s)', ex1.__class__.__name__, ex1)

    for sig_name in ('SIGINT', 'SIGTERM', 'SIGQUIT'):
        signum = getattr(signal, sig_name)
        loop.add_signal_handler(signum, term_handler, signum)

    try:
        kwargs_bk = kwargs.copy()
        futures = []
        for url in urls:
            kwargs = kwargs_bk.copy()
            if not url.lower().startswith('http://') and not url.lower().startswith('https://'):
                kwargs.pop('out_file', None)
                kwargs.pop('restart_on_done', None)
            else:
                kwargs.pop('path_concur', None)
                kwargs.pop('print_status', None)
                kwargs.pop('magic_param', None)
            if conc:
                futures.append(_d(url, **kwargs))
            else:
                _, _, cancelled = loop.run_until_complete(_d(url, **kwargs))
                if cancelled is not None:
                    break
        if len(futures) > 0:
            loop.run_until_complete(asyncio.gather(*futures, loop=loop))
        if platform.system() == 'Darwin':
            os.system('osascript -e \'display notification "PyCLDA DONE" with title "PyCLDA"\'')
    except CancelledError:
        pass
    except BaseException as ex:
        logger.exception('error: %s(%s)', ex.__class__.__name__, ex)
    # finally:
    #     loop.close()


def get_mac_ver():
    import platform
    mac_ver = platform.mac_ver()[0]
    if mac_ver.count('.') < 2:
        mac_ver += '.0'
    return mac_ver


def args_parse(*args):
    parser = argparse.ArgumentParser(description="download accelerate",
                                     formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-n', dest='n', type=int, default=0,
                        help="specify maximum number of connections, default is 6")
    parser.add_argument('-o', dest='out_file',
                        help="specify local output file")
    parser.add_argument('-H', dest='headers', action='append', default=[],
                        help="add header string")
    parser.add_argument('-U', dest='user_agent',
                        help="set user agent")
    parser.add_argument('-x', dest='http_proxy',
                        help="set http proxy")
    parser.add_argument('-a', dest='use_curses', default=False, action='store_true',
                        help="use curses progress indicator")
    parser.add_argument('-r', dest='restart_on_done', default=True, action='store_false',
                        help="don't re-allocate download resource when one thread done")
    parser.add_argument('-m', dest='method', default='GET',
                        help="set http method, default is GET")
    parser.add_argument('-c', dest='conc', default=False, action='store_true',
                        help="concurrency download urls, default is false")
    parser.add_argument('--post_data', dest='post_data',
                        help="Use POST as the method for all HTTP requests and send the specified data in the request body")
    parser.add_argument('--conn_timeout', dest='conn_timeout', default=5, type=int,
                        help="set connect timeout, default is 5 seconds")
    parser.add_argument('--timeout', dest='read_timeout', default=58, type=int,
                        help="set read timeout, default is 58 seconds")
    parser.add_argument('--retry_count', dest='retry_count', default=10, type=int,
                        help="http request retry count when io errors occur(including the first request try), default is 10")
    parser.add_argument('--path_concur', dest='path_concur', default=1, type=int,
                        help="concurrency download files when path download, default is 1")
    parser.add_argument('--print_status', dest='print_status', default=10, type=int,
                        help="path downloader status printing interval, default is 10 (seconds)")
    parser.add_argument('--magic_param', dest='magic_param', default='method=download',
                        help="magic param add to url for accelerate concurrent connection and download speed")
    parser.add_argument('--baidu', default=False, action='store_true',
                        help="auto set baiduYun request headers")
    parser.add_argument('--verbose', '-v', dest='verbose', action='count', default=0)
    parser.add_argument('urls', nargs='+')

    kwargs = vars(parser.parse_args(None if args is None or len(args) == 0 else args))
    headers = kwargs.pop('headers')
    if kwargs['post_data']:
        kwargs['method'] = 'POST'
    return kwargs, headers, kwargs.pop('verbose'), kwargs.pop('use_curses')


def is_all_http(urls):
    for url in urls:
        if not url.startswith('http'):
            return False
    return True


def main():
    print('pyclda version: %s' % __version__)
    _kwargs, _headers, _verbose, _use_curses = args_parse()
    _loop = uvloop.new_event_loop()
    asyncio.set_event_loop(_loop)
    _kwargs['loop'] = _loop

    if _use_curses and not _kwargs['conc'] and is_all_http(_kwargs['urls']):
        logs = []
        try:
            from pyclda.curses_downloader import curses_entry
            if curses_entry(logs, _verbose, main_entry, *_headers, **_kwargs):
                return
        finally:
            for l in logs:
                print(l[1])

    logging.basicConfig(format='%(asctime)s %(name)-23s - %(message)s',
                        level=(9 - _verbose) if _verbose > 2 else logging.DEBUG if _verbose > 1 else logging.INFO if _verbose > 0 else logging.WARNING,
                        stream=sys.stdout)
    main_entry(*_headers, **_kwargs)


if __name__ == '__main__':
    main()
