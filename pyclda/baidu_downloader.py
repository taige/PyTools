import asyncio
import logging
import json
import os
import time
from concurrent.futures import CancelledError
from pprint import pformat
from urllib.parse import quote_plus as quote

import aiohttp
from aiohttp import hdrs

from pyclda import get_mac_ver, async_MD5
from pyclda.aio_downloader import AioDownloader, Status
from tsproxy.common import Timeout, fmt_human_time as _fmt_human_time, fmt_human_bytes as _fmt_human_bytes

__all__ = ['BaiduDownloader']

logger = logging.getLogger(__name__)


class BaiduDownloader:
    """
        aio方式从Baidu Yun下载云端的一个目录或者文件
    """

    def __init__(self, path, path_concur=1, print_status=10, magic_param='method=download', conf_filename='pyclda.baidu.url.params.json', loop=None, **kwargs):
        self.path = path
        self.path_concur = path_concur
        self.print_status = print_status
        self.magic_param = magic_param
        self.loop = loop if loop is not None else asyncio.get_event_loop()
        self.retry_count = kwargs.get('retry_count', 10)
        self.baidu_request = BaiduRequest(os.path.expanduser('~/%s' % conf_filename) if os.sep not in conf_filename else conf_filename, self.loop, **kwargs)

        self._downloaders = []

    async def download(self):
        if self.path.lower().startswith('http://') or self.path.lower().startswith('https://'):
            return await self.baidu_request.aio_download(self.path)
        else:
            return await self._download_path()

    async def _download_path(self):
        file_list, failed_list_dirs = await self.baidu_request.path_list(self.path)
        file_count = len(file_list)

        base_dir = os.path.dirname(self.path if not self.path.endswith('/') else self.path[:len(self.path) - 1])
        if not base_dir.startswith('/'):
            base_dir = '/' + base_dir

        for index in range(min(self.path_concur, file_count)):
            self._downloaders.append(PathDownloader(self.baidu_request, base_dir, self.magic_param, self.retry_count, index, self.loop))

        cancelled = None
        d_futures = []
        failed_location_files = []
        time_out = time.time() + self.print_status
        while True:
            try:
                for downloader in self._downloaders:
                    if not cancelled and len(file_list) > 0 and downloader.status in ('INITIAL', 'DONE'):
                        d_futures.append(asyncio.ensure_future(downloader.download(file_list.pop(0))))

                if len(d_futures) == 0:
                    break

                results, pending = await asyncio.wait(d_futures, timeout=max(0, time_out - time.time()), return_when=asyncio.FIRST_COMPLETED, loop=self.loop)
                if len(results) > 0:
                    for result in results:
                        _cncl = result.result()
                        if _cncl is not None:
                            if isinstance(_cncl, str):
                                failed_location_files.append(_cncl)
                            else:
                                cancelled = _cncl
                                break
                    d_futures.clear()
                    d_futures.extend(pending)
                else:
                    time_out = time.time() + self.print_status
                    raise asyncio.TimeoutError()
            except asyncio.TimeoutError:
                logger.warning('=====DOWNLOADER STATUS=====')
                self._print_downloader_status()
                if len(file_list) > 0:
                    _tmp = ''
                    for i in range(min(10, len(file_list))):
                        _tmp += '\n\t%s(%s)' % (file_list[i]['path'], _fmt_human_bytes(file_list[i]['size']))
                    logger.info('files waiting process(Total: %d): %s', len(file_list), _tmp)
            except CancelledError as ce:
                cancelled = ce
                continue

        logger.debug('=====downloader status=====')
        self._print_downloader_status(log_level=logging.DEBUG)

        if len(failed_list_dirs) > 0:
            logger.info('FAILED list dirs:\n\t%s', failed_list_dirs)
        if len(failed_location_files) > 0:
            logger.info('FAILED location files:\n\t%s', failed_location_files)

        return None, None, cancelled

    def _print_downloader_status(self, log_level=logging.WARNING):
        for idx in range(len(self._downloaders)):
            downloader = self._downloaders[idx]
            status = downloader.status
            if isinstance(status, Status):
                filename = status['filename']
                if filename is None:
                    filename = status['_filename']
                logger.log(log_level, 'downloader.#%d downloading [ETA %s %sB/S %d%%] %s', idx, _fmt_human_time(status.eta()), _fmt_human_bytes(status.down_speed()), status.done_percent(), filename)
            else:
                logger.log(log_level, 'downloader.#%d %s', idx, status)


class PathDownloader:

    def __init__(self, baidu_request, base_dir, magic_param, retry_count, index, loop):
        self.baidu_request = baidu_request
        self.base_dir = base_dir
        self.magic_param = magic_param
        self.retry_count = retry_count
        self.index = index
        self.loop = loop
        self.status = 'INITIAL'

    async def download(self, file_path):
        try:
            _cancelled = await self._download(file_path)
        except CancelledError as _ce:
            _cancelled = _ce
        self.status = 'DONE' if (_cancelled is None or isinstance(_cancelled, str)) else 'CANCELLED'
        return _cancelled

    async def _download(self, file_info):
        self.status = 'READY for download task...'

        server_file_path = file_info['path']
        self.status = 'processing %s' % server_file_path
        self._logger(logging.DEBUG, "processing %s", server_file_path)
        server_size = file_info['size']
        server_md5 = file_info['md5']
        local_file_path = os.path.relpath(server_file_path, self.base_dir)

        same_size_but_diff_md5 = False
        if os.path.isfile(local_file_path):
            local_size = os.stat(local_file_path).st_size
            if local_size == server_size:
                self.status = 'calculating md5 of %s' % local_file_path
                local_md5 = await async_MD5(local_file_path, loop=self.loop)
                if local_md5.lower() == server_md5.lower():
                    self._logger(logging.INFO, '%s (%s/%d) has DOWNLOADED to %s' % (server_file_path, server_md5, server_size, local_file_path))
                    return None
                else:
                    same_size_but_diff_md5 = True
                    self._logger(logging.INFO, '%s & %s have SAME SIZE but DIFF MD5(%d/%s:%s)' % (server_file_path, local_file_path, server_size, server_md5, local_md5))
            else:
                self._logger(logging.INFO, '%s(%d) maybe partial downloaded at %s(%d)' % (server_file_path, server_size, local_file_path, local_size))

        self.status = 'requesting location of %s' % server_file_path
        urls = await self.baidu_request.path_file_location(server_file_path, _logger=self._logger)
        if urls is None or len(urls) == 0 or 'url' not in urls[0] or not urls[0]['url']:
            # self.failed_location_files.append(server_file_path)
            self._logger(logging.WARNING, '%s location request FAILED: %s', server_file_path, pformat(urls))
            return server_file_path

        self._logger(logging.DEBUG, '%s locations: %s', server_file_path, pformat(urls))
        _cancelled = None
        for url_idx, j_url in enumerate(urls):
            if 'url' not in j_url:
                continue
            url = j_url['url']
            if self.magic_param not in url:
                url += '&%s' % self.magic_param.lower()
            retry_count = max(0, self.retry_count - url_idx)
            if same_size_but_diff_md5:
                self.status = 're-requesting server-side md5 of %s' % server_file_path
                headers = await self.baidu_request.aio_request_with_retry(url, header=True, _logger=self._logger)
                server_md5 = headers.get(hdrs.CONTENT_MD5) if headers is not None else None
                if server_md5 and local_md5.lower() == server_md5.lower():
                    self._logger(logging.WARNING, '%s & %s have SAME SIZE AND MD5(%d/%s)' % (server_file_path, local_file_path, server_size, server_md5))
                    return None
                else:
                    self._logger(logging.WARNING, '%s & %s have SAME SIZE but DIFF MD5(%d/%s:%s) RE-Download' % (server_file_path, local_file_path, server_size, server_md5, local_md5))

            self._logger(logging.DEBUG, 'Downloading %s ...', server_file_path)
            self.status = Status(_filename=file_info['server_filename'])
            _done, _out_file, _cancelled = await self.baidu_request.aio_download(url, out_file=local_file_path, status=self.status, retry_count=retry_count)
            if not _done and _cancelled is None:
                self._logger(logging.WARNING, '%s download FAILED, maybe let\'s try next url', server_file_path)
                continue
            if _done:
                self._logger(logging.WARNING, '%s download SUCCESS, saved to %s', server_file_path, _out_file)
            else:
                self._logger(logging.WARNING, '%s download CANCELLED, check the log for detail', server_file_path)
            break

        return _cancelled

    def _logger(self, level, msg, *log_args, exc_info=False, **log_kwargs):
        if not logger.isEnabledFor(level):
            return
        logger.log(level, '_downloader.#%d ' % self.index + msg, *log_args, exc_info=exc_info, **log_kwargs)


class BaiduRequest:

    def __init__(self, conf_filename, loop, **kwargs):
        try:
            with open(conf_filename, 'r') as f:
                self.conf = json.load(f)
        except Exception as e:
            logger.warning('load %s fail: %s', conf_filename, e)
            self.conf = {}
        self.loop = loop
        if 'headers' in kwargs:
            dict_headers = kwargs['headers']
            dict_headers.setdefault('X-Download-From', 'baiduyun')
            dict_headers.setdefault('User-Agent', 'netdisk;2.1.0;pc;pc-mac;%s;macbaiduyunguanjia' % get_mac_ver())
            _, _headers = self._get_baidu_url_params('headers')
            for k in _headers:
                dict_headers.setdefault(k, _headers[k])
        self.kwargs = kwargs

        self._path_list_url = 'https://pan.baidu.com/api/list'
        self._file_loc_url = 'https://d.pcs.baidu.com/rest/2.0/pcs/file'

    async def aio_download(self, url, **kwargs):
        _kwargs = self.kwargs.copy()
        _kwargs.update(kwargs)
        return await AioDownloader(url, loop=self.loop, **_kwargs).download()

    async def path_list(self, path, _logger=logger.log):
        buffer_file = '.%s.file_list_buffering' % path.replace('/', '__')
        expired_time = 24 * 3600
        if os.path.isfile(buffer_file) and (time.time() - os.stat(buffer_file).st_mtime) < expired_time:
            with open(buffer_file, 'r') as f:
                buf_j = json.load(f)
            file_list = buf_j['file_list']
            failed_list_dirs = buf_j['failed_list_dirs']
            if len(failed_list_dirs) == 0:
                _logger(logging.DEBUG, 'reuse buffering list')
                return file_list, failed_list_dirs
        else:
            file_list = []
            failed_list_dirs = []

        path_list_url_params = {
            'devuid': '',
            'channel': 'MAC_%s_MacBookPro15,1_netdisk_1099a' % get_mac_ver(),
            'cuid': '',
            'time': '%d' % time.time(),
            'clienttype': '21',
            'rand': '',
            'logid': '',
            'version': '2.1.0',
            'vip': '0',
            'limit': '1001',
            'order': 'time',
            'folder': '0',
            'desc': '1',
            'start': '0',
        }
        path_list_url = self._compose_baidu_url(self._path_list_url, **path_list_url_params)

        concurrent = {'a': 1}
        _failed_list_dirs = []

        async def _request_file_list(_dir, search_depth=None, depth=1):
            _url = '%s&dir=%s' % (path_list_url, quote(_dir))

            _logger(logging.INFO, 'listing dir: %s', _dir)
            j = await self.aio_request_with_retry(_url)
            concurrent['a'] -= 1

            if j is None or 'list' not in j:
                _failed_list_dirs.append(_dir)
                _logger(logging.WARNING, 'list dir %s FAILED: %s', _dir, pformat(j))
            elif len(j['list']) > 0:
                j['list'].sort(key=lambda _p: _p['isdir'], reverse=True)
                for _path in j['list']:
                    _logger(logging.DEBUG, '%s%s%s', '\t' * depth, _path['path'], '/' if _path['isdir'] else '')
                while len(j['list']) > 0:
                    futures = []
                    while len(j['list']) > 0:
                        _path = j['list'].pop(0)
                        _logger(4, 'list: %s', pformat(_path))
                        if _path['isdir']:
                            if search_depth is not None and depth+1 > search_depth:
                                continue
                            futures.append(_request_file_list(_path['path'], search_depth=search_depth, depth=depth+1))
                            concurrent['a'] += 1
                            if concurrent['a'] >= 10:
                                break
                        else:
                            file_list.append(_path)
                    if len(futures) > 0:
                        await asyncio.gather(*futures, loop=self.loop)
            _logger(logging.DEBUG, 'list dir(%d) DONE: %s', concurrent['a'], _dir)

        if len(failed_list_dirs) == 0:
            await _request_file_list(path)
            if len(file_list) == 0 and len(_failed_list_dirs) == 0 and not path.endswith('/'):
                parent_path = os.path.dirname(path)
                await _request_file_list(parent_path, search_depth=1)
                if len(file_list) > 0:
                    _found = False
                    for _f in file_list:
                        if _f['path'] == path:
                            file_list = [_f]
                            _found = True
                            break
                    if not _found:
                        return [], _failed_list_dirs
        else:
            for f_dir in failed_list_dirs:
                await _request_file_list(f_dir)

        if len(file_list) == 0 and len(_failed_list_dirs) == 0:
            return file_list, _failed_list_dirs

        file_list.sort(key=lambda _f: _f['path'])

        try:
            with open(buffer_file + '.ing', 'w') as pf:
                json.dump({
                    'file_list': file_list,
                    'failed_list_dirs': _failed_list_dirs
                }, pf, indent=2)
            os.rename(buffer_file + '.ing', buffer_file)
        except:
            pass

        return file_list, _failed_list_dirs

    async def path_file_location(self, server_path, _logger=logger.log):
        file_loc_url_params = {
            'devuid': '',
            'channel': 'MAC_%s_MacBookPro15,1_netdisk_1099a' % get_mac_ver(),
            'cuid': '',
            'time': '',
            'clienttype': '21',
            'rand': '',
            'logid': '',
            'version': '2.1.0',
            'vip': '0',
            'app_id': '',
            'err_ver': '1.0',
            'ehps': '1',
            'dtype': '1',
            'ver': '4.0',
            'dp-logid': '',
            'check_blue': '1',
            'esl': '1',
            'method': 'locatedownload',
        }
        file_loc_url = self._compose_baidu_url(self._file_loc_url, **file_loc_url_params)

        _url = '%s&path=%s' % (file_loc_url, quote(server_path))

        _logger(logging.DEBUG, "%s going to request location", server_path)
        j = await self.aio_request_with_retry(_url, _logger=_logger)
        return j['urls'] if j is not None and 'urls' in j else None

    def _compose_baidu_url(self, url, _logger=logger.log, **kwargs):
        baidu_url, params_from_confile = self._get_baidu_url_params(url)
        kwargs.update(params_from_confile)
        for idx, k in enumerate(kwargs):
            v = kwargs[k]
            baidu_url += ('?' if idx == 0 else '&') + '%s=%s' % (k, quote(v))
        return baidu_url

    async def aio_request_with_retry(self, url, header=False, _logger=logger.log):
        return await self._aio_request_with_retry(url, header=header, _logger=_logger, **self.kwargs)

    async def _aio_request_with_retry(self, url, header=False, read_timeout=58, conn_timeout=5, limit_per_host=50, retry_count=10, retry_interval=2, _logger=logger.log, **kwargs):
        kwargs.pop('n', None)
        kwargs.pop('method', None)
        io_retry = 0
        method = 'HEAD' if header else 'GET'
        while True:
            _logger(6, '%sing.#%d %s', method, io_retry, url)

            try:
                async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit_per_host=limit_per_host, enable_cleanup_closed=True, force_close=True),
                                                 conn_timeout=conn_timeout, raise_for_status=True, loop=self.loop) as session:
                    with Timeout(conn_timeout + 1, loop=self.loop):
                        resp = await session.request(method=method, url=url, **kwargs)
                        if header:
                            _logger(6, '%s headers: %s', url[:50], resp.headers)
                            return resp.headers

                    with Timeout(read_timeout, loop=self.loop):
                        j = await resp.json(content_type='')

                    return j
            except (asyncio.TimeoutError, aiohttp.ClientResponseError) as err:
                _logger(logging.WARNING, '%sing.#%d %s\n\terror: %s', method, io_retry, url, err)
                if isinstance(err, aiohttp.ClientResponseError):
                    if 400 <= err.code < 500:
                        break
                io_retry += 1
                if io_retry < retry_count:
                    await asyncio.sleep(retry_interval * io_retry, loop=self.loop)
                else:
                    break
        return None

    def _get_baidu_url_params(self, url: str) -> (str, dict):
        """
        从json配置文件中读取baidu访问的url的和参数
        json文件格式格式：
            {
                "headers": {
                    "params": {
                        "<key1>": "<value1>",
                        "<key2>": "<value2>",
                        ...
                    }
                }
                "<URL-1>": {
                    "url": "<alternative_url>",
                    "params": {
                        "devuid": "<alternative_devuid>",
                        ...
                    }
                },
                "<URL-2>": {
                    "url": ...,
                    "params": ...
                }
            }
          <URL-*> 可能是
            1. https://pan.baidu.com/api/list （list目录的URL）
            2. https://d.pcs.baidu.com/rest/2.0/pcs/file （获取文件下载location的URL）
        :param url:
        :return:
        """
        if url in self.conf:
            conf = self.conf[url]
            if 'url' in conf:
                url = conf['url']
            if 'params' in conf:
                return url, conf['params'].copy()
        return url, {}

