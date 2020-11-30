import asyncio
import logging
import os
import time
from concurrent.futures import CancelledError
from urllib.parse import urlparse

import aiohttp
from aiohttp import hdrs, client_exceptions as errors

from pyclda import fmt_human_bytes, fmt_human_time, async_MD5
from tsproxy.common import Timeout

__all__ = ['AioDownloader']


class Progress(list):

    def __init__(self, iterable=None):
        super().__init__(iterable)
        self.active = True
        self._done_percent = -1
        self._next_read_len = 0

    @property
    def start0(self):
        return self[0]

    @start0.setter
    def start0(self, v):
        self[0] = v

    @property
    def pos(self):
        return self[1]

    @pos.setter
    def pos(self, v):
        self[1] = v

    @property
    def last_read_len(self):
        return self._next_read_len

    @last_read_len.setter
    def last_read_len(self, _last_len):
        if _last_len >= self._next_read_len:
            self._next_read_len = int(_last_len * 1.1)
        elif _last_len > 0:
            self._next_read_len = _last_len
        if len(self) < 4:
            self.append(self._next_read_len)
        elif self[3] < self._next_read_len:
            self[3] = self._next_read_len

    @property
    def end(self):
        return self[2]

    @end.setter
    def end(self, v):
        self[2] = v

    def is_done(self):
        return self.end is not None and self.pos > self.end

    def __done_percent(self, ret_float=False):
        if self.end is None:
            return 0
        if ret_float:
            return (self.pos - self.start0) / (self.end - self.start0 + 1)
        return int((self.pos - self.start0) * 100 / (self.end - self.start0 + 1))

    def done_percent(self, ret_float=False):
        p = self.__done_percent(ret_float=ret_float)
        self._done_percent = p
        return p

    def done_percent_changed(self):
        p = self.__done_percent()
        if p != self._done_percent:
            # logger.debug("progress p=%d, self._done_percent=%d", p, self._done_percent)
            self._done_percent = p
            return True
        else:
            return False

    def eta(self, speed):
        if self.end is None or speed == 0:
            return None
        return (self.end - self.pos + 1) / speed

    def buf_len(self):
        if self.end is None:
            return max(10240, self._next_read_len)
        return min(max(10240, self._next_read_len), self.end - self.pos + 1)


class Status(dict):

    def __init__(self, **kwargs):
        super().__init__()
        self.initial()
        self.update(**kwargs)
        self._done_percent = -1

    def initial(self):
        self['con_len'] = None
        self['con_down'] = 0
        self['start_down'] = 0
        self['_start_time'] = time.time()
        self['_log_time'] = time.time()
        self['break_cont'] = True
        self['filename'] = None
        self['con_md5'] = None

    def __done_percent(self):
        if self['con_len'] is None:
            return 0
        return int(self['con_down'] * 100 / self['con_len'])

    def done_percent(self):
        p = self.__done_percent()
        self._done_percent = p
        return p

    def done_percent_changed(self):
        p = self.__done_percent()
        if p != self._done_percent:
            # logger.debug("status p=%d, self._done_percent=%d", p, self._done_percent)
            self._done_percent = p
            return True
        else:
            return False

    def down_speed(self):
        return (self['con_down'] - self['start_down']) / (time.time() - self['_start_time'])

    def eta(self):
        if self['con_len'] is None or self.down_speed() == 0:
            return None
        return (self['con_len'] - self['con_down']) / self.down_speed()

    def log(self, logger, log_level=logging.WARNING, inactive=True):
        for idx in sorted(self, key=lambda k: k if isinstance(k, int) else 999):
            if not isinstance(idx, int):
                break
            logger.log(log_level, 'task[%d]: %s %s', idx, self[idx], 'inactive' if not self[idx].active and inactive else '')

    def on_loaded(self, out_file, url):
        pass

    def on_done_percent_changed(self, index=None):
        pass

    def refresh(self):
        pass

    def flush(self):
        pass

    @property
    def active_n(self):
        return self['_active_n'] if '_active_n' in self else 0

    @active_n.setter
    def active_n(self, n):
        self['_active_n'] = n


class RemoteFileChanged(Exception):

    def __init__(self, before, after):
        self.before = before
        self.after = after


class AioDownloader:
    AD_IDX = 0

    def __init__(self, url, method='GET', out_file=None, n=0, conn_timeout=5, limit_per_host=50, status=None, loop=None, **kwargs):
        self.url = url
        self.method = method
        self.out_file = out_file
        self.n = min(50, n)
        self.conn_timeout = conn_timeout
        self.limit_per_host = limit_per_host
        self.status = status if status is not None else Status(_filename=out_file)
        self.loop = loop if loop is not None else asyncio.get_event_loop()
        if 'headers' in kwargs:
            dict_headers = kwargs['headers']
            dict_headers.setdefault('User-Agent', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.41 Safari/537.36')
        self.kwargs = kwargs

        self.status.initial()
        self._out_file, self._status_file = self.init_filenames()
        self.start_download_tasks = None
        self.logger = None

    async def download(self):
        self.logger = logging.getLogger('%s.%d' % (__name__, AioDownloader.AD_IDX))
        AioDownloader.AD_IDX += 1
        try:
            return await self._download()
        finally:
            AioDownloader.AD_IDX -= 1

    async def _download(self):
        logger = self.logger
        logger.warning('%sing %s', self.method, self.url)
        break_cont = False  # 断点续传
        if not os.path.exists(self._out_file):
            with open(self._out_file, 'wb'):
                pass
        if os.path.exists(self._status_file):
            break_cont = True
            with open(self._status_file, 'r+b') as status_fd:
                self._recover_last_status(status_fd)
        if self.n <= 0:
            self.n = 6
        elif self.n > 50:
            self.n = 50
        self.status.on_loaded(out_file=self._out_file, url=self.url)

        futures = []
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit_per_host=self.limit_per_host, enable_cleanup_closed=True, force_close=True, loop=self.loop),
                                         auto_decompress=False, conn_timeout=self.conn_timeout, loop=self.loop) as session:

            async def __start_download_task(_index=0):
                if _index > 0:
                    await asyncio.sleep(_index)
                return await DownloadTask(self, self._out_file, self._status_file, self.logger, index=_index, loop=self.loop, **self.kwargs).adownload(session)

            def _start_download_tasks(count):
                start_index = len(futures)
                for idx in range(start_index, start_index + count):
                    _future = asyncio.ensure_future(__start_download_task(idx), loop=self.loop)
                    futures.append(_future)

            self.start_download_tasks = _start_download_tasks
            self.start_download_tasks(1)
            try:
                await futures[0]
            except CancelledError:
                pass
            except RemoteFileChanged as rfc:
                logger.warning('Content-Length changed(%d->%d), restart from the begin? %s', rfc.before, rfc.after, 'YES' if break_cont else 'NO')
                logger.warning('Please delete files: "%s" and "%s" manually', self._out_file, self._status_file)
                if break_cont:
                    self.status.initial()
                    self._out_file, self._status_file = self.init_filenames(create_new=True)
                    return await self.download()
                else:
                    raise
            success, cancelled = await self.await_completed(futures)
            return success, self._out_file, cancelled

    def init_filenames(self, create_new=False):
        out_file = self.out_file
        if out_file is None:
            o_url = urlparse(self.url)
            out_file = os.path.basename(o_url.path)
        else:
            out_file = out_file.strip()
            dirs = os.path.dirname(out_file)
            if dirs and not os.path.isdir(dirs):
                os.makedirs(dirs, exist_ok=True)

        _out_file = out_file
        for i in range(1, 200):
            status_file = '%s.axel.st' % out_file
            if not os.path.exists(out_file) or os.stat(out_file).st_size == 0:
                # with open(out_file, 'wb'):
                #     pass
                break
            elif create_new or not os.path.exists(status_file):
                _t_suf = _out_file.rsplit('.', maxsplit=1)
                if len(_t_suf) > 1:
                    out_file = '%s(%d).%s' % (_t_suf[0], i, _t_suf[1])
                else:
                    out_file = '%s(%d)' % (_out_file, i)
            elif i >= 100:
                raise Exception('too many same filename[%s] files' % _out_file)
            else:
                break
        return out_file, status_file

    def _recover_last_status(self, status_fd):
        logger = self.logger
        n = self.n
        last_n = self._load_status(status_fd)
        logger.warning('recover from last download: %d/%d/%d', last_n, self.status['con_down'], self.status['con_len'])
        if n <= 0:
            n = last_n
        elif n > last_n:
            # 增加下载连接数
            new_n = self._extend_status(status_fd, last_n, new_n=n)
            logger.warning('extend download tasks from %d to %d', last_n, new_n)
            n = new_n
        elif n < last_n:
            # 减少下载连接数
            for i in range(n, last_n):
                if self.status[i].pos > self.status[i].end:
                    del self.status[i]
                else:
                    self.status[i].active = False
            logger.warning('reduce download tasks from %d to %d', last_n, n)
        self.status.log(self.logger)
        self.n = n
        return n

    def _load_status(self, status_fd):
        status_fd.seek(0)
        n = int.from_bytes(status_fd.read(2), 'big')
        self.status['con_len'] = int.from_bytes(status_fd.read(8), 'big')
        self.status['con_down'] = int.from_bytes(status_fd.read(8), 'big')
        for idx in range(0, n):
            _start0 = int.from_bytes(status_fd.read(8), 'big')
            _start = int.from_bytes(status_fd.read(8), 'big')
            _end = int.from_bytes(status_fd.read(8), 'big')
            self.status[idx] = Progress([_start0, _start, _end])
        self.status['start_down'] = self.status['con_down']
        return n

    def _extend_status(self, status_fd, last_n, new_n):
        status = self.status
        for i in range(last_n, new_n):
            if last_n >= new_n:
                break
            for idx in sorted(status, key=lambda k: status[k].end - status[k].pos + 1 if isinstance(k, int) else -1, reverse=True)[:last_n]:
                if last_n >= new_n or not isinstance(status[idx], Progress):
                    break
                undown = status[idx].end - status[idx].pos + 1
                if undown >= 10240 * last_n:
                    # initial new segment
                    new_st_pos = int((status[idx].end + status[idx].pos) / 2)
                    status[last_n] = Progress([new_st_pos, new_st_pos, status[idx].end])
                    status_fd.seek(24 * last_n + 18)
                    status_fd.write(new_st_pos.to_bytes(8, 'big'))
                    status_fd.write(new_st_pos.to_bytes(8, 'big'))
                    status_fd.write(status[idx].end.to_bytes(8, 'big'))

                    # update old segment' END position
                    old_end_pos = new_st_pos - 1  # old segment end position
                    status_fd.seek(24 * idx + 34)
                    status[idx].end = old_end_pos
                    status_fd.write(old_end_pos.to_bytes(8, 'big'))

                    last_n += 1
                else:
                    break
        status_fd.seek(0)
        status_fd.write(last_n.to_bytes(2, 'big'))
        status_fd.flush()
        return last_n

    def on_http_resp_ok(self, resp):
        status = self.status
        con_len, con_md5 = self._parse_http_headers(resp)
        if con_len is None:
            self.n = 1
            status['break_cont'] = False
        elif status['con_len'] is not None and status['con_len'] != con_len:
            # TODO 与上次Content-Length、Etag或MD5不等，则重新下载
            raise RemoteFileChanged(status['con_len'], con_len)
        else:
            if status['break_cont']:
                self.n = max(1, min(int(status['con_len'] / 10240), self.n))
                if not os.path.exists(self._status_file):
                    with open(self._status_file, 'w+b') as status_fd:
                        self._init_status(status_fd)
                    status.log(self.logger)

                self.start_download_tasks(self.n - 1)
            else:
                self.n = 1
                status[0].end = status['con_len'] - 1

            status.refresh()

    def _parse_http_headers(self, resp) -> (int, str):
        logger = self.logger
        status = self.status

        con_len = resp.headers.get(hdrs.CONTENT_LENGTH)
        logger.warning('Content-Length: %s', con_len)
        if status['con_len'] is None and con_len:
            status['con_len'] = int(con_len)

        con_disp = resp.headers.get(hdrs.CONTENT_DISPOSITION)
        if con_disp:
            _fs = con_disp.split('filename=', 1)
            if len(_fs) == 2:
                status['filename'] = _fs[1].strip("'").strip('"')
                logger.warning('Filename: %s', status['filename'])

        con_md5 = resp.headers.get(hdrs.CONTENT_MD5)
        if con_md5:
            logger.warning('Content-MD5: %s', con_md5)
        if status['con_md5'] is None and con_md5:
            status['con_md5'] = con_md5

        return None if con_len is None else int(con_len), con_md5

    def _init_status(self, status_fd):
        status = self.status
        n = self.n
        con_len = status['con_len']
        unit = int(con_len / n)
        status_fd.seek(0)
        status_fd.write(n.to_bytes(2, 'big'))
        status_fd.write(con_len.to_bytes(8, 'big'))
        status_fd.write(int(0).to_bytes(8, 'big'))
        for i in range(0, n):
            if i == 0:
                _start = 0
            else:
                _start = unit * i
            if i == (n - 1):
                _end = con_len - 1
            else:
                _end = unit * (i + 1) - 1
            if i in status:
                status[i].start0 = _start
                status[i].pos = _start
                status[i].end = _end
            else:
                status[i] = Progress([_start, _start, _end])
            status_fd.write(_start.to_bytes(8, 'big'))
            status_fd.write(_start.to_bytes(8, 'big'))
            status_fd.write(_end.to_bytes(8, 'big'))
        status_fd.flush()

    async def await_completed(self, futures):
        cancelled = None
        success = True
        while True:
            try:
                _done, _pending = await asyncio.wait(futures, loop=self.loop)
                if len(_pending) > 0:
                    success = False
                    break
                for f in _done:
                    if f.cancelled():
                        success = False
                        if cancelled is None:
                            cancelled = CancelledError()
                        break
                    _d, _c = f.result()
                    if _c is not None and cancelled is None:
                        cancelled = _c
                    if not _d:
                        success = False
                        break
                break
            except CancelledError as ce:
                cancelled = ce
                continue
        logger = self.logger
        status = self.status
        if (status['con_len'] is None and not success) or (status['con_len'] is not None and status['con_len'] != status['con_down']):
            # segment下载fail后，其他task会尝试，所以`success`不是判断下载成功的必要条件
            success = False
            logger.warning('====FAIL====')
            logger.warning('%s %s FAIL', self.method, self.url)
            for k in sorted(status, key=lambda sk: 'z' + str(sk) if isinstance(sk, int) else sk):
                if isinstance(k, str) and not k.startswith('_'):
                    logger.warning('%-10s: %s', k, status[k])
            status.log(self.logger, inactive=False)
            logger.warning('========')
        else:
            success = True
            if os.path.isfile(self._status_file):
                os.remove(self._status_file)
            logger.warning('=============SUCCESS=============')
            logger.warning('%s %s%s DONE used [%s], speed [%sB/S]', self.method, self.url[:80], '' if len(self.url) < 80 else '...', fmt_human_time(time.time() - status['_start_time']), fmt_human_bytes(status.down_speed()))
            filename = status['filename']
            if filename is not None and self.out_file is None and filename != self._out_file:
                self._out_file = self._rename_to(filename)
                logger.warning('save to server-side name: %s', self._out_file)
            elif self.out_file is None:  # or self._out_file != self.out_file
                logger.warning('save to name in url: %s', self._out_file)
            else:
                logger.warning('save to %s', self._out_file)
            if status['con_md5'] is not None:
                logger.warning('server md5: %s, calculating local file md5...', status['con_md5'])
                _md5 = await async_MD5(self._out_file, loop=self.loop)
                if _md5.lower() != status['con_md5'].lower():
                    logger.warning('local file md5: %s', _md5)
                else:
                    logger.warning('congratulations! md5 is same!')
            logger.warning('==========================')
        return success, cancelled

    def _rename_to(self, out_file):
        _out_file = out_file
        for i in range(1, 200):
            if not os.path.exists(out_file):
                os.rename(self._out_file, out_file)
                return out_file
            elif i >= 100:
                self.logger.warning('too many same filename[%s] files' % _out_file)
                break
            else:
                _t_suf = _out_file.rsplit('.', maxsplit=1)
                if len(_t_suf) > 1:
                    out_file = '%s(%d).%s' % (_t_suf[0], i, _t_suf[1])
                else:
                    out_file = '%s(%d)' % (_out_file, i)
        return _out_file


class DownloadTask:

    def __init__(self, downloader: AioDownloader, out_file, status_file, logger, index=0, read_timeout=58, restart_on_done=True, retry_count=10, retry_interval=2, headers=None, loop=None, **kwargs):
        self.downloader = downloader
        self.out_file = out_file
        self.status_file = status_file
        self.index = index
        self.read_timeout = read_timeout
        self.restart_on_done = restart_on_done
        self.retry_count = retry_count
        self.retry_interval = retry_interval
        self.headers = headers.copy() if headers is not None else {}
        self.loop = loop if loop is not None else asyncio.get_event_loop()
        self.kwargs = kwargs

        self._io_retry = 0
        self._status = downloader.status
        self._status_fd = None
        self._logger = logger

    def save_status(self, idx, read_len, start0=None, end=None):
        status = self._status
        status_fd = self._status_fd
        if start0 is not None:
            status[idx].start0 = start0
            status_fd.seek(24 * idx + 18)
            status_fd.write(start0.to_bytes(8, 'big'))
        else:
            status_fd.seek(24 * idx + 26)
        status[idx].pos += read_len
        status[idx].last_read_len = read_len
        status_fd.write(status[idx].pos.to_bytes(8, 'big'))
        status['con_down'] += read_len
        if end is not None:
            status[idx].end = end
            status_fd.write(end.to_bytes(8, 'big'))

    def re_allocate_status(self):
        status_fd = self._status_fd
        index = self.index
        if status_fd is None:
            return None
        status = self._status
        for idx in sorted(status, key=lambda k: status[k].end-status[k].pos+1 if isinstance(k, int) else -1, reverse=True):
            if index == idx:
                break
            undown = status[idx].end - status[idx].pos + 1
            if undown > 0 and not status[idx].active:
                status[index], status[idx] = status[idx], status[index]
                self.save_status(index, 0, start0=status[index].start0, end=status[index].end)
                self.save_status(idx, 0, start0=status[idx].start0, end=status[idx].end)
                del status[idx]
                status_fd.flush()
                return idx
            if undown >= 40960 * self.downloader.n:
                pos = int((status[idx].end + status[idx].pos) / 2)
                status[index].pos = pos
                self.save_status(index, 0, start0=pos, end=status[idx].end)
                self.save_status(idx, 0, end=pos-1)
                status_fd.flush()
                return idx
            break
        return None

    def prepare_http_headers(self, is_first_request=False) -> (bool, dict):
        headers = self.headers.copy()
        status = self._status
        index = self.index
        # if status['break_cont'] and status[index].is_done():
        #     if index == 0:
        #         # 断点重启时，即使0号线程(协程)已完成，还需要由它重新获取服务端信息
        #         headers['Range'] = 'bytes=0-'
        #     else:
        #         self._log(logging.INFO, 'DONE with (%d-%d)', status[index].start0, status[index].end)
        #         return True, headers
        # elif status['break_cont']:
        #     if index == 0 and is_first_request:
        #         # 获取服务端文件长度等信息
        #         headers['Range'] = 'bytes=0-'
        #     else:
        #         headers['Range'] = 'bytes=%d-%s' % (status[index].pos, '' if status[index].end is None else status[index].end)
        if status['break_cont']:
            if index == 0 and is_first_request:
                # 断点重启时，由0号线程(协程)获取服务端信息
                headers['Range'] = 'bytes=0-'
            elif status[index].is_done():
                self._log(logging.INFO, 'DONE with (%d-%d)', status[index].start0, status[index].end)
                return True, headers
            else:
                headers['Range'] = 'bytes=%d-%s' % (status[index].pos, '' if status[index].end is None else status[index].end)
            self._log(logging.INFO, 'downloading with Range: %s', headers['Range'])
        else:
            if 'Range' in headers:
                del headers['Range']
            status[index].pos = 0
            status['con_down'] = 0
            status['start_down'] = 0
            status['_start_time'] = time.time()
            self._log(logging.INFO, 'downloading without Range')
        return False, headers

    def on_http_resp_not_ok(self, resp):
        status = self._status
        index = self.index
        status_fd = self._status_fd
        self._log(logging.WARNING, '%d %s', resp.status, resp.reason)
        self._log(logging.WARNING, '%s', resp.headers)
        cont = True
        done = False
        if 416 == resp.status:
            if index == 0:
                if not status['break_cont']:
                    cont = False
                else:
                    status['break_cont'] = False
            else:
                del status[index]
                done = True
                self._log(logging.WARNING, 'DONE cause server NOT support break&cont')
                if status['break_cont']:
                    status['break_cont'] = False
                    status[0].end = status['con_len'] - 1
                    if status_fd is not None:
                        status_fd.seek(0)
                        status_fd.write(int(1).to_bytes(2, 'big'))
                        status_fd.seek(34)
                        status_fd.write(status[0].end.to_bytes(8, 'big'))
                cont = False
        elif 400 <= resp.status < 500:
            cont = False
        return cont, done

    async def stream_to_file(self, resp, _s_time):
        with open(self.out_file, 'r+b') as fd:
            return await self._stream_to_file(resp, fd, _s_time)

    async def _stream_to_file(self, resp, fd, _s_time):
        status = self._status
        index = self.index
        p = fd.seek(status[index].pos)
        if p != status[index].pos:
            self._log(logging.ERROR, 'fd.seek(%d) fail', status[index].pos)
            return False

        _s_p = status[index].done_percent()
        self._log(logging.INFO, 'START reading %s %d/%d/%s(%d%%)',
                  fmt_human_bytes(status[index].end - status[index].pos + 1) if status[index].end is not None else '', status[index].start0, status[index].pos, status[index].end, _s_p)
        status['done_per'] = -1
        _s_pos = status[index].pos
        _timeout_start = time.time()
        _log_time = 0
        while True:
            _buf_len = status[index].buf_len()
            if _buf_len <= 0:
                break
            try:
                with Timeout(1, loop=self.loop):
                    # if _buf_len > 10240:
                    #     _log(logging.DEBUG, "going to read %d", _buf_len)
                    chunk = await resp.content.read(_buf_len)
                if not chunk:
                    self._log(5, 'chunk is False')
                    break
                _read_len = len(chunk)
                fd.write(chunk)
                if self._status_fd is not None:
                    self.save_status(self.index, _read_len)
                else:
                    status[index].pos += _read_len
                    status[index].last_read_len = _read_len
                    status['con_down'] += _read_len
                self._log(5, 'read %d bytes %d/%d/%s', _read_len, status[index].start0, status[index].pos, status[index].end)
            except asyncio.TimeoutError:
                _timeout = time.time() - _timeout_start
                if _timeout >= self.read_timeout:
                    raise asyncio.TimeoutError('read timeout %.1f > %d' % (_timeout, self.read_timeout))
            else:
                _timeout_start = time.time()
            _s_time, _s_pos, _s_p, _log_time = self.do_log(_s_time, _s_pos, _s_p, _log_time)
        return True

    def do_log(self, _s_time, _s_pos, _s_p, _log_time):
        status = self._status
        index = self.index
        __now = time.time()
        _time_thro = __now - status['_log_time']
        if _time_thro > 0.99 and (__now - _log_time) > status.active_n*0.99:
            __p = status[index].done_percent()
            __all_p = status.done_percent()

            if status['done_per'] != __all_p or _s_p != __p:
                self._status.refresh()

            if (not self._logger.isEnabledFor(logging.WARNING) and status['done_per'] == __all_p) or (not self._logger.isEnabledFor(logging.INFO) and _s_p == __p):
                self._status.flush()
                return _s_time, _s_pos, _s_p, _log_time

            status['_log_time'] = __now
            _log_time = __now
            _s_time_thro = __now - _s_time
            _s_read_len = status[index].pos - _s_pos
            if _s_read_len > 0:
                _s_time = __now
                _s_pos = status[index].pos
            _speed = _s_read_len / _s_time_thro
            _eta = fmt_human_time(status[index].eta(_speed), status, '_eta_len')
            _speed = fmt_human_bytes(_speed, status, '_speed_len')
            _all_speed = fmt_human_bytes(status.down_speed(), status, '_all_speed_len')
            _all_speed2 = fmt_human_bytes((status['con_down'] - (status['_log_down'] if '_log_down' in status else status['start_down'])) / _time_thro, status, '_all_speed2_len')
            status['_log_down'] = status['con_down']
            _all_eta = fmt_human_time(status.eta(), status, '_all_eta_len')
            self._log(logging.WARNING if status['done_per'] != __all_p else logging.INFO if _s_p != __p else logging.DEBUG,
                      '[ETA %s %sB/S] [%sB/S] [%s %sB/S] %d/%d/%s(%d%%/%d%%) read %d bytes in %.1f seconds',
                      _all_eta, _all_speed, _all_speed2, _eta, _speed,
                      status[index].start0, status[index].pos, status[index].end, __p, __all_p, _s_read_len, _s_time_thro)
            _s_p = __p
            status['done_per'] = __all_p
        elif status[index].done_percent_changed():
            self._status.on_done_percent_changed(index)
        elif status.done_percent_changed():
            self._status.on_done_percent_changed()
        return _s_time, _s_pos, _s_p, _log_time

    def on_task_done(self) -> bool:
        """
        当一个download task完成时，判断是否要继续下载文件其他部分
        :return: True 如果要继续尝试download task
        """
        if self.restart_on_done:
            idx = self.re_allocate_status()
            if idx is not None:
                self._log(logging.INFO, 'RESTART with (%s) for #%d(%s)', self._status[self.index], idx, self._status[idx] if idx in self._status else 'takeover')
                return True
            else:
                return False
        else:
            return False

    def _log(self, level, msg, *log_args, exc_info=False, **log_kwargs):
        if not self._logger.isEnabledFor(level):
            return
        if self.downloader.n > 10:
            self._logger.log(level, '#%02d.%d ' % (self.index, self._io_retry) + msg, *log_args, exc_info=exc_info, **log_kwargs)
        else:
            self._logger.log(level, '#%d.%d ' % (self.index, self._io_retry) + msg, *log_args, exc_info=exc_info, **log_kwargs)

    async def adownload(self, session):
        _log = self._log
        status = self._status
        index = self.index

        cancelled_err = None
        done = False

        status.active_n += 1
        try:
            if os.path.isfile(self.status_file):
                self._status_fd = open(self.status_file, 'r+b')
            else:
                status[index] = Progress([0, 0, None])

            http_code = 200
            io_error = None
            is_first_request = True
            while True:
                if not status[index].active:
                    status[index].active = True
                    _log(logging.INFO, '%s activated', status[index])

                if io_error is not None or http_code < 200 or http_code >= 300:
                    self._io_retry += 1
                    if self._io_retry < self.retry_count:
                        await asyncio.sleep(self.retry_interval * self._io_retry, loop=self.loop)
                    else:
                        break

                _done, headers = self.prepare_http_headers(is_first_request)
                if _done:
                    if self.on_task_done():
                        continue
                    else:
                        done = True
                        break

                resp = None
                io_error = None
                _s_time = time.time()
                try:

                    # with Timeout(self.conn_timeout+1, loop=self.loop):
                    resp = await session.request(method=self.downloader.method, url=self.downloader.url, timeout=None, headers=headers, **self.kwargs)

                    http_code = resp.status
                    if http_code < 200 or http_code >= 300:
                        cont, done = self.on_http_resp_not_ok(resp)
                        if cont:
                            continue
                        else:
                            break

                    # # for test
                    # if self.retry_count == 3:
                    #     raise IOError('for test retry_count=%d' % self.retry_count)

                    if index == 0 and is_first_request:  # len(futures) == 0 and status['break_cont']:
                        is_first_request = False
                        self.downloader.on_http_resp_ok(resp)
                        if status['break_cont'] and self._status_fd is not None:
                            # 处于断点续传，则重新从断点处下载
                            continue
                        elif self._status_fd is None:
                            self._status_fd = open(self.status_file, 'r+b')

                    await self.stream_to_file(resp, _s_time)

                    if status['con_len'] is None or status[index].is_done():
                        _log(logging.INFO, 'DONE with (%d/%s)', status[index].start0, status[index].end)
                        if self.on_task_done():
                            continue
                        else:
                            done = True
                            break
                    else:
                        raise IOError('stream_to_file(#%d) interrupted' % index)
                except (IOError, errors.ClientError, asyncio.TimeoutError) as ioe:
                    _log(logging.WARNING, 'IOError: %s(%s)', ioe.__class__.__name__, ioe)
                    io_error = ioe
                finally:
                    if resp is not None:
                        resp.close()
        except CancelledError as ex:
            cancelled_err = ex
        except RemoteFileChanged:
            raise
        except BaseException as ex:
            _log(logging.ERROR, 'Error: %s(%s)', ex.__class__.__name__, ex, exc_info=True)
        finally:
            if self._status_fd is not None:
                try:
                    self._status_fd.seek(10)
                    self._status_fd.write(status['con_down'].to_bytes(8, 'big'))
                    self._status_fd.seek(24 * index + 18)
                    self._status_fd.close()
                except:
                    pass
            status.active_n -= 1
        if not done:
            status[index].active = False
            _log(logging.INFO, 'NOT DONE%s: %s', '(cancelled)' if cancelled_err is not None else '', status[index] if index in status else '')
        else:
            del status[index]
        return done, cancelled_err

