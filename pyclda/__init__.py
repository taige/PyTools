#!/usr/bin/env python3

import argparse
import asyncio
try:
    import curses
except:
    pass
import functools
import logging
import math
import os
import signal
import sys
import time
import platform
import json
from concurrent.futures import CancelledError
from urllib.parse import urlparse, quote_plus as quote
from pprint import pformat

import aiohttp
import uvloop
from aiohttp import client_exceptions as errors
from aiohttp import formdata, hdrs

from tsproxy.common import MyThreadPoolExecutor, Timeout
from tsproxy.common import fmt_human_bytes as _fmt_human_bytes
from tsproxy.common import fmt_human_time as _fmt_human_time

from tsproxy.version import version

__version__ = version

__all__ = ['aio_download', 'aio_download_path']

logger = logging.getLogger(__name__)

SCR_ROWS = None
SCR_COLS = None

md5_executor = None


def count_rows(string, cols=10):
    str_rows = 0
    s = 0
    while True:
        e = string.find('\n', s)
        if e < 0:
            if s < len(string):
                str_rows += math.ceil((len(string) - s) / cols)
            break
        _s = e + 1
        if string[e-1] == '\r':
            e -= 1
        if e == s:
            str_rows += 1
        else:
            str_rows += math.ceil((e - s) / cols)
        s = _s
    return str_rows


class IndicatorWindow:
    SINGLETON = None
    ROWS = 0

    def __init__(self, status, out_file=None, url=None, y=0, x=0):
        self._pwin = curses.newwin(IndicatorWindow.ROWS, SCR_COLS, y, x)
        self._pwin.border(0)
        self._pwin.noutrefresh()
        self._width = SCR_COLS - 2
        self._height = IndicatorWindow.ROWS - 2
        self._win = self._pwin.subwin(self._height+1, self._width, 1, 1)
        self._win.keypad(1)
        self._status = status
        self._full_num = self._width * self._height
        self._con_len = status['con_len']
        self._out_file = out_file
        self._segments = {}
        if url is not None:
            if len(url) > SCR_COLS*0.7:
                self._url = url[:int(SCR_COLS*0.7)] + '...'
            else:
                self._url = url
            self._pwin.addstr(self._height+1, int((SCR_COLS - len(self._url)) / 2), '[ ' + self._url + ' ]')
            self._pwin.noutrefresh()
        logger.debug('IndicatorWindow: %d x %d = %d', self._width, self._height, self._full_num)
        IndicatorWindow.SINGLETON = self

    def flush(self):
        curses.doupdate()

    def getyx(self, pos=None, yx=None):
        if yx is None:
            yx = math.trunc(self._full_num * pos / (self._con_len - 1))
        y = math.trunc(yx / self._width)
        x = (yx % self._width)
        return yx, y, x

    def move(self, yx):
        self._win.move(math.trunc(yx / self._width), yx % self._width)

    def mark_progress(self, yx, start=None, end=None, prog=None):
        if prog is not None:
            start, pos, end, active = prog.start0, prog.pos, prog.end, prog.active
        else:
            pos = prog.pos if prog is not None else None
            active = False
        _, y, x = self.getyx(start, yx)
        if end >= self._con_len - 1:
            end_yx, end_y, end_x = self.getyx(end, self._full_num - 1)
        else:
            end_yx, end_y, end_x = self.getyx(end)

        if pos is not None and pos <= end:
            done_yx, done_y, done_x = self.getyx(pos)
            done_yx -= 1
            if done_yx == end_yx:
                done_yx -= 1
                logger.log(4, '%d/%s/%d==== done_yx=%d(%d,%d)', start, pos, end, done_yx, done_y, done_x)
            if done_yx + 1 < yx:
                done_yx = yx - 1
        else:
            done_yx, done_y, done_x = end_yx, end_y, end_x
        logger.log(4, '%d/%s/%d==== %d(%d,%d)/%d(%d,%d)/%d(%d,%d)', start, pos, end, yx, y, x, done_yx, done_y, done_x, end_yx, end_y, end_x)

        blink_num = 1 if pos is not None and pos <= end else 0
        done_num = done_yx - yx + 1
        undone_num = end_yx - done_yx - blink_num

        percent = 100 if pos is None else int((pos - start) * 100 / (end - start + 1))
        p_str = "%d%%" % percent
        percent_before_blink = percent_after_blink = None
        if undone_num >= len(p_str):
            undone_num -= len(p_str)
            percent_after_blink = p_str
            logger.log(4, '%d/%s/%d ==== [► x %d, blink x %d, %s, ▻ x %d]', start, pos, end, done_num, blink_num, p_str, undone_num)
        elif done_num >= len(p_str):
            done_num -= len(p_str)
            percent_before_blink = p_str
            logger.log(4, '%d/%s/%d ==== [► x %d, %s, blink x %d, ▻ x %d]', start, pos, end, done_num, p_str, blink_num, undone_num)

        if done_num > 0:
            self._win.addstr("►" * done_num)
        if percent_before_blink is not None:
            self._win.addstr(percent_before_blink, curses.color_pair(curses.COLOR_GREEN))
        if blink_num > 0:
            if active:
                self._win.addstr("►", curses.A_BLINK | curses.color_pair(curses.COLOR_GREEN))
            else:
                self._win.addstr("►", curses.A_BLINK | curses.color_pair(curses.COLOR_RED))
        if percent_after_blink is not None:
            self._win.addstr(percent_after_blink, curses.color_pair(curses.COLOR_GREEN))
        if undone_num > 0:
            self._win.addstr("▻" * undone_num)
        return end_yx

    def progress_cmp(self, idx1, idx2):
        status = self._status
        if isinstance(status[idx1], Progress) and isinstance(status[idx2], Progress):
            c = status[idx1].start0 - status[idx2].start0
            if c == 0:
                c = status[idx1].pos - status[idx2].pos
            return c
        elif not isinstance(status[idx1], Progress):
            return -1
        elif not isinstance(status[idx2], Progress):
            return 1

    def refresh_status(self, index=None, done_percent_changed=False):
        status = self._status
        pos = yx = 0
        if index is None:
            self._win.move(0, 0)
            for idx in sorted(status, key=functools.cmp_to_key(self.progress_cmp)):
                if not isinstance(status[idx], Progress):
                    continue
                prog = status[idx]
                if prog.start0 > pos:
                    yx = self.mark_progress(yx, pos, prog.start0-1) + 1
                elif prog.start0 < pos:
                    logger.debug('error status[%d]: %s', idx, prog)
                    continue
                self._segments[idx] = yx
                yx = self.mark_progress(yx, prog=prog) + 1
                pos = prog.end+1
            if pos < self._con_len:
                self.mark_progress(yx, pos, self._con_len - 1)
        else:
            yx = self._segments[index]
            self.move(yx)
            self.mark_progress(yx, prog=status[index])
            self._win.move(self._height, 0)
            # logger.debug("only refresh status[%d]: %s %d%%", index, status[index], status[index].done_percent())
        self._win.noutrefresh()
        if status.done_percent_changed() or done_percent_changed:
            # self._pwin.border(0)
            p_str = "[ %s %3d%% ]" % (self._out_file, status.done_percent())
            # logger.debug("self._pwin.p_str=%s", p_str)
            self._pwin.addstr(0, int((SCR_COLS - len(p_str)) / 2), p_str)
            self._pwin.noutrefresh()


class ScrollablePad:
    SINGLETON = None

    def __init__(self, rows, cols, sminy, sminx, smaxy, smaxx=None):
        self._rows = rows
        self._cols = cols
        self._sminy = sminy
        self._sminx = sminx
        self._smaxy = smaxy
        self._smaxx = smaxx if smaxx is not None else cols - 1
        self._swidth = self._smaxx - self._sminx + 1
        self._sheight = self._smaxy - self._sminy + 1
        self._y = self._x = 0
        self._pad = curses.newpad(rows, cols)
        self._pad.keypad(1)
        self._pad.timeout(1000)
        self._pad.attrset(curses.color_pair(curses.COLOR_GREEN))
        ScrollablePad.SINGLETON = self

    def flush(self):
        curses.doupdate()

    def getkey(self, *args, **kwargs):
        return self._pad.getkey(*args, **kwargs)

    def getch(self, *args, **kwargs):
        return self._pad.getch(*args, **kwargs)

    def attrset(self, attr):
        self._pad.attrset(attr)

    def addstr(self, *args, **kwargs):
        string = None
        end_cr = True
        if len(args) + len(kwargs) >= 3:
            if len(args) > 2:
                string = args[2]
        elif len(args) > 0:
            string = args[0]
        if string is None and 'str' in kwargs['str']:
            string = kwargs['str']
        if string == '\n' or string == '\r\n':
            return
        if not string.endswith('\n'):
            string += '\n'
            end_cr = False
        str_rows = count_rows(string, self._cols)
        py, px = self._pad.getyx()
        if py + str_rows >= self._rows:
            self._pad.move(0, 0)
            for _ in range(0, str_rows):
                self._pad.deleteln()
            self._pad.move(py - str_rows, px)
        py0, px0 = self._pad.getyx()
        self._pad.addstr(*args, **kwargs)
        if not end_cr:
            self._pad.addstr('\n')
        py, px = self._pad.getyx()
        y0 = 0 if py0 < self._sheight - 1 else py0 - self._sheight + 1
        if self._y >= y0:
            self._y = 0 if py < self._sheight - 1 else py - self._sheight + 1
            self._pad.noutrefresh(self._y, self._x, self._sminy, self._sminx, self._smaxy, self._smaxx)

    def scroll_up(self, lines=1):
        lines = min(self._y, lines)
        if lines > 0:
            self._y -= lines
            self._pad.refresh(self._y, self._x, self._sminy, self._sminx, self._smaxy, self._smaxx)

    def scroll_down(self, lines=1):
        py, _ = self._pad.getyx()
        maxl = py - self._sheight - self._y + 1
        lines = min(maxl, lines)
        if lines > 0:
            self._y += lines
            self._pad.refresh(self._y, self._x, self._sminy, self._sminx, self._smaxy, self._smaxx)

    def __repr__(self):
        py, px = self._pad.getyx()
        return 'y=%d, x=%d, cy=%d, cx=%d' % (self._y, self._x, py, px)


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
        kwargs.setdefault('con_len', None)
        kwargs.setdefault('con_down', 0)
        kwargs.setdefault('start_down', 0)
        kwargs.setdefault('_start_time', time.time())
        kwargs.setdefault('_log_time', time.time())
        kwargs.setdefault('break_cont', True)
        kwargs.setdefault('filename', None)
        kwargs.setdefault('con_md5', None)
        super().__init__(**kwargs)
        self._done_percent = -1

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

    def log(self, log_level=logging.WARNING, inactive=True):
        for idx in sorted(self, key=lambda k: k if isinstance(k, int) else 999):
            if not isinstance(idx, int):
                break
            logger.log(log_level, 'task[%d]: %s %s', idx, self[idx], 'inactive' if not self[idx].active and inactive else '')

    @property
    def active_n(self):
        return self['_active_n'] if '_active_n' in self else 0

    @active_n.setter
    def active_n(self, n):
        self['_active_n'] = n


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


def load_status(status_fd, status):
    status_fd.seek(0)
    n = int.from_bytes(status_fd.read(2), 'big')
    status['con_len'] = int.from_bytes(status_fd.read(8), 'big')
    status['con_down'] = int.from_bytes(status_fd.read(8), 'big')
    logger.warning('recover from last download: %d/%d/%d', n, status['con_down'], status['con_len'])
    for idx in range(0, n):
        _start0 = int.from_bytes(status_fd.read(8), 'big')
        _start = int.from_bytes(status_fd.read(8), 'big')
        _end = int.from_bytes(status_fd.read(8), 'big')
        status[idx] = Progress([_start0, _start, _end])
    status['start_down'] = status['con_down']
    return n


def save_status(status_fd, status, idx, read_len, start0=None, end=None):
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


def init_status(status_fd, status, n):
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


def re_allocate_status(status_fd, status, n, index):
    if status_fd is None:
        return None
    for idx in sorted(status, key=lambda k: status[k].end-status[k].pos+1 if isinstance(k, int) else -1, reverse=True):
        if index == idx:
            break
        undown = status[idx].end - status[idx].pos + 1
        if undown > 0 and not status[idx].active:
            status[index], status[idx] = status[idx], status[index]
            save_status(status_fd, status, index, 0, start0=status[index].start0, end=status[index].end)
            save_status(status_fd, status, idx, 0, start0=status[idx].start0, end=status[idx].end)
            del status[idx]
            status_fd.flush()
            return idx
        if undown >= 40960 * n:
            pos = int((status[idx].end + status[idx].pos) / 2)
            status[index].pos = pos
            save_status(status_fd, status, index, 0, start0=pos, end=status[idx].end)
            save_status(status_fd, status, idx, 0, end=pos-1)
            status_fd.flush()
            return idx
        break
    return None


def extend_status(status_fd, status, last_n, new_n):
    for i in range(last_n, new_n):
        if last_n >= new_n:
            break
        for idx in sorted(status, key=lambda k: status[k].end - status[k].pos + 1 if isinstance(k, int) else -1, reverse=True)[:last_n]:
            if last_n >= new_n or not isinstance(status[idx], Progress):
                break
            undown = status[idx].end - status[idx].pos + 1
            if undown >= 10240 * last_n:
                pos = int((status[idx].end + status[idx].pos) / 2)
                status[last_n] = Progress([pos, pos, status[idx].end])
                status_fd.seek(24 * last_n + 18)
                status_fd.write(pos.to_bytes(8, 'big'))
                status_fd.write(pos.to_bytes(8, 'big'))
                status_fd.write(status[idx].end.to_bytes(8, 'big'))
                save_status(status_fd, status, idx, 0, end=pos - 1)
                last_n += 1
            else:
                break
    status_fd.seek(0)
    status_fd.write(last_n.to_bytes(2, 'big'))
    status_fd.flush()
    return last_n


def find_task(status_fd, status, index, n, _log):
    idx = re_allocate_status(status_fd, status, n, index)
    if idx is not None:
        _log(logging.INFO, 'RESTART with (%s) for #%d(%s)', status[index], idx, status[idx] if idx in status else 'transfer')
        return True
    else:
        return False


def prepare_http_headers(headers, status, index, _log):
    if status['break_cont'] and status[index].is_done():
        if index == 0:
            headers['Range'] = 'bytes=0-'
        else:
            _log(logging.INFO, 'DONE with (%d-%d)', status[index].start0, status[index].end)
            return True
    elif status['break_cont']:
        headers['Range'] = 'bytes=%d-%s' % (status[index].pos, '' if status[index].end is None else status[index].end)
    if status['break_cont']:
        _log(logging.INFO, 'downloading with Range: %s', headers['Range'])
    else:
        if 'Range' in headers:
            del headers['Range']
        status[index].pos = 0
        status['con_down'] = 0
        status['start_down'] = 0
        status['_start_time'] = time.time()
        _log(logging.INFO, 'downloading without Range')
    return False


def on_http_notok(resp, status_fd, status, index, _log):
    _log(logging.WARNING, '%d %s', resp.status, resp.reason)
    _log(logging.WARNING, '%s', resp.headers)
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
            _log(logging.WARNING, 'DONE cause server NOT support break&cont')
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


def init_download_task(futures, status_fd, status, con_len, n, status_file, loop, coro_task):
    if con_len is not None:
        if status['con_len'] is None:
            status['con_len'] = int(con_len)
        if status['break_cont']:
            n = max(1, min(int(status['con_len'] / 10240), n))
            if status_fd is None:
                status_fd = open(status_file, 'w+b')
                init_status(status_fd, status, n)
                status.log()
            for i in range(1, n):
                _f = asyncio.ensure_future(coro_task(n, i), loop=loop)
                futures.append(_f)
        else:
            status[0].end = status['con_len'] - 1
    else:
        status['break_cont'] = False
    return status_fd


async def save_to_file(resp, status_fd, status, index, fd, read_timeout, _s_time, _log, loop):
    p = fd.seek(status[index].pos)
    if p != status[index].pos:
        _log(logging.ERROR, 'fd.seek(%d) fail', status[index].pos)
        return False

    _s_p = status[index].done_percent()
    _log(logging.INFO, 'START reading %s %d/%d/%s(%d%%)', fmt_human_bytes(status[index].end - status[index].pos + 1) if status[index].end is not None else '', status[index].start0, status[index].pos,
         status[index].end, _s_p)
    status['done_per'] = -1
    _s_pos = status[index].pos
    _timeout_start = time.time()
    _log_time = 0
    while True:
        _buf_len = status[index].buf_len()
        if _buf_len <= 0:
            break
        try:
            with Timeout(1, loop=loop):
                # if _buf_len > 10240:
                #     _log(logging.DEBUG, "going to read %d", _buf_len)
                chunk = await resp.content.read(_buf_len)
            if not chunk:
                _log(5, 'chunk is False')
                break
            _read_len = len(chunk)
            fd.write(chunk)
            if status_fd is not None:
                save_status(status_fd, status, index, _read_len)
            else:
                status[index].pos += _read_len
                status[index].last_read_len = _read_len
                status['con_down'] += _read_len
            _log(5, 'read %d bytes %d/%d/%s', _read_len, status[index].start0, status[index].pos, status[index].end)
        except asyncio.TimeoutError:
            _timeout = time.time() - _timeout_start
            if _timeout >= read_timeout:
                raise asyncio.TimeoutError('read timeout %.1f > %d' % (_timeout, read_timeout))
        else:
            _timeout_start = time.time()
        _s_time, _s_pos, _s_p, _log_time = do_log(status, index, _log, _s_time, _s_pos, _s_p, _log_time)
    return True


def do_log(status, index, _log, _s_time, _s_pos, _s_p, _log_time):
    __now = time.time()
    _time_thro = __now - status['_log_time']
    if _time_thro > 0.99 and (__now - _log_time) > status.active_n*0.99:
        __p = status[index].done_percent()
        __all_p = status.done_percent()

        if IndicatorWindow.SINGLETON is not None and (status['done_per'] != __all_p or _s_p != __p):
            IndicatorWindow.SINGLETON.refresh_status()

        if (not logger.isEnabledFor(logging.WARNING) and status['done_per'] == __all_p) or (not logger.isEnabledFor(logging.INFO) and _s_p == __p):
            if IndicatorWindow.SINGLETON is not None:
                IndicatorWindow.SINGLETON.flush()
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
        _log(logging.WARNING if status['done_per'] != __all_p else logging.INFO if _s_p != __p else logging.DEBUG,
             '[ETA %s %sB/S] [%sB/S] [%s %sB/S] %d/%d/%s(%d%%/%d%%) read %d bytes in %.1f seconds',
             _all_eta, _all_speed, _all_speed2, _eta, _speed,
             status[index].start0, status[index].pos, status[index].end, __p, __all_p, _s_read_len, _s_time_thro)
        _s_p = __p
        status['done_per'] = __all_p
    elif IndicatorWindow.SINGLETON is not None:
        if status[index].done_percent_changed():
            IndicatorWindow.SINGLETON.refresh_status(index=index)
        elif status.done_percent_changed():
            IndicatorWindow.SINGLETON.refresh_status(done_percent_changed=True)
        IndicatorWindow.SINGLETON.flush()
    return _s_time, _s_pos, _s_p, _log_time


def get_filenames(url, out_file):
    if out_file is None:
        o_url = urlparse(url)
        out_file = os.path.basename(o_url.path)
    else:
        out_file = out_file.strip()
        dirs = os.path.dirname(out_file)
        if dirs and not os.path.isdir(dirs):
            os.makedirs(dirs, exist_ok=True)

    _out_file = out_file
    for i in range(0, 200):
        status_file = '%s.axel.st' % out_file
        if not os.path.isfile(out_file):
            with open(out_file, 'wb'):
                pass
            break
        elif not os.path.isfile(status_file):
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


async def await_status(done, status, futures, url, out_file, method, _out_file, _status_file, start, loop=None):
    global md5_executor

    cancelled = None
    if loop is None:
        loop = asyncio.get_event_loop()
    if len(futures) > 0:
        while True:
            try:
                _done, _pending = await asyncio.wait(futures, loop=loop)
                if len(_pending) > 0:
                    done = False
                    break
                for f in _done:
                    if f.cancelled():
                        done = False
                        if cancelled is None:
                            cancelled = CancelledError()
                        break
                    _d, _, _c = f.result()
                    if _c is not None and cancelled is None:
                        cancelled = _c
                    if not _d:
                        done = False
                        break
                break
            except CancelledError as ce:
                cancelled = ce
                continue
    if (status['con_len'] is None and not done) or (status['con_len'] is not None and status['con_len'] != status['con_down']):
        done = False
        logger.warning('====FAIL====')
        logger.warning('%s %s FAIL', method, url)
        for k in sorted(status, key=lambda sk: 'z' + str(sk) if isinstance(sk, int) else sk):
            if isinstance(k, str) and not k.startswith('_'):
                logger.warning('%-10s: %s', k, status[k])
        status.log(inactive=False)
        logger.warning('========')
    else:
        done = True
        if os.path.isfile(_status_file):
            os.remove(_status_file)
        speed = (status['con_down'] - status['start_down']) / (time.time() - start)
        logger.warning('=============SUCCESS=============')
        logger.warning('%s %s DONE used [%s], speed [%sB/S]', method, url, fmt_human_time(time.time() - start), fmt_human_bytes(speed))
        filename = status['filename']
        if filename is not None and out_file is None and filename != _out_file and not os.path.exists(filename):
            os.rename(_out_file, filename)
            logger.warning('save to %s', filename)
            _out_file = filename
        elif out_file is None or _out_file != out_file:
            logger.warning('save to %s', _out_file)
        if status['con_md5'] is not None:
            logger.warning('server md5: %s, calculating local file md5...', status['con_md5'])

            def _async_md5():
                return MD5(_out_file)

            if md5_executor is None:
                md5_executor = MyThreadPoolExecutor(max_workers=os.cpu_count(), pool_name='md5-helper')
            _md5 = await loop.run_in_executor(md5_executor, _async_md5)
            if _md5.lower() != status['con_md5'].lower():
                logger.warning('local file md5: %s', _md5)
            else:
                logger.warning('congratulations! md5 is same!')
        logger.warning('==========================')
    return done, cancelled


def load_last_status(status_fd, status, n, _out_file, url):
    last_n = load_status(status_fd, status)
    if n <= 0:
        n = last_n
    elif n > last_n:
        # 增加下载连接数
        last_n = extend_status(status_fd, status, last_n, new_n=n)
        logger.warning('extend download tasks to %d', last_n)
        n = last_n
    elif n < last_n:
        # 减少下载连接数
        for i in range(n, last_n):
            if status[i].pos > status[i].end:
                del status[i]
            else:
                status[i].active = False
        logger.warning('reduce download tasks from %d to %d', last_n, n)
    status.log()
    if SCR_COLS is not None:
        win = IndicatorWindow(status, out_file=_out_file, url=url)
        win.refresh_status()
    return n


async def aio_download(url, method='GET', out_file=None, n=0, index=0, read_timeout=58, conn_timeout=5, status=None, restart_on_done=True, retry_count=10, retry_interval=2, session=None, loop=None, **kwargs):
    if session is None:
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit_per_host=50, enable_cleanup_closed=True, force_close=True), conn_timeout=conn_timeout, loop=loop) as session:
            return await aio_download(url, method=method, out_file=out_file, n=n, index=index, read_timeout=read_timeout, conn_timeout=conn_timeout, status=status,
                                      restart_on_done=restart_on_done, retry_count=retry_count, retry_interval=retry_interval, session=session, loop=loop, **kwargs)

    if loop is None:
        loop = asyncio.get_event_loop()

    start = time.time()
    if 'headers' in kwargs:
        headers = kwargs['headers'].copy()
    else:
        headers = {}
    kwargs['headers'] = headers
    kwargs.setdefault('timeout', None)

    futures = []
    status_fd = None
    if status is None:
        status = Status()
    fd = None
    done = False

    if index == 0:
        logger.warning('%sing %s', method, url)
        _out_file, _status_file = get_filenames(url, out_file)
    else:
        _out_file, _status_file = out_file, '%s.axel.st' % out_file

    def __log(level, msg, *log_args, exc_info=False, **log_kwargs):
        if not logger.isEnabledFor(level):
            return
        if n > 10:
            logger.log(level, '#%02d ' % index + msg, *log_args, exc_info=exc_info, **log_kwargs)
        else:
            logger.log(level, '#%d ' % index + msg, *log_args, exc_info=exc_info, **log_kwargs)

    _log = __log

    cancelled_err = None
    status.active_n += 1
    try:
        fd = open(_out_file, 'r+b')
        if os.path.isfile(_status_file):
            status_fd = open(_status_file, 'r+b')
            if index == 0:
                n = load_last_status(status_fd, status, n, _out_file, url)
        else:
            status[index] = Progress([0, 0, None])
        if n <= 0:
            n = 6
        elif n > 50:
            n = 50
        io_retry = 0
        http_code = 200
        io_error = None
        while True:
            if not status[index].active:
                status[index].active = True
                _log(logging.INFO, '%s activated', status[index])

            if io_error is not None or http_code < 200 or http_code >= 300:
                io_retry += 1
                if io_retry < retry_count:
                    await asyncio.sleep(retry_interval * io_retry, loop=loop)
                else:
                    break

            def __log(level, msg, *log_args, exc_info=False, **log_kwargs):
                if not logger.isEnabledFor(level):
                    return
                if n > 10:
                    logger.log(level, '#%02d.%d ' % (index, io_retry) + msg, *log_args, exc_info=exc_info, **log_kwargs)
                else:
                    logger.log(level, '#%d.%d ' % (index, io_retry) + msg, *log_args, exc_info=exc_info, **log_kwargs)

            _log = __log

            _done = prepare_http_headers(headers, status, index, _log)
            if _done:
                if restart_on_done and find_task(status_fd, status, index, n, _log):
                    continue
                else:
                    done = True
                    break

            resp = None
            try:
                _s_time = time.time()

                with Timeout(conn_timeout+1, loop=loop):
                    resp = await session.request(method=method, url=url, **kwargs)

                http_code = resp.status
                if http_code < 200 or http_code >= 300:
                    cont, done = on_http_notok(resp, status_fd, status, index, _log)
                    if cont:
                        if not status['break_cont']:
                            n = 1
                        continue
                    else:
                        break

                con_len = status['con_len']
                if con_len is None:
                    con_len = resp.headers.get(hdrs.CONTENT_LENGTH)
                    logger.warning('Content-Length: %s', con_len)
                if status['filename'] is None:
                    con_disp = resp.headers.get(hdrs.CONTENT_DISPOSITION)
                    if con_disp:
                        _fs = con_disp.split('filename=', 1)
                        if len(_fs) == 2:
                            status['filename'] = _fs[1].strip("'").strip('"')
                            logger.warning('Filename: %s', status['filename'])
                if status['con_md5'] is None:
                    status['con_md5'] = resp.headers.get(hdrs.CONTENT_MD5)
                    if status['con_md5']:
                        logger.warning('Content-MD5: %s', status['con_md5'])

                if index == 0 and len(futures) == 0:

                    async def _aio_download_task(_n, _index):
                        await asyncio.sleep(_index)
                        return await aio_download(url, method=method, out_file=_out_file, n=_n, index=_index, read_timeout=read_timeout, conn_timeout=conn_timeout, status=status,
                                                  restart_on_done=restart_on_done, retry_count=retry_count, retry_interval=retry_interval, session=session, loop=loop, **kwargs)

                    _status_fd = status_fd
                    status_fd = init_download_task(futures, status_fd, status, con_len, n, _status_file, loop, _aio_download_task)
                    n = len(futures) + 1

                    if _status_fd is None and status_fd is not None and SCR_COLS is not None:
                        win = IndicatorWindow(status, out_file=_out_file, url=url)
                        win.refresh_status()

                await save_to_file(resp, status_fd, status, index, fd, read_timeout, _s_time, _log, loop)

                if status['con_len'] is None or status[index].is_done():
                    _log(logging.INFO, 'DONE with (%d/%s)', status[index].start0, status[index].end)
                    if restart_on_done and find_task(status_fd, status, index, n, _log):
                        continue
                    else:
                        done = True
                        break
                else:
                    raise IOError('save_to_file(#%d) interrupted' % index)
            except (IOError, errors.ClientError, asyncio.TimeoutError) as ioe:
                _log(logging.WARNING, 'IOError: %s(%s)', ioe.__class__.__name__, ioe)
                io_error = ioe
            else:
                io_error = None
            finally:
                if resp is not None:
                    resp.close()
    except CancelledError as ex:
        cancelled_err = ex
    except BaseException as ex:
        _log(logging.ERROR, 'Error: %s(%s)', ex.__class__.__name__, ex, exc_info=True)
    finally:
        if fd is not None:
            fd.close()
        if status_fd is not None:
            status_fd.seek(10)
            status_fd.write(status['con_down'].to_bytes(8, 'big'))
            status_fd.seek(24 * index + 18)
            status_fd.close()
        status.active_n -= 1
    if not done:
        status[index].active = False
        _log(logging.INFO, 'NOT DONE: %s', status[index] if index in status else '')
    else:
        del status[index]
    if index == 0:
        done, cancelled = await await_status(done, status, futures, url, out_file, method, _out_file, _status_file, start, loop)
        if cancelled is not None and cancelled_err is None:
            cancelled_err = cancelled
    return done, _out_file, cancelled_err


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


async def aio_request_with_retry(url, loop, header=False, read_timeout=58, conn_timeout=5, retry_count=10, retry_interval=2, **kwargs):
    io_retry = 0
    method = 'HEAD' if header else 'GET'
    while True:
        logger.log(6, '%sing.#%d %s', method, io_retry, url)

        try:
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit_per_host=50, enable_cleanup_closed=True, force_close=True), conn_timeout=conn_timeout, raise_for_status=True,
                                             loop=loop) as session:
                with Timeout(conn_timeout + 1, loop=loop):
                    resp = await session.request(method=method, url=url, **kwargs)
                    if header:
                        logger.log(6, '%s headers: %s', url[:50], resp.headers)
                        return resp.headers

                with Timeout(read_timeout, loop=loop):
                    j = await resp.json(content_type='')

                return j
        except (asyncio.TimeoutError, aiohttp.ClientResponseError) as err:
            logger.warning('%sing.#%d %s\n\terror: %s', method, io_retry, url, err)
            if isinstance(err, aiohttp.ClientResponseError):
                if 400 <= err.code < 500:
                    break
            io_retry += 1
            if io_retry < retry_count:
                await asyncio.sleep(retry_interval * io_retry, loop=loop)
            else:
                break
    return None


def _get_baidu_url_params(url: str, conf_filename='pyclda.baidu.url.params.json') -> (str, dict):
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
    :param conf_filename:
    :return:
    """
    if os.sep not in conf_filename:
        filename = os.path.expanduser('~/%s' % conf_filename)
    else:
        filename = conf_filename
    try:
        with open(filename, 'r') as f:
            j = json.load(f)
        if url in j:
            conf = j[url]
            if 'url' in conf:
                url = conf['url']
            if 'params' in conf:
                return url, conf['params']
    except Exception as e:
        logger.warning('load %s fail: %s', filename, e)
    return url, {}


async def path_list(path, loop, **kwargs):
    buffer_file = '.%s.file_list_buffering' % path.replace('/', '__')
    expired_time = 24 * 3600
    if os.path.isfile(buffer_file) and (time.time() - os.stat(buffer_file).st_mtime) < expired_time:
        with open(buffer_file, 'r') as f:
            buf_j = json.load(f)
        file_list = buf_j['file_list']
        failed_list_dirs = buf_j['failed_list_dirs']
        if len(failed_list_dirs) == 0:
            logger.debug('reuse buffering list')
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
    path_list_url = 'https://pan.baidu.com/api/list'

    path_list_url, params_from_confile = _get_baidu_url_params(path_list_url)
    for idx, k in enumerate(path_list_url_params):
        v = params_from_confile.get(k, path_list_url_params[k])
        path_list_url += ('?' if idx == 0 else '&') + '%s=%s' % (k, quote(v))

    concurrent = {'a': 1}
    _failed_list_dirs = []

    async def _request_file_list(_dir, search_depth=None, depth=1):
        _url = '%s&dir=%s' % (path_list_url, quote(_dir))

        logger.debug('listing dir(%d): %s', concurrent['a'], _dir)
        j = await aio_request_with_retry(_url, loop, **kwargs)
        concurrent['a'] -= 1

        if j is None or 'list' not in j:
            _failed_list_dirs.append(_dir)
            logger.warning('list dir %s FAILED: %s', _dir, pformat(j))
        elif len(j['list']) > 0:
            j['list'].sort(key=lambda _p: _p['isdir'], reverse=True)
            for _path in j['list']:
                logger.debug('%s%s%s', '\t'*depth, _path['path'], '/' if _path['isdir'] else '')
            while len(j['list']) > 0:
                futures = []
                while len(j['list']) > 0:
                    _path = j['list'].pop(0)
                    logger.log(4, 'list: %s', pformat(_path))
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
                    await asyncio.gather(*futures, loop=loop)
        logger.debug('list dir(%d) DONE: %s', concurrent['a'], _dir)

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


async def path_file_location(path, loop, **kwargs):
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
    file_loc_url = 'https://d.pcs.baidu.com/rest/2.0/pcs/file'

    file_loc_url, params_from_confile = _get_baidu_url_params(file_loc_url)
    for idx, k in enumerate(file_loc_url_params):
        v = params_from_confile.get(k, file_loc_url_params[k])
        file_loc_url += ('?' if idx == 0 else '&') + '%s=%s' % (k, quote(v))

    _url = '%s&path=%s' % (file_loc_url, quote(path))

    logger.debug("%s going to request location", path)
    j = await aio_request_with_retry(_url, loop, **kwargs)
    return j['urls'] if j is not None and 'urls' in j else None


def _print_downloader_status(downloader_status):
    for idx in downloader_status:
        status = downloader_status[idx]
        if isinstance(status, Status):
            logger.warning('downloader.#%d downloading [ETA %s %sB/S %d%%] %s', idx, _fmt_human_time(status.eta()), _fmt_human_bytes(status.down_speed()), status.done_percent(), status['filename'])
        else:
            logger.warning('downloader.#%d %s', idx, status)


async def aio_download_path(path, method='GET', n=0, path_concur=1, print_status=10, magic_param='method=download', loop=None, **kwargs):
    """
        aio方式从Baidu Yun下载云端的一个目录
    """
    global md5_executor

    if loop is None:
        loop = asyncio.get_event_loop()

    file_list, failed_list_dirs = await path_list(path, loop, **kwargs)
    file_count = len(file_list)

    base_dir = os.path.dirname(path if not path.endswith('/') else path[:len(path) - 1])
    if not base_dir.startswith('/'):
        base_dir = '/' + base_dir

    if md5_executor is None:
        md5_executor = MyThreadPoolExecutor(max_workers=os.cpu_count(), pool_name='md5-helper')

    failed_location_files = []
    downloader_status = {_idx: 'INITIAL' for _idx in range(min(path_concur, file_count))}

    async def _downloader(_idx, _file_info, **_kwargs):
        downloader_status[_idx] = 'READY for download task...'

        server_file_path = _file_info['path']
        downloader_status[_idx] = 'processing %s' % server_file_path
        logger.debug("_downloader.#%d processing %s", _idx, server_file_path)
        server_size = _file_info['size']
        server_md5 = _file_info['md5']
        local_file_path = os.path.relpath(server_file_path, base_dir)

        same_size_but_diff_md5 = False
        if os.path.isfile(local_file_path):
            local_size = os.stat(local_file_path).st_size
            if local_size == server_size:
                downloader_status[_idx] = 'calculating md5 of %s' % local_file_path
                local_md5 = await loop.run_in_executor(md5_executor, MD5, local_file_path)
                if local_md5.lower() == server_md5.lower():
                    logger.info('%s (%s/%d) has DOWNLOADED to %s' % (server_file_path, server_md5, server_size, local_file_path))
                    return None
                else:
                    same_size_but_diff_md5 = True
                    logger.info('%s & %s have SAME SIZE but DIFF MD5(%d/%s:%s)' % (server_file_path, local_file_path, server_size, server_md5, local_md5))
            else:
                logger.info('%s(%d) maybe partial downloaded at %s(%d)' % (server_file_path, server_size, local_file_path, local_size))

        downloader_status[_idx] = 'requesting location of %s' % server_file_path
        urls = await path_file_location(server_file_path, loop, **_kwargs)
        if urls is None or len(urls) == 0 or 'url' not in urls[0] or not urls[0]['url']:
            failed_location_files.append(server_file_path)
            logger.warning('%s location request FAILED: %s', server_file_path, pformat(urls))
            return None

        logger.debug('%s locations: %s', server_file_path, pformat(urls))
        _cancelled = None
        retry_count = _kwargs.get('retry_count', 10)
        for url_idx, j_url in enumerate(urls):
            if 'url' not in j_url:
                continue
            url = j_url['url']
            if magic_param not in url:
                url += '&%s' % magic_param.lower()
            _kwargs['retry_count'] = max(0, retry_count - url_idx - 1)
            if same_size_but_diff_md5:
                downloader_status[_idx] = 're-requesting server-side md5 of %s' % server_file_path
                headers = await aio_request_with_retry(url, loop, header=True, **_kwargs)
                server_md5 = headers.get(hdrs.CONTENT_MD5)
                if server_md5 and local_md5.lower() == server_md5.lower():
                    logger.warning('%s & %s have SAME SIZE AND MD5(%d/%s)' % (server_file_path, local_file_path, server_size, server_md5))
                    return None
                else:
                    logger.warning('%s & %s have SAME SIZE but DIFF MD5(%d/%s:%s) RE-Download' % (server_file_path, local_file_path, server_size, server_md5, local_md5))

            logger.debug('_downloader.#%d Downloading %s ...', _idx, server_file_path)
            _status = Status()
            downloader_status[_idx] = _status
            _done, _out_file, _cancelled = await aio_download(url, method=method, out_file=local_file_path, n=n, status=_status, loop=loop, **_kwargs)
            if not _done and _cancelled is None:
                logger.warning('downloader.#%d %s download FAILED, maybe let\'s try next url', _idx, server_file_path)
                continue
            if _done:
                logger.warning('downloader.#%d %s download SUCCESS, saved to %s', _idx, server_file_path, _out_file)
            else:
                logger.warning('downloader.#%d %s download CANCELLED, check the log for detail', _idx, server_file_path)
            break

        return _cancelled

    async def _downloader_wrap(_idx, _file_path):
        try:
            _cancelled = await _downloader(_idx, _file_path, **kwargs)
        except CancelledError as _ce:
            _cancelled = _ce
        _stat = 'DONE' if _cancelled is None else 'CANCELLED'
        downloader_status[_idx] = _stat
        return _cancelled

    d_futures = []

    cancelled = None
    time_out = time.time() + print_status
    while True:
        try:
            for idx in downloader_status:
                if not cancelled and len(file_list) > 0 and downloader_status[idx] in ('INITIAL', 'DONE'):
                    d_futures.append(asyncio.ensure_future(_downloader_wrap(idx, file_list.pop(0))))

            if len(d_futures) == 0:
                break

            results, pending = await asyncio.wait(d_futures, timeout=max(0, time_out - time.time()), return_when=asyncio.FIRST_COMPLETED, loop=loop)
            if len(results) > 0:
                for result in results:
                    _cncl = result.result()
                    if _cncl is not None:
                        cancelled = _cncl
                        break
                d_futures.clear()
                d_futures.extend(pending)
            else:
                time_out = time.time() + print_status
                raise asyncio.TimeoutError()
        except asyncio.TimeoutError:
            logger.warning('=====DOWNLOADER STATUS=====')
            _print_downloader_status(downloader_status)
            if len(file_list) > 0:
                _tmp = ''
                for i in range(min(10, len(file_list))):
                    _tmp += '\n\t%s(%s)' % (file_list[i]['path'], _fmt_human_bytes(file_list[i]['size']))
                logger.info('files waiting process(Total: %d): %s', len(file_list), _tmp)
        except CancelledError as ce:
            cancelled = ce
            continue

    logger.warning('=====downloader status=====')
    _print_downloader_status(downloader_status)

    if len(failed_list_dirs) > 0:
        logger.info('FAILED list dirs:\n\t%s', failed_list_dirs)
    if len(failed_location_files) > 0:
        logger.info('FAILED location files:\n\t%s', failed_location_files)

    return None, None, cancelled


def main_entry(*headers, urls: list, baidu=False, user_agent=None, http_proxy=None, n=0, conc=False, loop=None, **kwargs):
    dict_headers = {}
    if baidu:
        dict_headers['X-Download-From'] = 'baiduyun'
        if user_agent is None:
            dict_headers['User-Agent'] = 'netdisk;2.1.0;pc;pc-mac;%s;macbaiduyunguanjia' % get_mac_ver()
        _, _headers = _get_baidu_url_params('headers')
        dict_headers.update(_headers)
    elif user_agent is None:
        dict_headers['User-Agent'] = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.41 Safari/537.36'
    else:
        dict_headers['User-Agent'] = user_agent.strip()
    for h in headers:
        if ':' in h:
            k, w = h.split(':', 1)
            dict_headers[k.strip()] = w.strip()
    if 'Host' in dict_headers:
        del dict_headers['Host']
    if http_proxy:
        http_proxy = http_proxy.strip()

    if loop is None:
        loop = asyncio.get_event_loop()

    async def _d(url, post_data=None, aio_download_func=None, **kw_args):
        if aio_download_func is None:
            aio_download_func = aio_download
        data = None
        if post_data is not None:
            data = formdata.FormData()
            for _data in post_data.split('&'):
                _kv = _data.split('=', 1)
                if len(_kv) == 2:
                    data.add_field(_kv[0], _kv[1])
        done, _out_file, cancelled_err = await aio_download_func(
            url,
            n=min(50, n),
            loop=loop,
            headers=dict_headers,
            data=data,
            proxy=http_proxy,
            skip_auto_headers=('Accept-Encoding', 'Accept'),
            **kw_args
        )
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
                kwargs['aio_download_func'] = aio_download_path
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


def curses_entry(stdscr, log_cache, verbose, *headers, **kwargs):
    global SCR_ROWS
    global SCR_COLS

    if curses.has_colors():
        for c in range(1, 8):
            curses.init_pair(c, c, 0)

    SCR_ROWS, SCR_COLS = stdscr.getmaxyx()

    win_rows = math.ceil(1000 / (SCR_COLS - 2)) + 2
    if win_rows > 12:
        win_rows = 12
    pad = ScrollablePad(10000, SCR_COLS, win_rows, 0, SCR_ROWS - 1, SCR_COLS - 1)
    IndicatorWindow.ROWS = win_rows

    def shift_log_cache(levelno):
        i = 0
        while len(log_cache) > SCR_ROWS * 10 and i < len(log_cache):
            lvl, _ = log_cache[i]
            if lvl <= levelno:
                log_cache.pop(i)
            else:
                i += 1

    class PadHandler(logging.Handler):

        def emit(self, record):
            msg = self.format(record)
            pad.addstr(msg)
            pad.flush()
            if record.levelno >= logging.NOTSET:
                log_cache.append((record.levelno, msg))
            shift_log_cache(logging.DEBUG)
            shift_log_cache(logging.INFO)

    logging.basicConfig(format='%(asctime)s - %(message)s',
                        level=logging.NOTSET if verbose > 2 else logging.DEBUG if verbose > 1 else logging.INFO if verbose > 0 else logging.WARNING,
                        handlers=[PadHandler()])

    _pad_queue = asyncio.Queue()

    _quiting = False

    async def _pad_scroll():
        try:
            while not _quiting:
                ch = await _pad_queue.get()
                if ch == curses.KEY_UP:
                    pad.scroll_up()
                elif ch == curses.KEY_DOWN:
                    pad.scroll_down()
        except:
            pass
        finally:
            logger.log(4, '_pad_scroll quit')

    _pad_task = asyncio.ensure_future(_pad_scroll(), loop=kwargs['loop'] if 'loop' in kwargs else None)

    def _pad_key_handle(final_call=False):
        while not _quiting or final_call:
            ch = pad.getch()
            if ch == curses.KEY_UP:
                if final_call:
                    pad.scroll_up()
                else:
                    _pad_queue.put_nowait(ch)
            elif ch == curses.KEY_DOWN:
                if final_call:
                    pad.scroll_down()
                else:
                    _pad_queue.put_nowait(ch)
            elif ch in (ord('Q'), ord('q')) and _quiting:
                break

    _executor = MyThreadPoolExecutor(max_workers=1, pool_name='pad_key_handle')
    _executor.submit(_pad_key_handle)

    try:
        main_entry(*headers, **kwargs)
    finally:
        _quiting = True
        _pad_task.cancel()
        _executor.shutdown()
        pad.addstr('Press "Q" to Quit.')
        pad.flush()
        try:
            _pad_key_handle(final_call=True)
        except:
            pass


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


def main():
    print('pyclda version: %s' % __version__)
    _kwargs, _headers, _verbose, _use_curses = args_parse()
    _loop = uvloop.new_event_loop()
    asyncio.set_event_loop(_loop)
    _kwargs['loop'] = _loop

    if _use_curses:
        logs = []
        try:
            curses.wrapper(curses_entry, logs, _verbose, *_headers, **_kwargs)
            return
        except curses.error:
            pass
        finally:
            for l in logs:
                print(l[1])

    logging.basicConfig(format='%(asctime)s - %(message)s',
                        level=(9 - _verbose) if _verbose > 2 else logging.DEBUG if _verbose > 1 else logging.INFO if _verbose > 0 else logging.WARNING,
                        stream=sys.stdout)
    main_entry(*_headers, **_kwargs)


if __name__ == '__main__':
    main()
