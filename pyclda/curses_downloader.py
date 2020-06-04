import asyncio
import logging
try:
    import curses
except:
    pass
import functools
import math

from tsproxy.common import MyThreadPoolExecutor
from pyclda.aio_downloader import Status, Progress

logger = logging.getLogger(__name__)

SCR_ROWS = -1
SCR_COLS = -1


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
        self._url = self._out_file = None
        self._pwin = curses.newwin(IndicatorWindow.ROWS, SCR_COLS, y, x)
        self._pwin.border(0)
        self._pwin.noutrefresh()
        self._width = SCR_COLS - 2
        self._height = IndicatorWindow.ROWS - 2
        self._win = self._pwin.subwin(self._height+1, self._width, 1, 1)
        self._win.keypad(1)
        self._status = status
        self._full_num = self._width * self._height
        # self._con_len = status['con_len']
        self._segments = {}
        self.out_file = out_file
        self.url = url
        logger.debug('IndicatorWindow: %d x %d = %d', self._width, self._height, self._full_num)
        IndicatorWindow.SINGLETON = self

    @property
    def url(self):
        return self._url

    @url.setter
    def url(self, url):
        self._pwin.border(0)
        if len(url) > SCR_COLS * 0.7:
            self._url = url[:int(SCR_COLS * 0.7)] + '...'
        else:
            self._url = url
        self._pwin.addstr(self._height + 1, int((SCR_COLS - len(self._url)) / 2), '[ ' + self._url + ' ]')
        self._pwin.noutrefresh()

    @property
    def out_file(self):
        return self._out_file

    @out_file.setter
    def out_file(self, out_file):
        self._out_file = out_file
        self._pwin.border(0)
        p_str = "[ %s %3d%% ]" % (self.out_file, self._status.done_percent())
        self._pwin.addstr(0, int((SCR_COLS - len(p_str)) / 2), p_str)
        self._pwin.noutrefresh()

    def flush(self):
        curses.doupdate()

    def getyx(self, pos=None, yx=None):
        if yx is None:
            yx = math.trunc(self._full_num * pos / (self._status['con_len'] - 1))
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
        if end >= self._status['con_len'] - 1:
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
            if self._status['con_len'] is not None and pos < self._status['con_len']:
                self.mark_progress(yx, pos, self._status['con_len'] - 1)
        else:
            yx = self._segments[index]
            self.move(yx)
            self.mark_progress(yx, prog=status[index])
            self._win.move(self._height, 0)
            # logger.debug("only refresh status[%d]: %s %d%%", index, status[index], status[index].done_percent())
        self._win.noutrefresh()
        if status.done_percent_changed() or done_percent_changed:
            # self._pwin.border(0)
            p_str = "[ %s %3d%% ]" % (self.out_file, status.done_percent())
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


class CursesStatus(Status):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.ind_win = None

    def on_loaded(self, out_file, url):
        if self.ind_win is None:
            self.ind_win = IndicatorWindow(self, out_file=out_file, url=url)
        else:
            self.ind_win.out_file = out_file
            self.ind_win.url = url
        self.ind_win.refresh_status(done_percent_changed=True)

    def on_done_percent_changed(self, index=None):
        if self.ind_win is None:
            return
        if index is not None:
            self.ind_win.refresh_status(index=index)
        else:
            self.ind_win.refresh_status(done_percent_changed=True)
        self.ind_win.flush()

    def refresh(self):
        if self.ind_win is not None:
            self.ind_win.refresh_status()

    def flush(self):
        if self.ind_win is not None:
            self.ind_win.flush()


def curses_entry(log_cache, verbose, main_entry, *headers, **kwargs):
    try:
        curses.wrapper(_curses_entry, log_cache, verbose, main_entry, *headers, **kwargs)
        return True
    except curses.error:
        return False


def _curses_entry(stdscr, log_cache, verbose, main_entry, *headers, **kwargs):
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
        kwargs['status'] = CursesStatus()
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

