
from simcity import *


class Factories(dict):
    STYLE = 4
    FG = 38
    BG = 48
    STYLE_MAP = {}

    STARS_SPEED_UP = {
        0: 1.0,
        1: 0.9,
        2: 0.85,  # not confirm
        3: 0.8    # not confirm
    }

    def __hash__(self) -> int:
        return self['cn_name'].__hash__()

    def __init__(self, city, cn_name='工厂', slot=2, **kwargs):
        self._city = city
        self.update(kwargs)
        self['cn_name'] = cn_name
        self['slot'] = slot
        self._idle = slot
        self.__producting = {}
        if cn_name not in Factories.STYLE_MAP:
            self._fg = Factories.FG
            self._bg = Factories.BG
            if Factories.BG == 48:
                Factories.FG -= 1
                if Factories.FG < 31:
                    Factories.BG = 47
            else:
                Factories.FG += 1
            _fmt = ';'.join([str(Factories.STYLE), str(self._fg), str(self._bg)])
            Factories.STYLE_MAP[cn_name] = _fmt
        else:
            _fmt = Factories.STYLE_MAP[cn_name]
        self._cn_name = '\x1b[%sm%s\x1b[0m' % (_fmt, self['cn_name'])

        self.setdefault('_factory', [])
        if len(self._factory) < self.slot:
            for i in range(len(self._factory), self.slot):
                self._factory.append(None)

        self.setdefault('_waiting', [])

        for k in self._factory:
            if k is not None:
                self._idle -= 1

        logging.debug('%s[%d/%d] -> %s ... %s', self._cn_name, self._idle, self.slot, self._factory, self._waiting)

    def __str__(self):
        return "%s#%d" % (self.cn_name, self.slot)

    def _producting(self, pid, raise_on_nf=True):
        if pid in self.__producting:
            return self.__producting[pid]
        p = self._city.get_product(pid)
        if p is None and raise_on_nf:
            raise Exception("没有找到 %d 的生产" % pid)
        if p is not None:
            self.__producting[pid] = p
        return p

    def _factory_get(self, i: int) -> Product:
        if i >= self.slot or self._factory[i] is None or self._factory[i] < 0:
            return None
        pid = self._factory[i]
        return self._producting(pid)

    def _factory_set(self, i, p: Product):
        self.__producting[p.pid] = p
        self._factory[i], old_pid = p.pid, self._factory[i]
        if old_pid is None:
            self._idle -= 1

    def _factory_del(self, i) -> Product:
        if i >= self.slot or self._factory[i] is None:
            return None
        pid, self._factory[i] = self._factory[i], None
        p = self.__producting.pop(abs(pid), None)
        self._idle += 1
        return p if p is not None else abs(pid)

    def waiting_delete(self, product: Product):
        if self._waiting_del(product.pid):
            product.product_done()
            if product.depth == 0:
                del_need = False
                for i in range(len(product.root.needs)):
                    n = product.root.needs[i]
                    if product.cn_name == n:
                        del product.root.needs[i]
                        del_need = True
                        break
                if not del_need:
                    self._city.cprint('\x1b[1;37;41m删除需求[%s]失败\x1b[0m' % product.cn_name)
            return True
        return False

    def _waiting_del(self, pid) -> bool:
        for i in range(0, len(self._waiting)):
            if self._waiting[i] == pid:
                del self._waiting[i]
                return True
        return False

    def waiting_push(self, p: Product):
        self.__producting[p.pid] = p
        self._waiting.append(p.pid)

    def extend_slot(self, ext_slot: int):
        self['slot'] += ext_slot
        self._idle += ext_slot
        for i in range(len(self._factory), self.slot):
            self._factory.append(None)

    def adjust_time_consuming(self, time_consuming, start_timing):
        _time_consuming = int(self.stars_speed_up * time_consuming)
        if 0 <= start_timing < self.speed_up_end_timing and (self.speed_up_start_timing - start_timing) < _time_consuming:
            if start_timing < self.speed_up_start_timing:
                _normal_time = self.speed_up_start_timing - start_timing
                _speed_up_duration = (_time_consuming - _normal_time) / self.speed_up_times
                if _speed_up_duration > (self.speed_up_end_timing - self.speed_up_start_timing):
                    _speed_up_duration = self.speed_up_end_timing - self.speed_up_start_timing
                _time_consuming -= _speed_up_duration * (self.speed_up_times - 1)
            else:
                _speed_up_duration = self.speed_up_end_timing - start_timing
                if _time_consuming > (_speed_up_duration * self.speed_up_times):
                    _time_consuming -= _speed_up_duration * (self.speed_up_times - 1)
                else:
                    _time_consuming /= self.speed_up_times
        return _time_consuming

    def set_times_speed_up(self, times=2, duration=3600, start_timing=None):
        if self.speed_up_end_timing >= (start_timing if start_timing is not None else self._city.city_timing):
            return False, self.speed_up_start_timing, self.speed_up_end_timing, self.speed_up_times
        self['speed_up_start_timing'] = start_timing if start_timing is not None else self._city.city_timing
        self['speed_up_end_timing'] = self.speed_up_start_timing + duration
        self['speed_up_times'] = times
        if start_timing is None:
            self._city.cprint('%s 的加速币(x%d) 开始生效, 持续时间 %s 直至 %s', format_cn(self.cn_name, 8, left_align=True), self.speed_up_times, fmt_time(duration), fmt_time(self.speed_up_end_timing))
        return True, self.speed_up_start_timing, self.speed_up_end_timing, self.speed_up_times

    @property
    def speed_up_start_timing(self):
        return self['speed_up_start_timing'] if 'speed_up_start_timing' in self else -1

    @property
    def speed_up_end_timing(self):
        return self['speed_up_end_timing'] if 'speed_up_end_timing' in self else -1

    @property
    def speed_up_times(self):
        return abs(self['speed_up_times']) if 'speed_up_times' in self else 1

    def reset_speed_up(self):
        self.pop('speed_up_start_timing', None)
        self.pop('speed_up_end_timing', None)
        self.pop('speed_up_times', None)

    @property
    def stars(self):
        return self['stars'] if 'stars' in self else 0

    @stars.setter
    def stars(self, s):
        self['stars'] = s

    @property
    def stars_speed_up(self):
        return Factories.STARS_SPEED_UP.get(self.stars, 1.0)

    @property
    def cn_name(self):
        return self._cn_name

    @property
    def _factory(self):
        return self['_factory']

    @property
    def _waiting(self):
        return self['_waiting']

    @property
    def slot(self):
        return self['slot']

    @property
    def idle_slot(self):
        return self._idle

    @property
    def available_slot(self):
        n = 0
        for k in self._factory:
            n += 1 if k is None or k < 0 else 0
        return n

    @property
    def is_idle(self):
        n = 0
        for k in self._factory:
            n += 1 if k is not None and k > 0 else 0
        n += len(self._waiting)
        return n == 0

    def _manufacturing_count(self, batch_id=0):
        return self.__count(self._factory, batch_id)

    def _waiting_count(self, batch_id=0):
        return self.__count(self._waiting, batch_id)

    def __count(self, iterable, batch_id):
        n = 0
        for pid in iterable:
            if pid is None or pid < 0:
                continue
            p = self._producting(pid)
            if batch_id == 0 or p.batch_id == batch_id:
                n += 1
        return n

    def is_busying(self):
        return False

    def start_products(self, *products, force=False):
        _sth_started = MaterialList()
        for product in products:
            if self._idle <= 0:
                self._city.cprint('排产[%s]失败: 没有空闲生产位', product)
                break
            reason = self._product(product, force=force)
            if reason is not None:
                self._city.cprint('排产[%s]失败: %s', product, reason)
                continue
            if not self.is_busying():
                product.start_timing = self._city.city_timing
                self._city.cprint('  %s 开始生产 %s\x1b[1;38;48m%s\x1b[0m%s.%d, 预计耗时 %s', self.cn_name, '' if product.depth > 0 else '\x1b[4;38;48m',
                                  repr(product), product.consumed_info, self._city.warehouse.capacity, fmt_time(product.time_consuming))
            else:
                product.start_timing = -1
                self._city.cprint('  %s 准备生产 %s\x1b[1;38;48m%s\x1b[0m%s.%d', self.cn_name, '' if product.depth > 0 else '\x1b[4;38;48m',
                                  repr(product), product.consumed_info, self._city.warehouse.capacity)
            self._waiting_del(product.pid)
        return _sth_started

    def _product(self, material: Product, force=False):
        '''尝试排产（如果有空闲生产位的话）'''
        for idx in range(0, self.slot):
            if self._factory[idx] is None:
                self._factory_set(idx, material)
                logging.debug('  %s[%d] 等待生产 "%s"', self.cn_name, idx, material)
                return None
        return '没有找到空闲生产位'

    def _check_slot_done(self, i):
        m = self._factory_get(i)
        if m is not None and not m.is_done() and m.time_to_done <= 1:
            m.product_done()
            self._city.warehouse.append(m)
            self._factory[i] = -self._factory[i]
            self._city.cprint('  %s%s 完成生产 %s\x1b[1;38;42m%s\x1b[0m, 耗时 %s/%s', self.cn_name, ('[%02d]' % i) if m.is_factory_material else '',
                              '' if m.depth > 0 else '\x1b[4;38;48m', repr(m),
                              fmt_time(self._city.city_timing - m.start_timing),
                              fmt_time(self._city.city_timing - m.arrange_timing))
            if m.depth == 0 and self._city.auto_into_warehouse:
                self.move_to_warehouse(m, i)
            return m
        else:
            return None

    def move_to_warehouse(self, product: Product, i=-1):
        if i < 0 or product is None:
            for idx in range(0, self.slot):
                pid = self._factory[idx]
                if pid is None or pid > 0:
                    continue
                if product is not None:
                    if -pid == product.pid:
                        i = idx
                        break
                else:
                    self._move_fact_to_ware(idx, product)
        if i >= 0 and product is not None:
            self._move_fact_to_ware(i, product)
            return True
        else:
            return product is None

    def _move_fact_to_ware(self, slot_idx: int, product: Product):
        p = self._factory_del(slot_idx)
        if isinstance(p, int) and product is not None and product.pid == p:
            p = product
        if isinstance(p, Product):
            p.in_warehouse = True
        self._city.cprint('    \x1b[1;38;44m%s 进入仓库\x1b[0m.%d', repr(p), self._city.warehouse.capacity)

    def producting_list(self, include_pending=False):
        _ps = []
        for pid in self._factory:  # 外面还会再排序，此处不需要排序
            if pid is None or pid < 0:
                continue
            p = self._producting(pid)
            if p.is_done():
                continue
            if p.start_timing >= 0 or include_pending:
                _ps.append(p)
        return _ps

    def _compose_factory_arrange(self, batch_id, detail=False):
        if detail:
            return self._compose_factory_arrange_detail()
        _fact = MaterialList()
        _done = MaterialList()
        for pid in self._factory:
            if pid is None:
                continue
            p = self._producting(abs(pid))
            if batch_id == 0 or p.batch_id == batch_id:
                if pid < 0:
                    _done.append(p)
                else:
                    _fact.append(p)
        if len(_done) > 0:
            return '%s, \x1b[0;34;46m%s\x1b[0m]' % (MaterialList.to_str(_fact, suffix=''), MaterialList.to_str(_done, prefix='', suffix=''))
        else:
            return '%s' % _fact

    def _compose_factory_arrange_detail(self):
        _arrange_str = '['
        for pid in sorted(self._factory, key=lambda _pid: 0 if _pid is None else self._producting(abs(_pid)).latest_product_timing):
            if pid is None:
                continue
            if _arrange_str != '[':
                _arrange_str += '|'
            p = self._producting(abs(pid), raise_on_nf=False)
            if p is None:
                _arrange_str += '\x1b[0;34;46m%d\x1b[0m' % (abs(pid) % 1000)
            else:
                if pid < 0:
                    _arrange_str += '\x1b[0;34;46m'
                if p.depth == 0:
                    _arrange_str += '\x1b[4;38;48m'
                _arrange_str += '%s' % repr(p)
                if p.depth == 0 or pid < 0:
                    _arrange_str += '\x1b[0m'
        if self.idle_slot > 0:
            if _arrange_str != '[':
                _arrange_str += '|'
            _arrange_str += '..x%d' % self.idle_slot
        _arrange_str += ']'
        return _arrange_str

    def print_arrangement(self, batch_id=0, print_idle=False):
        '''输出工厂的生产安排'''
        if self.is_idle or (self._manufacturing_count(batch_id) + self._waiting_count(batch_id) == 0):
            if not print_idle:
                return 'zzz...'
        _arrange_str = self._compose_factory_arrange(batch_id, detail=print_idle) if batch_id == 0 or self._manufacturing_count(batch_id) > 0 else '[ ]'
        if self._waiting_count(batch_id) > 0:
            self._waiting.sort(key=lambda wm: self._producting(wm).latest_product_timing)
            _w_str = ''
            for i in range(0, len(self._waiting)):
                w = self._producting(self._waiting[i])
                if batch_id == 0 or w.batch_id == batch_id:
                    if _w_str != '':
                        _w_str += '->'
                    if w.depth == 0:
                        _w_str += '\x1b[4;38;48m'
                    if batch_id == 0 and w.start_timing == -2:
                        _w_str += '\x1b[5;38;48m%s\x1b[0m' % (w if batch_id != 0 else repr(w))
                    else:
                        _w_str += '%s' % (w if batch_id != 0 else repr(w))
                        if w.depth == 0:
                            _w_str += '\x1b[0m'
            _arrange_str += ' ...... [%s]' % _w_str
        if batch_id > 0:
            self._city.cprint('     %s 生产 %s', format_cn(self.cn_name, 8), _arrange_str)
        return _arrange_str

    def check_products_done(self):
        '''时光飞逝，让我们看看生产完成了没有'''
        if 0 <= self.speed_up_end_timing < self._city.city_timing and self['speed_up_times'] > 0:
            self._city.cprint('%s 的加速币[x%d] 已经于 %s 失效', self.cn_name, self.speed_up_times, fmt_time(self.speed_up_end_timing))
            self['speed_up_times'] = -self.speed_up_times
        sth_done = False
        for i in range(0, self.slot):
            if self._factory_get(i) is None:
                continue
            if self._check_slot_done(i) is not None:
                sth_done = True
        return sth_done
