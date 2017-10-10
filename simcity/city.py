import json
import platform
import socket

from simcity import *
from simcity.factories import Factories
from simcity.mayor import Mayor
from simcity.shop import Shop
from simcity.warehouse import Warehouse
from tsproxy.listener import Listener


class SimCity(Listener, dict):

    def __init__(self, json_materials, dump_func, loop=None, **kwargs):
        dict.__init__(self, **kwargs)
        materials_init(json_materials, self.one_minute)
        self._dump_func = dump_func
        self.loop = loop if loop is not None else asyncio.get_event_loop()

        self._city_main_queue = TimeoutQueue(loop=self.loop)
        self.closed = False
        self.setdefault('_nfc_on', True)
        self.setdefault('_double_confirm', 2)
        self.setdefault('_city_zero_timing', self._zero_timing())
        self.setdefault('city_nick_name', 'SC')
        self.setdefault('warehouse_capacity', 100)
        self.setdefault('auto_into_warehouse', False)
        self.setdefault('_special_products', 0)
        self.__nearest_producted = None
        self._nearest_producted_timing = -1
        self._city_idle = True
        self._city_idle_timing = -1
        self._last_city_timing = self['_city_timing'] if '_city_timing' in self else None

        self._mayor = Mayor(self, loop=self.loop)

        if '_factories' in self:
            if 'factories' in self:
                self['_factories'].update(**self['factories'])
            self['_factories'] = Factories(self, **self['_factories'])
        else:
            if 'factories' in self:
                self['_factories'] = Factories(self, **self['factories'])
            else:
                self['_factories'] = Factories(self, slot=10)

        if '_shops' in self:
            for i in range(len(self.shops)):
                d_s = self.shops[i]
                if 'shops' in self and d_s['cn_name'] in self['shops']:
                    d_s.update(**self['shops'][d_s['cn_name']])
                s = Shop(self, **d_s)
                self.shops[i] = s
        else:
            self['_shops'] = []
        for shop_name in MaterialDict.shops_name():
            if self.get_shop(shop_name) is None:
                if 'shops' in self and shop_name in self['shops']:
                    self.shops.append(Shop(self, cn_name=shop_name, **self['shops'][shop_name]))
                else:
                    self.shops.append(Shop(self, cn_name=shop_name))
        self.shops.sort(key=lambda _s: _s.seq)

        def _to_product(material):
            return self.compose_product_chain(0, [material.cn_name], warehouse_will_used=None, prod_type=Product.PT_SELL, expect_done_time=0, initial=True)

        MaterialDict.init_all_product_time_consuming(_to_product)
        Product.PID = 0

        if '_producting_list' in self:
            for i in range(len(self._producting_batch)):
                d_p = self._producting_batch[i]
                p = Product(city=self, **d_p)
                self._producting_batch[i] = p
        else:
            self['_producting_list'] = []

        warehouse = Warehouse(self)
        if '_warehouse' in self:
            for p in self['_warehouse']:
                if isinstance(p, dict):
                    if not MaterialDict.has(p['cn_name']):
                        raise Exception("仓库中有未定义的物料： %s" % p['cn_name'])
                    if 'p_pid' not in p:
                        raise Exception("仓库中的物料 %s 没有pid" % p)
                    prod = self.get_product(p['p_pid'], include_warehouse=False)
                    if prod is None:
                        prod = Product(material=MaterialDict.get(p['cn_name']), city=self, **p)
                    warehouse.append(prod)
                else:
                    if not MaterialDict.has(p):
                        raise Exception("仓库中有未定义的物料： %s" % p)
                    warehouse.append(p)
        self['_warehouse'] = warehouse

        Listener.__init__(self, ('0.0.0.0', self._listen_port), 'simcity', loop=loop)
        self.load_acl(self)

    def do_list_acl(self, out):
        for ipn, mask in self._acl:
            out.write('%s, 0x%x' % (socket.inet_ntoa(ipn.to_bytes(length=4, byteorder='big')), mask))

    def do_acl_add(self, out, ips):
        for ipv4 in ips:
            if self.acl_op(ipv4):
                self.cprint('ACL add %s ok.' % ipv4)
            else:
                self.cprint('ACL add %s FAIL.' % ipv4)
        self.do_list_acl(out)

    def do_acl_del(self, out, ips):
        for ipv4 in ips:
            if self.acl_op(ipv4, delete=True):
                self.cprint('ACL delete %s ok.' % ipv4)
            else:
                self.cprint('ACL delete %s FAIL.' % ipv4)
        self.do_list_acl(out)

    @property
    def _nearest_producted(self):
        if self.__nearest_producted is not None and self._nearest_producted_timing == self.city_timing:
            return self.__nearest_producted
        ps = self.get_producting_list()
        self.__nearest_producted = None if len(ps) == 0 else ps[0]
        self._nearest_producted_timing = self.city_timing
        return self.__nearest_producted

    @property
    def _waiting(self):
        _waiting_list = []
        for p in self._producting_batch:
            self.batch_to_list(p.children, _waiting_list)
        return _waiting_list

    @property
    def _listen_port(self):
        return self['listen_port']

    @property
    def warehouse_capacity(self):
        return self['warehouse_capacity']

    @warehouse_capacity.setter
    def warehouse_capacity(self, c):
        self['warehouse_capacity'] = c

    @property
    def products_capacity(self):
        return self.warehouse_capacity - self['_special_products']

    @products_capacity.setter
    def products_capacity(self, c):
        self['_special_products'] += (self.products_capacity - c)
        if self['_special_products'] < 0:
            self['_special_products'] = 0

    @property
    def city_nick_name(self):
        return self['city_nick_name']

    @property
    def city_mayor(self) -> Mayor:
        return self._mayor

    @property
    def nfc_on(self):
        if platform.system() == 'Darwin':
            return self['_nfc_on']
        else:
            return False

    @nfc_on.setter
    def nfc_on(self, u):
        self['_nfc_on'] = u

    @property
    def double_confirm(self):
        return self['_double_confirm']

    @double_confirm.setter
    def double_confirm(self, u):
        self['_double_confirm'] = u

    @property
    def auto_into_warehouse(self):
        return self['auto_into_warehouse']

    @auto_into_warehouse.setter
    def auto_into_warehouse(self, u):
        self['auto_into_warehouse'] = u

    @property
    def one_minute(self):
        if 'one_minute' not in self:
            self['one_minute'] = 60
        return self['one_minute']

    @property
    def city_abs_timing(self):
        return round(time.time() - self['_city_zero_timing'])

    @property
    def city_timing(self):
        if '_city_forward_timing' in self:
            self['_city_timing'] = self['_city_forward_timing']
        else:
            self['_city_timing'] = self.city_abs_timing
        return self['_city_timing']

    @property
    def product_batch_no(self):
        if '_product_batch_no' not in self:
            self['_product_batch_no'] = 0
        return self['_product_batch_no']

    @property
    def _producting_batch(self) -> list:
        return self['_producting_list']

    @property
    def fast_mode(self):
        '''
            # 0 - 一批物料最快完成
            # 1 - 单个物料最快完成
        '''
        return 0 if 'fast_mode' in self and self['fast_mode'] == 'package' else 1

    @property
    def factories(self) -> Factories:
        return self['_factories']

    @property
    def shops(self) -> list:
        return self['_shops']

    @property
    def warehouse(self) -> Warehouse:
        return self['_warehouse']

    @property
    def is_city_idle(self):
        if (time.time() - self._city_idle_timing) < 0.1 and not self.warehouse.changed:
            return self._city_idle
        _has_job = not self.factories.is_idle
        for shop in self.shops:
            if not shop.is_idle:
                _has_job = True
        self._city_idle = not _has_job
        self._city_idle_timing = time.time()
        return self._city_idle

    def producting_capacity(self, to_start=None):
        # TODO capacity计数可以优化
        # 生产中的物料最小仓库占用
        ps = self.get_producting_list(sort_by_done=False, include_pending=True, include_done=True)
        n = 1  # 留出1个富余
        children_max1 = 0
        children_max2 = 0
        max_product = None
        if to_start is not None:
            ps.extend(to_start)
        for p in ps:
            children_count = 0
            for child in p.parent.children:
                if not child.in_warehouse:
                    children_count += 1
            if children_max1 < children_count:
                children_max1 = children_count
                max_product = p
            if p.depth == 0:
                if children_max2 < len(p.root.needs):
                    children_max2 = len(p.root.needs)
            else:
                if children_max2 < len(p.parent.children):
                    children_max2 = len(p.parent.children)
        n += children_max1
        logging.debug('producting_capacity=%d(%d[%s]/%d)/%d', n, children_max1, max_product, children_max2, len(ps))
        return n

    @staticmethod
    def _zero_timing():
        n = datetime.now()
        z = datetime(n.year, n.month, n.day)
        return z.timestamp()

    def dump(self, file=None):
        self.dump_acl(self)
        return self._dump_func(self, file=file)

    def get_batch(self, batch_id) -> Product:
        for p in self._producting_batch:
            if p.batch_id == batch_id:
                return p
        return None

    def set_batch_consumed(self, batch: Product):
        batch.consumed = True
        self.wakeup()

    def shop_setting(self, shop, ext_slot=0, stars=-1):
        if ext_slot > 0:
            shop.extend_slot(ext_slot)
            if 'shops' in self and shop['cn_name'] in self['shops']:
                self['shops'][shop['cn_name']]['slot'] = shop.slot
        if stars >= 0:
            shop.stars = stars
            if 'shops' in self and shop['cn_name'] in self['shops']:
                self['shops'][shop['cn_name']]['stars'] = shop.stars

    def factories_setting(self, ext_slot=0):
        if ext_slot > 0:
            self.factories.extend_slot(ext_slot)
            if 'factories' in self:
                self['factories']['slot'] = self.factories.slot

    def is_batch_done(self, batch_id):
        b = self.get_batch(batch_id)
        return b.is_done() if b is not None else True

    def get_product(self, pid, include_warehouse=True, exact_pid=True) -> Product:
        for p in self._producting_batch:
            if p.pid == pid or (not exact_pid and (p.pid % 1000) == pid):
                return p
            c = p.get_child(pid)
            if c is not None:
                return c
        if not include_warehouse:
            return None
        for w in self.warehouse:
            if isinstance(w, Product) and (w.pid == pid or (not exact_pid and (w.pid % 1000) == pid)):
                return w
        return None

    def batch_to_list(self, chain, waiting_list=None):
        if waiting_list is None:
            waiting_list = []
        for c in chain:
            self.batch_to_list(c.children, waiting_list)
            if c.start_timing < -1 and not c.is_done():
                waiting_list.append(c)
        return waiting_list

    def get_producting_list(self, sort_by_done=True, include_pending=False, include_done=False):
        ps = self.factories.producting_list(include_pending=include_pending, include_done=include_done)
        for shop in self.shops:
            ps.extend(shop.producting_list(include_pending=include_pending, include_done=include_done))
        return sorted(ps, key=lambda _p: _p.time_to_done) if sort_by_done else ps

    def get_shop(self, shop_name) -> Shop:
        for shop in self.shops:
            if shop['cn_name'] == shop_name or ('en_name' in shop and shop['en_name'] == shop_name):
                return shop
        return None

    def cprint(self, *args, level=logging.INFO, out=None, stack_info=False, ignore_ui=False, print_prompt=True, **kwargs):
        if len(args) > 0:
            if out is not None:
                out.write(args[0] % args[1:], print_prompt=print_prompt, **kwargs)
            else:
                self._mayor.mprint(*args, ignore_ui=ignore_ui, print_prompt=print_prompt, **kwargs)
                kwargs.pop('end', None)
                print('%s' % fmt_city_timing(self.city_timing), args[0] % args[1:], **kwargs)
            kwargs.pop('end', None)
            kwargs.pop('flush', None)
            kwargs.pop('file', None)
            if stack_info:
                logging.exception(args[0], *args[1:], **kwargs)
            else:
                logging.log(level, ('%s ' % fmt_city_timing(self.city_timing)) + args[0], *args[1:], **kwargs)
        else:
            self._mayor.mprint(*args, ignore_ui=ignore_ui, print_prompt=print_prompt, **kwargs)
            print('%s' % fmt_city_timing(self.city_timing), **kwargs)

    def show_city_status(self, *shops, show_all=False, warehouse=False, factories=False, out=None):
        if show_all or warehouse:
            if len(self._producting_batch) > 0:
                _done_ = ''
                _needs_ = ''
                for prod in sorted(self._producting_batch, key=lambda p: p.batch_id):
                    if prod.consumed:
                        continue
                    if prod.is_done():
                        if _done_ != '':
                            _done_ += '\n%s' % (' ' * 23)
                        _done_ += '#%d: %s %s \x1b[1;31;48m[ ✔︎ ]\x1b[0m' % (prod.batch_id, prod.prod_type_icon, MaterialList.to_str(prod.needs, sort_by_count=False))
                        continue
                    if _needs_ != '':
                        _needs_ += '\n%s' % (' ' * 23)
                    _undone = prod.get_undone_list()
                    _warehouse_undone = len(prod.needs - self.warehouse)  # 如果仓库商品满足需求即显示标识✔︎
                    _needs_ += '#%d: %s %s 待完成: [\x1b[0;34;46m%s\x1b[0m]%s (%d)' % (prod.batch_id, prod.prod_type_icon, MaterialList.to_str(prod.needs, sort_by_count=False),
                                                                                    MaterialList.to_str(_undone, prefix='', suffix=''),
                                                                                    ' \x1b[1;35;48m[ ✔︎ ]\x1b[0m' if _warehouse_undone == 0 and not prod.is_for_sell else '', prod.pid)
                if _done_ != '':
                    self.cprint('    完成需求: %s', _done_, out=out)
                if _needs_ != '':
                    self.cprint('    在产需求: %s', _needs_, out=out)
            self.cprint('    [[仓库]]: %s' % self.warehouse, out=out)
            self.warehouse.status_reset()
        _idle = ''
        if self.factories.idle_slot:
            if len(_idle) > 0:
                _idle += ' '
            _idle += '%sx%d/%d' % (self.factories.cn_name, self.factories.idle_slot, self.factories.slot)
        if (show_all or factories) and not self.factories.is_idle:
            self.cprint('    %s: %s', format_cn(self.factories.cn_name, 8), self.factories.print_arrangement(), out=out)
        for _shop in self.shops:
            if _shop.is_idle:
                if len(_idle) > 0:
                    _idle += ' '
                _idle += '%sx%d' % (_shop.cn_name, _shop.idle_slot)
            elif show_all or (len(shops) > 0 and _shop in shops):
                self.cprint('    %s: %s' % (format_cn(_shop.cn_name, 8), _shop.print_arrangement()), out=out)
        show_idle = False  # len(shops) > 0 or warehouse or factories
        if (show_all or show_idle) and len(_idle) > 0:
            self.cprint('    ==空闲==: {%s}' % _idle, out=out)

    def wakeup(self, command=None):
        self._city_main_queue.put_nowait(command)

    def __call__(self, connection):
        yield from self._mayor.mayor_entry(connection)

    def display_notification(self, content, subtitle=None):
        if self.nfc_on:
            os.system('osascript -e \'display notification "%s" with title "SimCity-%s" %s\'' % (content, self.city_nick_name, 'subtitle "%s"' % subtitle if subtitle is not None else ''))

    def move_product_to_warehouse(self, product: Product):
        _fact = self.factories if product.is_factory_material else self.get_shop(product.shop_name)
        return _fact.move_to_warehouse(product)

    def manufacture_order(self, p1: Product, p2: Product):
        # shop优先(因为能降低库存)
        if not p1.is_factory_material and p2.is_factory_material:
            return -1
        if p1.is_factory_material and not p2.is_factory_material:
            return 1
        if p1.latest_product_timing > self.city_timing and p2.latest_product_timing > self.city_timing:
            # prod_type 越大越优先
            o = p2.prod_type - p1.prod_type
            if o != 0:
                return o
        # 同类型按照排产时间
        o = p1.latest_product_timing - p2.latest_product_timing
        if o != 0:
            return o
        # 优先生产最底层的原料
        o = p2.depth - p1.depth
        if o != 0:
            return o
        # 同一排产时间&depth，pid靠近的一起生产
        o = p1.pid - p2.pid
        return o

    def has_products_to_start(self) -> dict:
        products_to_start = OrderedDict()
        idle_slots = {}
        available_slots = {}
        nearest_products = {}
        warehouse_consumed = []
        capacity = self.warehouse.capacity
        for m in sorted(self._waiting, key=functools.cmp_to_key(self.manufacture_order)):
            _fact = self.factories if m.is_factory_material else self.get_shop(m.shop_name)
            idle_slots.setdefault(_fact, _fact.idle_slot)
            available_slots.setdefault(_fact, _fact.available_slot)
            nearest = nearest_products.get(_fact, None)
            _consumed = []
            to_start = products_to_start.get(_fact, None)
            if m.is_factory_material and available_slots[_fact] > 0 and idle_slots[_fact] >= 0 and ((to_start is not None and to_start.contain_brother(m)) or (self.factories.is_manufacturing_brother(m))):
                pass
            elif available_slots[_fact] <= 0 or idle_slots[_fact] < 0 \
                    or (m.is_factory_material and capacity <= 0) \
                    or (not m.is_factory_material
                        and not self.warehouse.consume(*m.raw_materials, batch_id=m.batch_id, consumed=_consumed, exact_batch=m.is_for_sell, any_batch=available_slots[_fact] == _fact.slot)):
                if nearest is None:
                    nearest_products[_fact] = m
                if m.latest_product_timing <= self.city_timing and m.prod_type > 2:
                    if available_slots[_fact] <= 0 or idle_slots[_fact] < 0:
                        self.cprint('\x1b[1;37;41m产品 [%s] 该开始但是 [%s] 生产槽位不足\x1b[0m', m, _fact['cn_name'])
                    elif m.is_factory_material and capacity <= 0:
                        self.cprint('\x1b[1;37;41m产品 [%s] 该开始生产但是仓库容量不足\x1b[0m', m)
                    elif not m.is_factory_material:
                        self.cprint('\x1b[1;37;41m产品 [%s] 该开始生产但是原料%s不足, 缺少: %s\x1b[0m', m, m.raw_materials, m.raw_materials - _consumed)
                    else:
                        self.cprint('\x1b[1;37;41m产品 [%s] 错过了生产时间\x1b[0m', m)
                continue
            if not m.is_factory_material and nearest is not None and available_slots[_fact] < _fact.slot \
                    and 0 <= nearest.latest_product_timing < self.city_timing + m.time_consuming:
                # 已经在生产，则不排产耗时长于nearest_product既定排产时间的产品??
                logging.info('优先完成最近物料[%s], 暂不排产[%s]', nearest, m)
                self.warehouse.extend(_consumed)
            else:
                if idle_slots[_fact] == 0:
                    idle_slots[_fact] -= 1
                    self.warehouse.extend(_consumed)
                    if m.latest_product_timing <= self.city_timing:
                        self.cprint('%s \x1b[5;38;48m的产品入库后再安排生产\x1b[0m [%s]', _fact.cn_name, m)
                    continue
                if not m.is_factory_material:
                    m.raw_consumed = _consumed
                    warehouse_consumed.extend(_consumed)
                if to_start is None:
                    to_start = MaterialList()
                    products_to_start[_fact] = to_start
                to_start.append(m)
                idle_slots[_fact] -= 1
                available_slots[_fact] -= 1
                m.start_timing = -2  # 标识m可以排产
                _capacity, capacity = capacity, self.products_capacity - self.warehouse.products_len - self.producting_capacity(to_start)
                logging.info('%s 欲排产[%s]%s warehouse.capacity: %d => %d, ', _fact.cn_name, m, '' if m.is_factory_material else ', 消耗%s' % m.raw_consumed, _capacity, capacity)
        if len(warehouse_consumed) > 0:
            self.warehouse.extend(warehouse_consumed)
        return products_to_start

    def _check_batch_done(self):
        batch_done = False
        for i in range(len(self._producting_batch)):
            p = self._producting_batch.pop(0)
            logging.debug('batch[%s] is %sdone and children is %sdone and %snotified and %sconsumed', repr(p),
                          '' if p.is_done() else 'not ',
                          '' if len(p.children) == 0 else 'not ',
                          '' if p.notified else 'not ',
                          '' if p.consumed else 'not ')
            if p.is_done() and not p.notified:
                batch_done = True
                self.cprint()
                self.cprint('========================================', level=logging.NOTSET)
                self.cprint('\x1b[1;35;48m恭喜！需求#%d %s 完成生产，历时 %s\x1b[0m', p.batch_id, MaterialList.to_str(p.needs, sort_by_count=False), fmt_time_delta(self.city_timing - p.arrange_timing))
                self.cprint('========================================', level=logging.NOTSET)
                self.cprint()
                self._mayor.notify_by_bel()
                self.display_notification('需求#%d %s 完成生产，历时 %s' % (p.batch_id, MaterialList.to_str(p.needs, sort_by_count=False), fmt_time_delta(self.city_timing - p.arrange_timing)), subtitle='恭喜！')
                p.notified = True
            if p.is_done() and len(p.children) == 0 and (p.consumed or p.is_for_sell):  # 非储备/待售物料的话，等待UI消费，否则自动入仓库待售
                logging.debug('batch[%s] is free', repr(p))
            else:
                self._producting_batch.append(p)
        return batch_done

    def _time_goes(self, forward=0):
        '''
            city time goes
            forward
                -1: recover mode;
                0: normal mode;
                > 0: forward
        '''
        while True:
            _wh_changed = self.warehouse.changed
            logging.debug('time consumed ... %s %s', fmt_city_timing(self.city_timing), 'warehouse.changed' if _wh_changed else '')
            self.warehouse.status_reset()

            _show_factories = self.factories.check_products_done()
            _show_shops = set()
            for shop in self.shops:
                if shop.check_products_done():
                    _show_shops.add(shop)

            if self._check_batch_done():
                self.show_city_status(show_all=True)
            else:
                logging.debug('show_city_status shops=%s warehouse=%s factories=%s', _show_shops, self.warehouse.changed or _wh_changed, _show_factories)
                self.show_city_status(*_show_shops, factories=_show_factories)  # 减少warehouse和needs_list的显示

            to_start = self.has_products_to_start()
            if len(to_start) > 0:
                logging.debug('has sth to start: %s', to_start.values())
                if forward >= 0:
                    self._mayor.put_job(to_start)
                    return

            if forward == 0 or self._nearest_producted is None or self.is_city_idle:
                if self._nearest_producted is None and not self.is_city_idle:
                    self.show_city_status(show_all=True)
                    self.cprint('\x1b[1;38;41m\x1b[5;38;41m仓库容量不足以安排生产\x1b[0m')
                return

            # do forward
            _before = self.city_timing
            if forward > 0:
                _forward_sec = self._nearest_producted.time_to_done
            else:
                # don't forward ahead the abs timing in recover mode
                _forward_sec = min(self._nearest_producted.time_to_done, self.city_abs_timing - _before)
            if _forward_sec > 0:
                self['_city_forward_timing'] = _before + _forward_sec
            elif forward < 0:
                logging.debug('only occur in recover mode')
                return
            logging.info('forward %s ', fmt_time_delta(_forward_sec))

    def reset_timing(self, out=False):
        if not self.is_city_idle or self.factories.speed_up_end_timing > self.city_timing:
            return
        for shop in self.shops:
            if shop.speed_up_end_timing > self.city_timing:
                return
        self.factories.reset_speed_up()
        for shop in self.shops:
            shop.reset_speed_up()
        self['_city_zero_timing'] = self._zero_timing()
        self.pop('_city_forward_timing', None)
        if out:
            self.cprint('SimCity restart')

    def _recover(self, last_timing=None):
        if last_timing is None:
            last_timing = self._last_city_timing
        if last_timing is not None:
            self['_city_forward_timing'] = last_timing
            self.show_city_status(show_all=True)
            if self.city_abs_timing > self['_city_forward_timing'] + 10:
                self._time_goes(-1)
                self.pop('_city_forward_timing')
                self.cprint('SimCity recover from \x1b[1;37;40m%s\x1b[0m >>> \x1b[1;37;40m%s\x1b[0m(+\x1b[1;37;40m%s\x1b[0m)',
                            fmt_city_timing(last_timing), fmt_city_timing(self.city_timing), fmt_time_delta(self.city_timing - last_timing))
        else:
            self.show_city_status(show_all=True)

    @asyncio.coroutine
    def run_city(self):
        '''
            city main coroutine, responsibility for the time goes
        '''
        self.cprint('SimCity[%s] start @%d', self.city_nick_name, self._listen_port)
        self._recover()
        forward = 0
        nearest_done_time = None
        while not self.closed:
            city_idle = self.is_city_idle
            before_forward = self.city_timing
            try:
                self._time_goes(forward)
                if forward > 0:
                    if self.is_city_idle:
                        forward = 0
                    else:
                        forward -= 1
                    forward_seconds = self.city_timing - before_forward
                    self.cprint('SimCity time forward#%d %s: \x1b[1;37;40m%s\x1b[0m >>> \x1b[1;37;40m%s\x1b[0m(+\x1b[1;37;40m%s\x1b[0m)',
                                forward, 'END' if self.is_city_idle else 'DONE', fmt_city_timing(before_forward), fmt_city_timing(self.city_timing), fmt_time_delta(forward_seconds))
                if self.is_city_idle:
                    self.reset_timing(out=not city_idle)

                nearest_done_time = self._nearest_producted.time_to_done if self._nearest_producted is not None else -1
                if nearest_done_time > 0:
                    fact_name = self.factories.cn_name if self._nearest_producted.is_factory_material else self.get_shop(self._nearest_producted.shop_name).cn_name
                    self.cprint('  %s 将在 \x1b[1;37;40m%s\x1b[0m 后完成 \x1b[3;38;48m%s\x1b[0m' %
                                (format_cn(fact_name, 8, left_align=True), fmt_time_delta(nearest_done_time), repr(self._nearest_producted)), level=logging.DEBUG)

                # 等待输入或者下一个生产完成
                before_wait = self.city_timing
                if self.is_city_idle or nearest_done_time < 0:
                    if self.is_city_idle:
                        # TODO 工厂空闲时自动安排长耗时物料的需求
                        self.cprint('城市空闲着, 通过 `nc 127.0.0.1 %d` 来输入生产需求...' % self._listen_port)
                    else:
                        self.cprint('城市有生产需求等待排产, 通过 `nc 127.0.0.1 %d` 来安排生产...' % self._listen_port)
                        logging.debug('wait until user had arrangement done')
                    needs_list = yield from self._city_main_queue.get(block=True)
                elif '_city_forward_timing' in self and self.city_abs_timing < self['_city_forward_timing']:
                    wait_seconds = self['_city_forward_timing'] - self.city_abs_timing
                    logging.debug("forward#%d, wait %d seconds util timeout or needs input", forward, wait_seconds)
                    needs_list = yield from self._city_main_queue.get(timeout=wait_seconds)
                elif nearest_done_time == 0:
                    logging.debug("some products is DONE, but let's take a look the needs_queue")
                    needs_list = yield from self._city_main_queue.get(timeout=0.1)
                else:
                    # _to = min(60 - (self.city_abs_timing % 60), max(1, nearest_done_time))
                    _to = min(nearest_done_time, 60)
                    logging.debug('wait %d seconds until timeout or needs input', _to)
                    needs_list = yield from self._city_main_queue.get(timeout=_to)

                if needs_list is None:  # None is for wakeup the city time goes
                    continue
                elif isinstance(needs_list, int):  # forward to next user input
                    forward = needs_list
                else:
                    self.cprint('未知需求类型: %s', needs_list.__class__.__name__)
            except asyncio.QueueEmpty:
                pass
            except asyncio.CancelledError:
                break
            except BaseException as ex:
                self.cprint("fetal error: %s(%s)", ex.__class__.__name__, ex, stack_info=True)
                break
            finally:
                try:
                    if '_city_forward_timing' in self:
                        if self.city_abs_timing >= self['_city_forward_timing']:
                            logging.info('SimCity time[%s] catch up[%s] finally', fmt_city_timing(self.city_abs_timing), fmt_city_timing(self['_city_forward_timing']))
                            self.pop('_city_forward_timing')
                        else:
                            logging.info('SimCity time ahead %s ...' % fmt_time_delta(self['_city_forward_timing'] - self.city_abs_timing))
                    if nearest_done_time is not None and nearest_done_time > 0:
                        _should_wakeup_timing = before_wait + nearest_done_time
                        if self.city_timing > _should_wakeup_timing + 10:
                            # wait too long to miss the nearest done, we should make up it
                            logging.info('wait too long to miss the nearest done @%s, we should make up it', fmt_city_timing(_should_wakeup_timing))
                            self._recover(last_timing=_should_wakeup_timing)
                except BaseException as ex_f:
                    self.cprint("fetal error at finally block: %s(%s)", ex_f.__class__.__name__, ex_f)

        self.dump()
        self.cprint('SimCity[%s] closed', self.city_nick_name)

    def arrange_materials_to_product(self, material_list, prod_type=Product.PT_BUILDING, expect_done_time=0):
        '''组装物料生产链并排产'''
        self['_product_batch_no'] += 1
        if self['_product_batch_no'] >= 100:
            self['_product_batch_no'] = 1

        warehouse_will_used = []  # 仓库中的现成的产品
        product_chain = self.compose_product_chain(self.product_batch_no, material_list, warehouse_will_used, prod_type=prod_type, expect_done_time=expect_done_time)

        self.cprint('========================================', level=logging.NOTSET)
        self.cprint('\x1b[1;35;48m即将生产需求#%d: %s\x1b[0m' % (product_chain.batch_id, MaterialList.to_str(material_list, sort_by_count=False)))

        if len(warehouse_will_used) > 0:
            self.warehouse.extend(warehouse_will_used)  # 先放回仓库中（已经打上批次号，不会被其他批次使用）
            self.cprint("将要消耗库存: %s" % warehouse_will_used)

        if not product_chain.is_done() and product_chain.has_children:
            self._arrange_product_chain(product_chain.children)

            logging.debug("product_chain: %s", json.dumps(product_chain, indent=2, ensure_ascii=False, sort_keys=True))

            self.cprint("\x1b[1;35;48m最少需要时间: %s\x1b[0m" % (fmt_time_delta(product_chain.all_product_time_consuming)))
            # self.cprint("生产链顶端: %s" % product_chain.children)
            self.cprint("\x1b[1;35;48m生产计划: %s\x1b[0m", '' if product_chain.put_off <= 0 else '(推迟%s开始生产)' % fmt_time_delta(product_chain.put_off))
            self.factories.print_arrangement(product_chain.batch_id)
            for shop in self.shops:
                shop.print_arrangement(product_chain.batch_id)

        self.cprint('========================================', level=logging.NOTSET)
        self._producting_batch.append(product_chain)

        return product_chain

    def compose_product_chain(self, batch_id, material_list, warehouse_will_used, prod_type=Product.PT_BUILDING, expect_done_time=0, initial=False):
        product_chain = Product(batch_id=batch_id, needs=material_list, depth=-1, arrange_timing=self.city_timing, prod_type=prod_type, city=self)
        self._compose_product_chain(material_list, product_chain, warehouse_will_used, expect_done_time=expect_done_time)
        self._schedule_product_chain(product_chain, initial=initial, expect_done_time=expect_done_time)
        return product_chain

    def _compose_product_chain(self, material_list, parent_chain, warehouse_will_used, depth=0, expect_done_time=0):
        '''根据物料依赖关系,生产简单的生产链'''
        for m_name in material_list:
            material = MaterialDict.get(m_name)
            _warehouse_consumed = []
            try_warehouse = material.all_product_time_consuming >= expect_done_time
            if try_warehouse and parent_chain.use_warehouse and self.warehouse.consume(material, batch_id=parent_chain.batch_id, consumed=_warehouse_consumed):
                if isinstance(_warehouse_consumed[0], Product):
                    _warehouse_consumed[0].batch_id = parent_chain.batch_id if depth > 0 else -parent_chain.batch_id
                    _warehouse_consumed[0].depth = depth
                else:
                    _warehouse_consumed[0] = Product(material=material, batch_id=parent_chain.batch_id if depth > 0 else -parent_chain.batch_id, depth=depth, city=self, in_warehouse=True)
                warehouse_will_used.append(_warehouse_consumed[0])
                continue
            chain = Product(material=material, parent=parent_chain, depth=depth, city=self)
            parent_chain.children.append(chain)
            if not material.is_factory_material:
                self._compose_product_chain(material.raw_materials, chain, warehouse_will_used, depth=depth + 1, expect_done_time=expect_done_time)

    def _schedule_order(self, c1: Product, c2: Product):
        d = c2.depth - c1.depth
        if d != 0:
            return d
        w = c1.waiting_time - c2.waiting_time
        if w != 0:
            return w
        c = c2.time_consuming - c1.time_consuming
        if c != 0:
            return c
        return c1.pid - c2.pid

    def _schedule_product_chain(self, chain, initial=False, expect_done_time=0):
        '''根据物料需要的商店,重构生产链(因为商店同时只能有一个生产位)'''
        shops_schedules = {}
        fact_schedule = self.factories.get_schedule(initial=initial)
        for child in sorted(self.batch_to_list(chain.children), key=functools.cmp_to_key(self._schedule_order)):
            if child.is_factory_material:
                _schedule = fact_schedule
            else:
                if child.shop_name in shops_schedules:
                    _schedule = shops_schedules[child.shop_name]
                else:
                    _schedule = self.get_shop(child.shop_name).get_schedule(initial=initial)
                    shops_schedules[child.shop_name] = _schedule
            _schedule.schedule_earliest(product=child)
        self._schedule_latest(chain, shops_schedules, fact_schedule, latest=expect_done_time)
        # chain.children maybe empty if warehouse have all needs
        if len(chain.children) > 0:
            latest_child = max(chain.children, key=lambda c: c.latest_product_timing + c.time_consuming)
            chain.all_product_time_consuming = latest_child.latest_product_timing + latest_child.time_consuming - self.city_timing
        # output the schedule
        fact_schedule.log(stdout=False)
        for shop_name in sorted(shops_schedules):
            shops_schedules[shop_name].log(stdout=False)
        return shops_schedules

    def _schedule_latest(self, chain: Product, shops_schedules: dict, fact_schedule: Schedule, latest=0):
        latest = max(chain.latest_product_timing, latest)
        chain.put_off = latest - chain.latest_product_timing
        for child in sorted(chain.children, key=lambda p: p.all_product_time_consuming, reverse=True):
            if child.is_factory_material:
                _schedule = fact_schedule
            else:
                _schedule = shops_schedules[child.shop_name]
            _schedule.schedule_latest(child, latest)
        for child in sorted(chain.children, key=lambda p: p.all_product_time_consuming, reverse=True):
            self._schedule_latest(child, shops_schedules, fact_schedule)

    def _reconstruction_product_chain(self, chain):
        '''根据物料需要的商店,重构生产链(因为商店同时只能有一个生产位)'''
        shops_mark = {}
        remove_children = []
        for child in sorted(chain.children, key=lambda c: c.all_product_time_consuming, reverse=True):  # 排产顺序跟输入的物料顺序无关
            if child.is_factory_material:
                continue
            if child.shop_name in shops_mark:
                child1 = shops_mark[child.shop_name]
                if child1.all_product_time_consuming < child.waiting_time:
                    shops_mark[child.shop_name] = child
                    child1.parent = child
                    remove_children.append(child1)
                else:
                    child.parent = child1
                    remove_children.append(child)
            else:
                shops_mark[child.shop_name] = child
        for rem in remove_children:
            chain.remove_child(rem)
        for child in chain.children:
            if child.is_factory_material:
                continue
            self._reconstruction_product_chain(child)

    def _arrange_product_chain(self, product_chain):
        '''生产链排产到工厂、商店'''
        for product in product_chain:
            if product.is_factory_material:
                self.factories.waiting_push(product)
            else:
                self._arrange_product_chain(product.children)
                self.get_shop(product.shop_name).waiting_push(product)


