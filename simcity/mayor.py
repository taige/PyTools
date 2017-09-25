import subprocess
import json

from simcity import *


class Mayor:

    def __init__(self, city, loop=None):
        self._city = city
        self._connection = None
        self.__waiting_ui = False
        self._ui_print_cache = []
        self._active_time = 0
        self._ui_queue = TimeoutQueue(loop=loop if loop is not None else asyncio.get_event_loop())

    @property
    def city_timing(self):
        return self._city.city_timing

    @property
    def is_online(self):
        return self._connection is not None and not self._connection.is_closed

    @property
    def _waiting_ui(self):
        return self.__waiting_ui

    @_waiting_ui.setter
    def _waiting_ui(self, b):
        self.__waiting_ui = b
        if not self.__waiting_ui:
            while len(self._ui_print_cache) > 0:
                _logs = self._ui_print_cache.pop(0)
                if self.is_online:
                    self._connection.raw_write(_logs)

    @property
    def is_idle(self):
        # 10秒没有UI认为mayor idle了
        return time.time() - self._active_time > 10

    @asyncio.coroutine
    def mayor_entry(self, connection):
        '''UI listener entry'''
        if self.is_online:
            yield from self._connection.close()
        self._connection = _Connection(self, connection)
        self._city.cprint('\x1b[1;37;48m市长来了\x1b[0m @%s', self._connection)
        try:
            self._city.show_city_status(show_all=True, out=self._connection)
            self._city.wakeup()
            while True:
                self._active_time = time.time()
                cmd_line = yield from self._connection.readline()
                if cmd_line is None or len(cmd_line) == 0:
                    break
                cmd_line = cmd_line.strip()
                if cmd_line in ('quit', 'exit'):
                    break
                self._ui_queue.put_nowait(cmd_line)
        except (asyncio.CancelledError, _ConnectionClosed):
            pass
        except BaseException as pe:
            logging.exception("fetal error: %s(%s)", pe.__class__.__name__, pe)
            try:
                connection.writer.write(('%s\n' % pe).encode())
            except BaseException as e:
                logging.exception(e)
        finally:
            if self._waiting_ui:
                yield from self._ui_queue.put(None)
            self._city.cprint('\x1b[2;37;48m市长走了\x1b[0m @%s', connection)

    def notify_by_bel(self):
        if self.is_online and self._connection.tty is not None:
            os.system('tput -Txterm bel >%s' % self._connection.tty)
        else:
            os.system('tput -Txterm bel')

    def put_job(self, job: dict):
        if not self._waiting_ui:
            self._ui_queue.put_nowait(job)

    def mprint(self, *args, ignore_ui=False, print_prompt=True, **kwargs):
        if self.is_online:
            msg = (args[0] % args[1:]) if len(args) > 0 else ''
            if not ignore_ui and self._waiting_ui:
                self._ui_print_cache.append(('%s ' % fmt_time(self.city_timing, always_show_hour=True)) + msg)
            else:
                self._connection.write(msg, print_prompt=print_prompt, **kwargs)

    @asyncio.coroutine
    def job_watcher(self):
        '''
            Mayor Job Watcher
        '''
        logging.debug('job_watcher online')
        while not self._city.closed:
            try:
                cmd = yield from self._ui_queue.get()
                if cmd is None:
                    continue
                elif isinstance(cmd, str):
                    if not self.is_online:
                        continue
                    yield from self._cmd_handle(cmd)
                    continue
                elif isinstance(cmd, dict):
                    products_to_start = cmd
                else:
                    logging.warning('unknown cmd type: %s', cmd.__class__.__name__)
                    continue
                if len(products_to_start) == 0:
                    continue
                yield from self._products_notify_and_start(products_to_start)
            except asyncio.CancelledError:
                pass
            except BaseException as ex:
                self._city.cprint("fetal error: %s(%s)", ex.__class__.__name__, ex, stack_info=True)
        logging.debug('job_watcher offline')

    @asyncio.coroutine
    def _products_notify_and_start(self, products_to_start: dict):
        _force_start = not isinstance(products_to_start, OrderedDict)
        _sth_started = []
        for _fact in products_to_start:
            _should_arrange_now = False
            _to_start = products_to_start[_fact]
            self._city.cprint()
            if _fact is self._city.factories:
                _should_arrange_now = True
                self._city.cprint('\x1b[1;38;48m\x1b[5;38;48m>>>>>\x1b[0m \x1b[1;38;48m安排\x1b[0m %s \x1b[1;38;48m生产\x1b[0m \x1b[1;38;41m%s\x1b[0m',
                                  _fact.cn_name, MaterialList.to_str(_to_start, sort_by_tm=True))
            else:
                _is_busying = _fact.is_busying()
                if not _is_busying:
                    _should_arrange_now = True
                for i in range(0, len(_to_start)):
                    _sth = _to_start[i]
                    _consume_info = '['
                    if _sth.raw_consumed is not None:
                        for c in _sth.raw_consumed:
                            if _consume_info != '[':
                                _consume_info += ', '
                            if isinstance(c, Product) and not c.in_warehouse:
                                _consume_info += '\x1b[2;38;46m%s\x1b[0m' % repr(c)
                            else:
                                _consume_info += '%s' % repr(c)
                        _consume_info += ']'
                    else:
                        _consume_info = '%s' % _sth.raw_materials
                    self._city.cprint('\x1b[1;38;48m\x1b[5;38;48m>>>>>\x1b[0m \x1b[1;38;48m安排\x1b[0m %s \x1b[%d;38;48m生产\x1b[0m %s\x1b[1;38;41m%s\x1b[0m, 将消耗 %s',
                                      _fact.cn_name, 1 if _should_arrange_now and i == 0 else 2,
                                      '' if _sth.depth > 0 else '\x1b[4;38;48m', repr(_sth), _consume_info)
            if self.is_idle and not _should_arrange_now:
                continue
            self._city.display_notification('安排 %s 生产 %s' % (_fact['cn_name'], MaterialList.to_str(_to_start, sort_by_tm=True)))
            if not self.is_online:
                self.notify_by_bel()
                continue
            if not (yield from self._wait_user_to_arrange_and_confirm()):
                continue
            if self._city.closed:
                break
            _sth_started.append(_fact.start_products(*_to_start, force=_force_start))
        if len(_sth_started) > 0 and self.is_online and not self._city.closed:
            self._city.wakeup()
            yield
            self._city.show_city_status(show_all=True)

    @asyncio.coroutine
    def _confirm_materials(self, material_list, action):
        self._waiting_ui = True
        try:
            while True:
                self._connection.write('即将%s %s, 确认(Y/N)?' % (action, MaterialList.to_str(material_list, sort_by_count=False)), end='', print_prompt=False)
                confirm = yield from self._ui_queue.get()
                if confirm is None:
                    return False
                if confirm in ('Y', 'y'):
                    return True
                elif confirm in ('N', 'n'):
                    self._connection.write('%s取消' % action)
                    return False
                else:
                    continue
        finally:
            self._waiting_ui = False

    @asyncio.coroutine
    def _wait_user_to_arrange_and_confirm(self):
        double_confirm = self._city.double_confirm
        self._waiting_ui = True
        try:
            while True:
                # clear user input
                try:
                    self._ui_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
            self._city.cprint(' 安排好生产之后，\x1b[1;38;48m\x1b[5;38;48m按[回车x%d]继续...\x1b[0m', double_confirm, end='', flush=True, level=logging.DEBUG, ignore_ui=True, print_prompt=False)
            while double_confirm > 0:
                try:
                    self.notify_by_bel()
                    _ui_timeout = max(30, self._city.city_timing - self._city.city_abs_timing)
                    confirm = yield from self._ui_queue.get(timeout=_ui_timeout)
                    # 输入N/n，暂跳过本次排产
                    if confirm is None or confirm in ('N', 'n'):
                        return False
                    double_confirm -= 1
                    if double_confirm == 0:
                        return True
                    self._city.cprint(' 安排好生产之后，\x1b[1;38;48m\x1b[5;38;48m按[回车x%d]继续...\x1b[0m', double_confirm, end='', flush=True, level=logging.DEBUG, ignore_ui=True, print_prompt=False)
                except asyncio.QueueEmpty:
                    pass
        finally:
            self._waiting_ui = False

    @asyncio.coroutine
    def _consume_batch(self, args):
        out = self._connection
        batch_id = int(args[0])
        args.pop(0)
        # TODO 强制消费批次需求(如果仓库中的商品满足的话)
        batch = self._city.get_batch(batch_id)
        if batch is None:
            out.write('没有找到批次[#%d]' % batch_id)
            return
        if not batch.is_done():
            out.write('批次[#%d]尚未完成' % batch_id)
            return
        to_consume = MaterialList(batch.needs)  # 批次消费在原始需求的基础上进行剪裁-
        if len(args) > 0:
            not_to_consume = None
            if args[0] == '-':
                args.pop(0)
                not_to_consume, not_found = MaterialList.needs2list(args)
            elif args[0] == '=':
                # normalize the products
                to_consume = MaterialList()
                not_found = []
            else:
                to_consume, not_found = MaterialList.needs2list(args)
            if len(not_found) > 0:
                out.write("在产品列表中没有找到 %s" % not_found)
                MaterialDict.show_dict(out, self._city)
                return
            if not_to_consume is not None and len(not_to_consume) > 0:
                if not (yield from self._confirm_materials(not_to_consume, '消费批次[#%d], 但留存' % batch_id)):
                    return
                for n in not_to_consume:
                    for i in range(0, len(to_consume)):
                        p = to_consume[i]
                        if n == (p.cn_name if hasattr(p, 'cn_name') else p):
                            to_consume.pop(i)
                            break
            if to_consume is not None and len(to_consume) > 0:
                if not (yield from self._confirm_materials(to_consume, '消费批次[#%d]中的' % batch_id)):
                    return
        consumed = MaterialList()
        if self._city.warehouse.consume(batch_id=batch_id, consumed=consumed):
            _not_consumed = []
            if to_consume is not None:
                _consumed = MaterialList()
                for c in to_consume:
                    for i in range(0, len(consumed)):
                        p = consumed[i]
                        if c == p.cn_name:
                            consumed.pop(i)
                            _consumed.append(p)
                            break
                _not_consumed.extend(consumed)
                if len(_not_consumed) > 0:
                    for n in _not_consumed:
                        n.batch_id = 0
                    self._city.warehouse.extend(_not_consumed)
            else:
                _consumed = consumed
            if len(_not_consumed) + len(_consumed) > 0:
                _unconsumed = None
                if to_consume is not None and len(to_consume) != len(_consumed):
                    # 对未找到的产品进行告警
                    _unconsumed = ''
                    for k in to_consume.material_kinds:
                        _n = to_consume.count(k) - _consumed.count(k)
                        if _n > 0:
                            if _unconsumed != '':
                                _unconsumed += ', '
                            _unconsumed += '%d*%s' % (_n, k)
                # 未入仓库的产品要先入库
                for c in _consumed:
                    if isinstance(c, Product) and not c.in_warehouse:
                        self._city.move_product_to_warehouse(c)
                self._city.cprint('已消费批次[#%d]中的 %s%s%s' % (batch_id, _consumed, (', 留存 %s' % _not_consumed) if len(_not_consumed) > 0 else '',
                                                           '' if _unconsumed is None else ', \x1b[1;37;41m缺少了 %s\x1b[0m' % _unconsumed))
            else:
                out.write('消费批次[#%d]为空' % batch_id)
            self._city.set_batch_consumed(batch)
        else:
            out.write('消费批次[#%d]失败, 也许不存在该批次' % batch_id)
        self._city.show_city_status(show_all=True, out=out)

    @asyncio.coroutine
    def _cmd_consume(self, args):
        out = self._connection
        if len(args) == 0:
            self._print_help(out)
            return
        try:
            # assume consume a batch of materials
            yield from self._consume_batch(args)
        except ValueError:
            # consume materials list
            material_list, not_found = MaterialList.needs2list(args)
            if len(not_found) > 0:
                out.write("在产品列表中没有找到 %s" % not_found)
                MaterialDict.show_dict(out, self._city)
                return
            elif len(material_list) == 0:
                return
            elif not (yield from self._confirm_materials(material_list, '消费')):
                return
            consumed = []
            if self._city.warehouse.consume(*material_list, batch_id=0, consumed=consumed):
                # 未入仓库的产品要先入库
                for c in consumed:
                    if isinstance(c, Product) and not c.in_warehouse:
                        self._city.move_product_to_warehouse(c)
                self._city.cprint('已消费 %s' % consumed)
                self._city.wakeup()
            else:
                _stock = ''
                _needs = ''
                for k in material_list.material_kinds:
                    _n = self._city.warehouse.products_count(k) - material_list.products_count(k)
                    if _n < 0:
                        _needs += '%s*%d ' % (k, -_n)
                    _stock += '%s*%d ' % (k, self._city.warehouse.products_count(k))
                out.write('库存不足, 缺: %s, 库存: %s' % (_needs, _stock))

    def _shop_detail(self, shop):
        out = self._connection
        _s = '%s: %s' % (format_cn('%s%s%s' % (shop.cn_name, '★' * shop.stars, (' x%d' % shop.speed_up_times) if shop.speed_up_end_timing > self._city.city_timing else ''), 13, left_align=True),
                         shop.print_arrangement(print_idle=True))
        if shop.speed_up_end_timing > self._city.city_timing:
            _s += ' \x1b[1;33;48m{加速币(x%d) 剩余时效%s}\x1b[0m' % (shop.speed_up_times, fmt_time(shop.speed_up_end_timing - self._city.city_timing))
        out.write(_s)

    def _cmd_shop(self, cmd_line):
        out = self._connection
        if len(cmd_line) < 1:
            for shop in self._city.shops:
                self._shop_detail(shop)
            return
        shop_name = cmd_line.pop(0)
        shop = self._city.get_shop(shop_name)
        if shop is None:
            out.write('shop[%s] NOT found' % shop_name)
            return
        if len(cmd_line) > 0:
            sub_cmd = cmd_line.pop(0)
            if sub_cmd == 'speed':
                if len(cmd_line) > 0:
                    times = int(cmd_line.pop(0))
                    duration = str2time(cmd_line.pop(0)) if len(cmd_line) > 0 else 3600
                else:
                    times = 2
                    duration = 3600
                rc, _start, _end, _times = shop.set_times_speed_up(times=times, duration=duration)
                if rc:
                    self._city.wakeup()
                else:
                    out.write('%s 已经在生产加速x%d, 持续至 %s' % (format_cn(shop.cn_name, 8, left_align=True), _times, fmt_time(_end)))
            elif sub_cmd == 'slot':
                ext_slot = int(cmd_line.pop(0)) if len(cmd_line) > 0 else 1
                self._city.shop_setting(shop, ext_slot=ext_slot)
                self._city.cprint('%s 增加 %d 个生产位，总共生产位: %d', format_cn(shop.cn_name, 8, left_align=True), ext_slot, shop.slot)
                self._city.wakeup()
            elif sub_cmd in ('star', 'stars'):
                star = int(cmd_line.pop(0)) if len(cmd_line) > 0 else shop.stars + 1
                self._city.shop_setting(shop, stars=star)
                self._city.cprint('%s 星级增加至 %s，生产速度提升 %d%%', format_cn(shop.cn_name, 8, left_align=True), '★' * shop.stars, round((1-shop.stars_speed_up)*100))
                self._city.wakeup()
            else:
                self._print_help(out)
        else:
            self._shop_detail(shop)

    def _cmd_prod(self, args):
        out = self._connection
        pids = args.pop(0).split(',')
        sub_cmd = args[0] if len(args) > 0 else 'show'
        if sub_cmd.startswith('{') and args[len(args)-1].endswith('}'):
            sub_cmd = 'set'
        start_dict = {}
        if sub_cmd not in ('start', 'del', 'delete', 'ware', 'show', 'set'):
            sub_cmd = args.pop(0) if len(args) > 1 else '='
            if sub_cmd not in ('+', '-', '='):
                self._print_help(out)
                return
            time_delta = str2time(args.pop(0))
        for s_pid in pids:
            pid = int(s_pid)
            prod = self._city.get_product(pid)
            if prod is None:
                out.write('没有找到编号为 %d 的产品' % pid)
                continue
            if sub_cmd == 'show':
                out.write('%s: %s' % (prod, json.dumps(prod, indent=2, ensure_ascii=False, sort_keys=True)))
            elif sub_cmd == 'set':
                p_j = json.loads(' '.join(args))
                prod.update(p_j)
                out.write('%s: %s' % (prod, json.dumps(prod, indent=2, ensure_ascii=False, sort_keys=True)))
            elif sub_cmd == 'start':
                if prod.start_timing >= 0:
                    out.write('产品 %s 已经开始生产' % prod)
                    continue
                _fact = self._city.factories if prod.is_factory_material else self._city.get_shop(prod.shop_name)
                if _fact in start_dict:
                    _start_list = start_dict[_fact]
                else:
                    _start_list = []
                    start_dict[_fact] = _start_list
                _start_list.append(prod)
            elif sub_cmd.startswith('del'):
                if prod.start_timing >= -1:
                    out.write('产品 %s 已经开始生产, 没法删除' % prod)
                    continue
                to_del = []
                if sub_cmd == 'delete':
                    self._city.collect_waiting(prod.children, to_del)
                to_del.append(prod)
                for _del in sorted(to_del, key=lambda d: d.latest_product_timing):
                    _fact = self._city.factories if _del.is_factory_material else self._city.get_shop(_del.shop_name)
                    if _fact.waiting_delete(_del):
                        self._city.cprint('产品 %s 已从生产队列中删除', _del)
                    else:
                        out.write('产品 %s 删除失败, 也许没在 %s 的生产队列中' % (_del, _fact.cn_name))
            elif sub_cmd == 'ware':
                if not prod.is_done():
                    out.write('产品 %s 尚未完成生产' % prod)
                    continue
                if not self._city.move_product_to_warehouse(prod):
                    self._city.cprint('\x1b[1;37;41m移动 %s 至仓库失败\x1b[0m', repr(prod))
            else:
                if prod.start_timing < 0:
                    out.write('产品 %s 还没有开始生产' % prod)
                    continue
                if prod.time_to_done <= 10:
                    out.write('产品 %s 已经结束或快要结束(%s)生产' % (prod, fmt_time(prod.time_to_done)))
                    continue
                time_to_done = prod.time_to_done
                if sub_cmd == '+':  # 增加剩余生产时间
                    pass
                elif sub_cmd == '-':  # 减少剩余生产时间
                    time_delta = -time_delta
                elif sub_cmd == '=':  # 设置剩余生产时间
                    time_delta -= time_to_done
                prod.start_timing += time_delta
        if sub_cmd == 'start':
            if len(start_dict) > 0:
                self.put_job(start_dict)
        elif sub_cmd.startswith('del'):
            self._city.show_city_status(show_all=True, out=out)
        elif sub_cmd != 'show':
            self._city.wakeup()

    def _cmd_ware(self, args):
        out = self._connection
        if len(args) > 0 and args[0] != 'show':
            try:
                capa = int(args[0])
                if args[0].startswith('+') or args[0].startswith('-'):
                    self._city.products_capacity += capa
                else:
                    self._city.warehouse_capacity = capa
            except ValueError:
                material_list, not_found = MaterialList.needs2list(args)
                if len(not_found) > 0:
                    out.write("在产品列表中没有找到 %s" % not_found)
                    MaterialDict.show_dict(out, self._city)
                    return
                elif len(material_list) == 0:
                    return
                elif not (yield from self._confirm_materials(material_list, '入库')):
                    return
                self._city.warehouse.extend(material_list)
                self._city.cprint('成功入库: %s', material_list)
            out.write('仓库: %d/%d(%d)/(%d)%d' %
                      (self._city.warehouse.capacity, self._city.warehouse.products_len, len(self._city.warehouse), (self._city['_special_products'] + self._city.warehouse.products_len),
                       self._city.warehouse_capacity))
            self._city.wakeup()
        elif len(args) > 0:
            if len(args) > 1:
                need = args[1]
                if not MaterialDict.has(need):
                    _m = MaterialDict.get_by_en_name(need)
                    if _m is None:
                        out.write("在产品列表中没有找到 %s" % need)
                        return
                    else:
                        need = _m.cn_name
                w = []
                for p in self._city.warehouse:
                    if (isinstance(p, Product) and p.cn_name == need) or (isinstance(p, str) and p == need):
                        w.append(p)
            else:
                w = self._city.warehouse
            out.write('仓库: %d/%d(%d)/(%d)%d\n%s' %
                      (self._city.warehouse.capacity, self._city.warehouse.products_len, len(self._city.warehouse), (self._city['_special_products'] + self._city.warehouse.products_len),
                       self._city.warehouse_capacity, json.dumps(w, indent=2, ensure_ascii=False, sort_keys=True)))
        else:
            # 未实际入库的商品分开显示
            _in_fact = []
            _in_ware = []
            for p in self._city.warehouse:
                if not isinstance(p, Product) or p.in_warehouse:
                    _in_ware.append(p)
                else:
                    _in_fact.append(p)
            out.write('仓库: %d/%d(%d)/(%d)%d%s%s' %
                      (self._city.warehouse.capacity, self._city.warehouse.products_len, len(self._city.warehouse), (self._city['_special_products'] + self._city.warehouse.products_len),
                       self._city.warehouse_capacity, '\n%s%s' % (' ' * 15, MaterialList.to_str(_in_ware, prefix='', suffix='')) if len(_in_ware) > 0 else '',
                       '\n%s\x1b[2;38;46m%s\x1b[0m' % (' ' * 15, MaterialList.to_str(_in_fact)) if len(_in_fact) > 0 else ''))

    _help_message = (
        ('消费库存', 'consume', '2*西瓜 3*面包 | BATCH_ID [= | [-] 1*西瓜 1*面包]'),
        ('看/加库存', 'warehouse', '[show [xig] | 2*西瓜 3*面包 | [+|-]CAPACITY]'),
        ('时间快进', 'forward', '[N]'),
        ('工厂设置', 'fact', '[slot [1]]'),
        ('商店设置', 'shop', '[建材店|jcd [slot [1] | speed [2 [3600] | star [1]]]'),
        # start 强制生产指定产品(忽略仓库容量); del 删除待产商品; ware 移入仓库; +/- time_delta 调整完成时间
        ('生产设置', 'prod', 'PID[,PID1,...] [show] | start | del[ete] | ware | {json} |[+ | -] TIME_DELTA \x1b[3;38;48m(format: 1h1m1s or 1:1:1)\x1b[0m'),
        ('通知中心', 'nfc', '[on | off]'),
        ('自动入库', 'auto_ware', '[on | off]'),
        ('二次确认', 'confirm', '[N]'),
        ('查看城市', 'show', '[c(ity) | m(aterial) | p(roducting)\x1b[3;38;48m(default)\x1b[0m]'),
        ('保存城市', 'dump', '[xxx.json]'),
        ('从头生产', '++', '2*西瓜 3*面包 [; 1*shab 3*muc]'),
        ('生产', '+', '[--air | --npc | --ship] 2*西瓜 3*面包 [; 1*shab 3*muc]')
    )

    def _print_help(self, out):
        _max1 = len(max(*self._help_message, key=lambda m: len(m[0].encode('GBK')))[0].encode('GBK')) + 1
        _max2 = len(max(*self._help_message, key=lambda m: len(m[1].encode('GBK')))[1].encode('GBK')) + 1
        for msg in self._help_message:
            out.write('%s: %s %s' % (format_cn(msg[0], _max1, left_align=True), format_cn('\x1b[4;38;48m\x1b[1;38;48m%s\x1b[0m' % msg[1], _max2, left_align=True), msg[2]))

    @asyncio.coroutine
    def _cmd_handle(self, cmd_line):
        out = self._connection
        args = cmd_line.lower().split()
        if len(args) == 0:
            # 让city检查是否有排产需求
            self._city.wakeup()
            return
        cmd = args.pop(0)
        if cmd == 'help':
            self._print_help(out)
        elif cmd.startswith('cons'):  # consume
            yield from self._cmd_consume(args)
        elif cmd == 'nfc':
            if len(args) == 0:
                out.write('Notification Center is %s' % ('on' if self._city.nfc_on else 'off'))
                return
            if args[0] == 'on':
                self._city.nfc_on = True
            elif args[0] == 'off':
                self._city.nfc_on = False
            self._city.cprint('Notification Center set to %s' % ('on' if self._city.nfc_on else 'off'))
        elif cmd.startswith('auto'):
            if len(args) == 0:
                out.write('Auto Move into Warehouse is %s' % ('on' if self._city.auto_into_warehouse else 'off'))
                return
            if args[0] == 'on':
                self._city.auto_into_warehouse = True
            elif args[0] == 'off':
                self._city.auto_into_warehouse = False
            self._city.cprint('Auto Move into Warehouse set to %s' % ('on' if self._city.auto_into_warehouse else 'off'))
        elif cmd.startswith('conf'):  # confirm
            if len(args) == 0:
                out.write('double Confirm is %d' % self._city.double_confirm)
                return
            self._city.double_confirm = max(1, int(args[0]))
            self._city.cprint('double Confirm set to %d' % self._city.double_confirm)
        elif cmd == 'dump':
            filename = self._city.dump(file=None if len(args) == 0 else args.pop(0))
            out.write('dump city to %s ok' % filename)
        elif cmd == 'acl':
            if len(args) > 1:
                op = args.pop(0)
                if op == '+':
                    self._city.do_acl_add(out, args)
                elif op == '-':
                    self._city.do_acl_del(out, args)
                else:
                    self._print_help(out)
            else:
                self._city.do_list_acl(out)
        elif cmd.startswith('forw'):  # forward
            forward = int(args[0]) if len(args) > 0 else 1
            self._city.wakeup(forward)
        elif cmd.startswith('prod'):  # product
            if len(args) < 1:
                self._print_help(out)
                return
            try:
                self._cmd_prod(args)
            except ValueError:
                self._print_help(out)
                return
        elif cmd == 'shop':
            self._cmd_shop(args)
        elif cmd.startswith('fact'):  # factories
            if len(args) > 0:
                sub_cmd = args.pop(0)
                if sub_cmd == 'slot':
                    ext_slot = int(args.pop(0)) if len(args) > 0 else 1
                    self._city.factories_setting(ext_slot=ext_slot)
                    self._city.cprint('%s 增加 %d 个生产位，总共生产位: %d', format_cn(self._city.factories.cn_name, 8, left_align=True), ext_slot, self._city.factories.slot)
                    self._city.wakeup()
                else:
                    self._print_help(out)
            else:
                out.write('%s: %d/%d %s' % (format_cn(self._city.factories.cn_name, 8, left_align=True), self._city.factories.idle_slot, self._city.factories.slot,
                                            self._city.factories.print_arrangement(print_idle=True)))
        elif cmd.startswith('ware'):  # warehouse
            yield from self._cmd_ware(args)
        elif cmd == 'show':
            show_what = 'p' if len(args) == 0 else args[0]
            if show_what.startswith('m'):
                MaterialDict.show_dict(out, self._city, sort_by_seq=show_what == 'ms')
            elif show_what.startswith('c'):
                self._city.show_city_status(show_all=True, out=out)
            elif show_what == 'p':
                ps = self._city.get_producting_list()
                for p in ps:
                    fact_name = self._city.factories.cn_name if p.is_factory_material else self._city.get_shop(p.shop_name).cn_name
                    out.write('%s将在 \x1b[1;37;40m%s\x1b[0m 后完成 \x1b[3;38;48m%s\x1b[0m' % (format_cn(fact_name, 11), fmt_time(p.time_to_done), repr(p)))
        elif cmd in ('+', '++') and len(args) > 0:
            if self._city.is_city_idle:
                self._city.reset_timing()
            # + 可以消费仓库中的物料进行排产; ++ 不消费仓库物料，全部从工厂商店进行生产
            # + --air | --npc | --ship 指定生产目的
            needs_list = cmd_line.split(cmd, 1)[1].strip().split(';')
            prod_type = Product.PT_SELL if cmd == '++' else Product.PT_BUILDING
            expect_done_time = 0
            for needs in needs_list:
                while needs.startswith('--'):
                    _prod_type, needs = needs.strip().split(None, 1)
                    _prod_type = _prod_type.strip().lower()
                    needs = needs.strip()
                    if _prod_type == '--air':
                        prod_type = Product.PT_CARGO_AIR
                    elif _prod_type == '--ship':
                        prod_type = Product.PT_CARGO_SHIP
                    elif _prod_type == '--npc':
                        prod_type = Product.PT_NPC
                    else:
                        expect_done_time = str2time(_prod_type[2:])
                material_list, not_found = MaterialList.needs2list(needs)
                if len(not_found) > 0:
                    out.write("在产品列表中没有找到 %s" % not_found)
                    MaterialDict.show_dict(out, self._city)
                    continue
                elif len(material_list) == 0:
                    continue
                elif not (yield from self._confirm_materials(material_list, '排产')):
                    continue
                # backup whole city, for recover if wrong product arrangement
                self._city.dump(file='city_backup/simcity_%s-%d.json' % (self._city.city_nick_name, self._city.product_batch_no))
                self._city.arrange_materials_to_product(material_list, prod_type=prod_type, expect_done_time=expect_done_time)
            # self._city.wakeup()  # not wakeup for wait more needs input
        else:
            self._print_help(out)


class _ConnectionClosed(Exception):
    pass


class _Connection:

    def __init__(self, mayor: Mayor, conn):
        self._mayor = mayor
        self._conn = conn
        self.__prompt_printed = -1  # -1 haven't printed prompt; 0 - printed prompt by readline(); 1 - printed
        self.tty = None
        try:
            _p = subprocess.Popen("lsof -i tcp:%d | sed -n '2,$p' | grep '%d->' | awk '{print \"ps -o tty -p \"$2}' | bash | sed -n '2,$p'" % (conn.lport, conn.lport), shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            _tty = _p.stdout.readline()
            if _tty is not None and len(_tty) > 0:
                self.tty = '/dev/%s' % _tty.decode().strip()
        except BaseException as ex:
            logging.exception("get connection's process tty fail: %s(%s)", ex.__class__.__name__, ex)

    def __str__(self):
        if self.tty is not None:
            return '%s:%s' % (self.tty, self._conn)
        else:
            return '%s' % self._conn

    def _prompt(self, pp):
        self._conn.writer.write(('%s%s ' % ('\n' if self._prompt_printed == 0 else '', fmt_time(self._mayor.city_timing, always_show_hour=True))).encode())
        self._prompt_printed = pp

    def write(self, sth, end='\n', print_prompt=True, **kwargs):
        pp = self._prompt_printed
        try:
            if pp <= 0:
                self._prompt(1 if not print_prompt else -1)
            self._conn.writer.write(('%s%s' % (sth, end)).encode())
        except BaseException as ex:
            logging.info("_Connection.write fail: %s(%s)", ex.__class__.__name__, ex, exc_info=True)

    def raw_write(self, raw_sth):
        try:
            self._conn.writer.write(('%s\n' % raw_sth).encode())
        except BaseException as ex:
            logging.info("_Connection.raw_write fail: %s(%s)", ex.__class__.__name__, ex, exc_info=True)

    @asyncio.coroutine
    def readline(self) -> str:
        try:
            with Timeout(0.1):
                _line = yield from self._conn.reader.readline()
        except asyncio.TimeoutError:
            if self._prompt_printed < 1:
                self._prompt(0)
            _line = yield from self._conn.reader.readline()
            self._prompt_printed = -1
        if _line is None or len(_line) == 0:
            raise _ConnectionClosed()
        return _line.decode()

    @property
    def _prompt_printed(self):
        return self.__prompt_printed

    @_prompt_printed.setter
    def _prompt_printed(self, pp):
        self.__prompt_printed = pp

    @property
    def is_closed(self):
        return self._conn.is_closing

    @asyncio.coroutine
    def close(self):
        yield from self._conn.aclose()


