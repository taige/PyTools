
from simcity.factories import *


class Shop(Factories):

    def __str__(self):
        return self.cn_name

    @property
    def seq(self):
        return self.get('seq', 0)

    @property
    def pending_timing(self):
        if not self.is_busying:
            return 0
        timing = 0
        for i in sorted(range(self.slot), reverse=True):
            p = self.factory_get(i)
            if p is None or p.is_done():
                continue
            timing = p.time_to_done
            break
        logging.debug('%s.pending_timing=%s', self.cn_name, fmt_time_delta(timing))
        return timing

    def get_schedule(self, initial=False) -> Schedule:
        return ShopSchedule(0, self) if initial else ShopSchedule(self._city.city_timing, fact=self, pending_timing=self.pending_timing)

    def is_busying(self):
        w = self._factory_get(0)
        return w is not None and w.start_timing >= 0

    def _product(self, product: Product, force=False):
        '''尝试排产（如果有空闲生产位的话）'''
        _consumed = []
        if not self._city.warehouse.consume(*product.raw_materials, batch_id=product.batch_id, consumed=_consumed,
                                            exact_batch=product.is_for_sell, any_batch=self.available_slot == self.slot, anything=force):
            return '没有足够的原材料: %s, 预期: %s, 实际: %s' % (product.raw_materials, product.raw_consumed, _consumed)
        else:
            product.raw_consumed = _consumed
            self._if_consumed_another_raw(product)
        for c in product.raw_consumed:
            if isinstance(c, Product) and not c.in_warehouse:
                if not self._city.move_product_to_warehouse(c):
                    self._city.cprint('\x1b[1;37;41m移动 %s 至仓库失败\x1b[0m', repr(c))
        return super()._product(product, force=force)

    def _if_consumed_another_raw(self, product):
        # 使用了别的batch的原料的话，要归还
        ret_prods = []  # 记录本次归还的产品id，防止一件prod重复用来归还
        for p in product.raw_consumed:
            if isinstance(p, Product) and p.batch_id != 0 and p.batch_id != product.batch_id:
                c = product.find_child(p.cn_name, ret_prods)
                if c is None:
                    # 在当前product下找不到的话，在批次内找
                    c = product.root.find_child(p.cn_name, ret_prods)
                if c is None:
                    # 全局查找
                    c = self.city.find_product(p.cn_name, ret_prods, not_in_batch=product.batch_id)
                if c is not None:
                    _log = '生产[%s]使用了批次[%d]的原料[%s.%d]，将[%s]归还给原批次' % (product, p.batch_id, p, p.depth, c)
                    c.batch_id = abs(p.batch_id)
                    c.depth = p.depth
                    c.prod_type = p.prod_type
                    c.latest_product_timing = p.latest_product_timing
                    logging.info(_log + ('=>[%s.%d]' % (c, c.depth)))
                    ret_prods.append(c.pid)
                else:
                    self._city.cprint('\x1b[1;38;41m\x1b[5;38;41m生产[%s]使用了批次[%d]的原料[%s.%d]，但没找到可归还的原料\x1b[0m' % (product, p.batch_id, p, p.depth))

    def move_to_warehouse(self, product: Product, i=-1):
        # 商店只能按顺序移入仓库
        for idx in range(0, self.slot):
            pid = self._factory[idx]
            if pid is None or pid > 0:
                continue
            self._move_fact_to_ware(idx, product)
            if product is not None and -pid == product.pid:
                return True
        return product is None

    def _check_slot_done(self, i):
        m = super()._check_slot_done(i)
        if m is not None:
            if m.depth > 0 and self._city.auto_into_warehouse:
                # depth == 0 在super里已经move_to_warehouse
                self.move_to_warehouse(m, i)
            _done = self._factory.pop(0)
            self._factory.append(_done)
            p = self._factory_get(0)
            if p is not None and p.start_timing == -1:
                p.start_timing = self._city.city_timing
                self._city.cprint('  %s 开始生产 %s\x1b[1;38;48m%s\x1b[0m, 预计耗时 %s', self.cn_name, '' if p.depth > 0 else '\x1b[4;38;48m', p, fmt_time_delta(p.time_consuming))
        return m

    def _compose_factory_arrange(self, batch_id, detail=False):
        _arrange_str = '['
        for i in range(0, self.slot):
            pid = self._factory[i]
            if i > 0:
                _arrange_str += '->'
            if pid is None:
                if batch_id > 0:
                    break  # shop的生产位始终在最前面
                _arrange_str += '..'
                continue
            p = self._producting(pid)
            clr = False
            if pid < 0:
                _arrange_str += '\x1b[0;34;46m'
                clr = True
            elif p.time_to_done > 0 and '_speed_up_timing' in p and p['_speed_up_end_timing'] > self.city.city_timing and batch_id == 0:
                _arrange_str += '\x1b[1;33;48m'
                clr = True
            if p.depth == 0:
                _arrange_str += '\x1b[4;38;48m'
                clr = True
            if batch_id == 0 or p.batch_id == batch_id:
                _arrange_str += '%s' % (p if batch_id != 0 else repr(p))
            if clr:
                _arrange_str += '\x1b[0m'
        _arrange_str += ']'
        # if self.speed_up_end_timing > self._city.city_timing and batch_id == 0:
        #     _arrange_str = '\x1b[1;33;48m%s\x1b[0m' % _arrange_str
        return _arrange_str


class ShopSchedule(FactorySchedule):

    def __init__(self, city_timing, fact=None, **kwargs):
        super().__init__(city_timing, fact, **kwargs)


if __name__ == '__main__':

    s = ShopSchedule(round(time.time() - zero_timing()))
    s.schedule_earliest_impl('6h', '2h15m', '牛肉1')
    s.schedule_earliest_impl('3h', '27m', '面粉1')
    s.schedule_earliest_impl('6h', '1h34m', '奶酪1')
    s.schedule_earliest_impl('3h', '27m', '面粉2')
    s.schedule_earliest_impl('6h', '1h34m', '奶酪2')
    s.schedule_earliest_impl('6h', '2h15m', '牛肉2')
    # s.schedule_earliest_impl('20m', '18m', '蔬菜')
    # s.schedule_earliest_impl('20m', '18m', '蔬菜2')
    # s.schedule_earliest_impl('20m', '18m', '蔬菜2')
    # s.schedule_earliest_impl('3h', '27m', '面粉3')
    # s.schedule_earliest_impl('3h', '27m', '面粉4')
    s.schedule_earliest_impl('20m', '18m', '蔬菜1')
    s.schedule_earliest_impl('1h57m', '1h21m', '西瓜1')
    s.schedule_earliest_impl('20m', '18m', '蔬菜2')
    s.schedule_earliest_impl('1h57m', '1h21m', '西瓜2')
    s.log(stdout=True, fact_name='农贸市场')
