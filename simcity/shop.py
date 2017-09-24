
from simcity.factories import *


class Shop(Factories):

    def __str__(self):
        return self.cn_name

    @property
    def seq(self):
        return self.get('seq', 0)

    def is_busying(self):
        w = self._factory_get(0)
        return w is not None and w.start_timing >= 0

    def _product(self, product: Product, force=False):
        '''尝试排产（如果有空闲生产位的话）'''
        _consumed = []
        if not self._city.warehouse.consume(*product.raw_materials, batch_id=product.batch_id, consumed=_consumed,
                                            exact_batch=product.is_for_sell, any_batch=self.idle_slot == self.slot, anything=force):
            return '没有足够的原材料: %s/%s' % (product.raw_materials, product.raw_consumed)
        else:
            product.raw_consumed = _consumed
            self._if_consumed_another_raw(product)
        for c in product.raw_consumed:
            if isinstance(c, Product) and not c.in_warehouse:
                if not self._city.factories.move_to_warehouse(c):
                    self._city.cprint('\x1b[1;37;41m移动 %s 至仓库失败\x1b[0m', repr(c))
        return super()._product(product, force=force)

    def _if_consumed_another_raw(self, product):
        # 使用了别的batch的原料的话，要归还
        for p in product.raw_consumed:
            if isinstance(p, Product) and p.batch_id != 0 and p.batch_id != product.batch_id:
                c = product.find_child(p.cn_name)
                if c is None:
                    # 在当前product下找不到的话，在批次内找
                    c = product.root.find_child(p.cn_name)
                if c is not None:
                    _log = '生产[%s]使用了批次[%d]的原料[%s.%d]，将[%s]归还给原批次' % (product, p.batch_id, p, p.depth, c)
                    c.batch_id = abs(p.batch_id)
                    c.depth = p.depth
                    c.prod_type = p.prod_type
                    logging.info(_log + ('=>[%s.%d]' % (c, c.depth)))
                else:
                    self._city.cprint('\x1b[1;38;41m\x1b[5;38;41m生产[%s]使用了批次[%d]的原料[%s.%d]，但没找到可归还的原料\x1b[0m' % (product, p.batch_id, p, p.depth))

    def _start_arranged(self):
        if self._manufacturing_count() > 0:
            self._factory.pop(0)
            self._factory.append(None)
            p = self._factory_get(0)
            if p.start_timing == -1:
                p.start_timing = self._city.city_timing
                self._city.cprint('  %s 开始生产 %s\x1b[1;38;48m%s\x1b[0m, 预计耗时 %s', self.cn_name, '' if p.depth > 0 else '\x1b[4;38;48m', repr(p), fmt_time(p.time_consuming))

    def _check_slot_done(self, i):
        m = super()._check_slot_done(i)
        if m is not None:
            if m.depth > 0:
                # depth == 0 在super里已经move_to_warehouse
                self.move_to_warehouse(m, i)
            self._start_arranged()
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
            if p.depth == 0:
                _arrange_str += '\x1b[4;38;48m'
            if batch_id == 0 or p.batch_id == batch_id:
                _arrange_str += '%s' % (p if batch_id != 0 else repr(p))
            if p.depth == 0:
                _arrange_str += '\x1b[0m'
        _arrange_str += ']'
        return _arrange_str
