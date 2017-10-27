
from simcity import *


class Warehouse(MaterialList):

    def __init__(self, city, interable=()):
        super().__init__(interable)
        self._city = city
        self._changed = False

    def status_reset(self):
        self._changed = False

    @property
    def products_len(self):
        l = 0
        for p in self:
            if not isinstance(p, Product) or p.in_warehouse:
                l += 1
        return l

    @property
    def capacity(self):
        return self._city.products_capacity - self.products_len - self._city.producting_capacity()

    @property
    def changed(self):
        return self._changed

    def append(self, obj):
        super().append(obj)
        self._changed = True

    def consume(self, *material_list, batch_id, just_peek=False, consumed=None, exact_batch=False, any_batch=False, anything=False):
        # TODO shop空闲时可以先使用"最终产品"来生产, 是否合理?
        if len(material_list) == 0 and batch_id <= 0:
            return False
        needs = [*material_list]
        if consumed is None:
            consumed = []
        for prod in sorted(self, key=lambda p: float('%d.%d' % (-p.batch_id, p.pid)) if isinstance(p, Product) and (p.batch_id == batch_id or p.batch_id == 0) else 1 if not isinstance(p, Product) else 2):
            if len(material_list) == 0:
                if not isinstance(prod, Product):
                    continue
                if abs(prod.batch_id) == batch_id:
                    consumed.append(prod)
                continue
            for j in range(0, len(needs)):
                name = needs[j].cn_name if hasattr(needs[j], 'cn_name') else needs[j]
                if (isinstance(prod, str) and prod == name)\
                        or (isinstance(prod, Product) and prod.cn_name == name and (prod.batch_id == 0 or anything or (any_batch and prod.batch_id > 0) or (batch_id == 0 and prod.batch_id < 0) or abs(prod.batch_id) == batch_id)) \
                        or (not isinstance(prod, Product) and isinstance(prod, Material) and prod.cn_name == name):
                    if not anything and not any_batch and exact_batch and (not hasattr(prod, 'batch_id') or prod.batch_id != batch_id):
                        continue
                    consumed.append(prod)
                    del needs[j]
                    break
        rc = len(consumed) == len(material_list) or len(material_list) == 0
        if not rc or just_peek:
            return rc
        for e in consumed:
            found = False
            for i in range(0, len(self)):
                if self[i] is e:
                    del self[i]
                    found = True
                    break
            if not found:
                raise Exception('%s not found in warehouse: %s' % (e, self))
        self._changed = True
        return rc

    def __str__(self):
        # [[ 2*金属, 1*被子 #1:[2*金属, 1*被子 ... 2*金属, 1*被子] ]]
        if len(self) == 0:
            return '%d/%d/(%d)%d %s' % (self.capacity, self.products_len, (self._city['_special_products']+self.products_len), self._city.warehouse_capacity, '[[  ]]')
        _newest = self[len(self)-1]
        _newest_bid = abs(_newest.batch_id) if isinstance(_newest, Product) else 0
        _batch_cache = {}
        _batchids = []
        for p in self:
            p_name = p.cn_name if isinstance(p, Material) else p
            p_batchid = p.batch_id if isinstance(p, Product) else 0
            if p_name.find('#') > 0:
                p_name = p_name.split('#')[1]
            if p_batchid in _batch_cache:
                _batch_cache[p_batchid].append(p_name)
            else:
                _batch_cache[p_batchid] = [p_name]
            if abs(p_batchid) not in _batchids:
                _batchids.append(abs(p_batchid))

        _str_ = ''
        for p_batchid in sorted(_batchids):
            if _str_ != '':
                _str_ += ' '
            _str_ += '\n%s' % (' '*23)
            if p_batchid == 0:
                _str_ += '%s' % MaterialList.to_str(_batch_cache[p_batchid], prefix='', suffix='')
            else:
                _str_ += '#%-2d:' % p_batchid
                if -p_batchid in _batch_cache:
                    _str_ += '[\x1b[1;38;46m%s\x1b[0m' % MaterialList.to_str(_batch_cache[-p_batchid], prefix='', suffix='')
                else:
                    _str_ += '['
                if p_batchid in _batch_cache:
                    _str_ += ' ... \x1b[0;34;46m%s\x1b[0m]' % MaterialList.to_str(_batch_cache[p_batchid], prefix='', suffix='')
                else:
                    _str_ += ']'
                if self._city.is_batch_done(p_batchid):
                    _str_ += ' \x1b[1;31;48m[ ✔︎ ]\x1b[0m'
        return '%d/%d/(%d)%d %s' % (self.capacity, self.products_len, (self._city['_special_products']+self.products_len), self._city.warehouse_capacity, _str_)
