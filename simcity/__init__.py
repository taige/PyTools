import logging
import sys
import tty
import termios
import fcntl
import os
import asyncio
import functools
import time
from collections import OrderedDict

from tsproxy.common import Timeout


conf_path = []


def lookup_conf_file(conf_file):
    global conf_path
    if len(conf_path) == 0:
        conf_path.append(os.getcwd())
        conf_path.append(os.getcwd() + '/conf')
        if __path__ is not None and len(__path__) > 0:
            conf_path.extend(__path__)
            conf_path.append(__path__[0] + '/conf')
        conf_path.extend(sys.path)
        print('conf_path=%s' % conf_path, file=sys.stderr, flush=True)
    for path in conf_path:
        if os.path.isdir(path):
            full_path = path + '/' + conf_file
            if os.path.isfile(full_path):
                print('lookup_conf_file: %s -> %s' % (conf_file, full_path), file=sys.stderr, flush=True)
                return full_path
    print('lookup_conf_file not found: %s' % conf_file, file=sys.stderr, flush=True)
    return conf_file


def fmt_time(t, always_show_hour=False, unit='s'):
    if unit == 's':
        m = t / 60
        t %= 60
    else:
        m = t
        t = 0
    if not always_show_hour and m < 60:
        return '%02d:%02d' % (m, t)
    h = m / 60
    m %= 60
    return '%02d:%02d:%02d' % (h % 24, m, t)


def str2time(str_time):
    str_time = str_time.strip().lower()
    fmt = ':' if str_time.find(':') >= 0 else 'hms'
    hms = [0, 0, 0]
    idx_h = str_time.find('h' if fmt == 'hms' else ':')
    if idx_h >= 0:
        hms[0] = int(str_time[:idx_h]) if len(str_time[:idx_h]) > 0 else 0
        str_time = str_time[idx_h + 1:]
    idx_m = str_time.find('m' if fmt == 'hms' else ':')
    if idx_m >= 0:
        hms[1] = int(str_time[:idx_m]) if len(str_time[:idx_m]) > 0 else 0
        str_time = str_time[idx_m + 1:]
    idx_s = str_time.find('s' if fmt == 'hms' else ':')
    if idx_s >= 0:
        hms[2] = int(str_time[:idx_s]) if len(str_time[:idx_s]) > 0 else 0
    else:
        if idx_m < 0 <= idx_h:
            hms[1] = int(str_time) if len(str_time) > 0 else 0
        else:
            hms[2] = int(str_time) if len(str_time) > 0 else 0
    return 3600*hms[0] + 60*hms[1] + hms[2]


def materials_init(json_materials, minute_unit=None):
    '''
        1、构建物料清单及相互依赖关系
    '''
    if minute_unit is not None:
        Material.ONE_MINUTE = minute_unit
    material_dict = MaterialDict()
    for m in json_materials:
        mo = Material(**m)
        material_dict[mo.cn_name] = mo
    for m in json_materials:
        if 'raw_materials' not in m:
            continue
        for rm in m['raw_materials']:
            raw_cls = rm['raw_class']
            if raw_cls not in material_dict:
                _err = "%s in %s's raw_materials not defined!" % (raw_cls, m['cn_name'])
                print(_err, file=sys.stderr)
                raise Exception(_err)


def format_cn(cn, width=1, left_align=False, right_align=False):
    if width < 1:
        width = 1
    clr = False
    for s in cn:
        if width <= 1:
            break
        if s == '\x1b':
            clr = True
        if clr:
            width += 1
        elif ord(s) >= 0x4E00:
            width -= 1
        if s == 'm' and clr:
            clr = False
    return '{0:>{wd}}'.format(cn, wd=width) if right_align else '{0:<{wd}}'.format(cn, wd=width) if left_align else '{0:^{wd}}'.format(cn, wd=width)


class MaterialDict(dict):
    _MATERIALS = None

    def __init__(self, iterable=(), **kwargs):
        super().__init__(iterable, **kwargs)
        self._cache = {}
        self._shops_name = None
        MaterialDict._MATERIALS = self

    @classmethod
    def has(cls, m_name):
        return m_name in cls._MATERIALS

    @classmethod
    def get(cls, m_name, default=None):
        if m_name not in cls._MATERIALS:
            raise Exception("没有找到物料 %s" % m_name)
        return cls._MATERIALS[m_name]

    @classmethod
    def shops_name(cls):
        self = cls._MATERIALS
        if self._shops_name is None:
            self._shops_name = set()
            for m in self.values():
                if not m.is_factory_material:
                    self._shops_name.add(m.shop_name)
        return self._shops_name

    @classmethod
    def show_dict(cls, out, city, sort_by_seq=False, sort_by_value=False, sort_by_value_pm=False, sort_by_profit_pm=False):
        _dict = cls._MATERIALS
        for name in sorted(_dict, key=lambda n: _dict[n].seq if sort_by_seq else _dict[n].max_value if sort_by_value else _dict[n].max_value_pm if sort_by_value_pm else _dict[n].profit_pm if sort_by_profit_pm else '%s%s%s' % ((0, _dict[n].en_name, '') if _dict[n].is_factory_material else (1, _dict[n].en_name, _dict[n].shop_name))):
            m = _dict[name]
            _str = format_cn('%s(%s)' % (m.cn_name, m.en_name), 16, left_align=True)
            if not m.is_factory_material:
                _factory = city.get_shop(m.shop_name)
            else:
                _factory = city.factories
            _str += '耗时: %s' % fmt_time(_factory.stars_speed_up * m.time_consuming, always_show_hour=True)
            _str += '/%s   ' % fmt_time(m.all_product_time_consuming, always_show_hour=True)
            _str += '$%4d/%2.0f/%2.0f   ' % (m.max_value, m.max_value_pm, m.profit_pm)
            if not m.is_factory_material:
                _str += '商店: %s 原材料: %s' % (format_cn(m.shop_name + '★' * _factory.stars, 12, left_align=True), m.raw_materials)
            out.write(_str)

    def __setitem__(self, k, v) -> None:
        if k in self:
            raise KeyError('%s exits!' % k)
        if not isinstance(v, Material):
            raise ValueError('%s is a %s, not a Material!' % (v, v.__class__.__name__))
        if v.en_name in self._cache:
            raise ValueError('%s.class=%s duplicated with %s' % (v, v.en_name, self._cache[v.en_name]))
        super().__setitem__(k, v)
        self._cache[v.en_name] = v

    @classmethod
    def get_by_en_name(cls, en_name):
        self = cls._MATERIALS
        if en_name in self._cache:
            return self._cache[en_name]
        return None

    @classmethod
    def init_all_product_time_consuming(cls, to_product):
        for m in cls._MATERIALS.values():
            if not m.is_factory_material:
                p = to_product(m)
                m['all_product_time_consuming'] = p.all_product_time_consuming


class Material(dict):
    '''物料信息,包括生产物料需要的原料清单'''
    ONE_MINUTE = 60

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._raw_materials = None
        if 'raw_materials' in self:
            self._raw_materials = MaterialList()
            for r in self['raw_materials']:
                count = r['count']
                raw_cls = r['raw_class']
                for i in range(count):
                    self._raw_materials.append(raw_cls)

    @property
    def en_name(self):
        return self['class'] if 'class' in self else ''

    @property
    def cn_name(self):
        return self['cn_name'] if 'cn_name' in self else ''

    @property
    def time_consuming(self):
        return (Material.ONE_MINUTE * self['time_consuming']) if 'time_consuming' in self else 0

    @property
    def all_product_time_consuming(self):
        return self['all_product_time_consuming'] if 'all_product_time_consuming' in self else self.time_consuming

    @property
    def is_factory_material(self):
        return self['is_factory_material'] if 'is_factory_material' in self else True if self.shop_name is None else False

    @property
    def shop_name(self):
        return self['shop'] if 'shop' in self else None

    @property
    def max_value(self):
        return self.get('max_value', 0)

    @property
    def profit(self):
        return self.max_value - (0 if self._raw_materials is None else self._raw_materials.max_values)

    @property
    def profit_pm(self):
        return self.profit / (self.time_consuming / 60)

    @property
    def max_value_pm(self):
        return self.max_value / (self.all_product_time_consuming / 60)

    @property
    def seq(self):
        return self['seq'] if 'seq' in self else ''

    @property
    def raw_materials(self):
        return self._raw_materials

    def __str__(self):
        return self.cn_name


class MaterialList(list):
    '''物料列表(主要用于优化list的显示)'''

    def __init__(self, iterable=()):
        super().__init__(iterable)

    @staticmethod
    def needs2list(needs) -> (list, list):
        if isinstance(needs, str):
            needs = needs.strip().split(',')

        material_list = MaterialList()
        not_found = []

        for ns in needs:
            ns = ns.strip().strip(',').split(' ')
            for need in ns:
                count = 1
                if need.find('*') > 0:
                    buf = need.split('*')
                    try:
                        count = 1 if buf[0] == '' else int(buf[0])
                        need = buf[1]
                    except ValueError:
                        count = 1 if buf[1] == '' else int(buf[1])
                        need = buf[0]
                if count == 0 or need == '':
                    continue
                if not MaterialDict.has(need):
                    _m = MaterialDict.get_by_en_name(need)
                    if _m is None:
                        not_found.append(need)
                        continue
                    else:
                        need = _m.cn_name
                for c in range(0, count):
                    material_list.append(need)

        return material_list, not_found

    def __str__(self):
        return MaterialList.to_str(self, empty=' ')

    @staticmethod
    def to_str(mlist, prefix='[', suffix=']', empty='', newest=None, sort_by_tm=False, sort_by_count=True):
        if len(mlist) == 0:
            return '%s%s%s' % (prefix, empty, suffix)
        _counter_ = {} if sort_by_tm or sort_by_count else OrderedDict()
        for key in mlist:
            key = key.cn_name if isinstance(key, Material) else key
            if key.find('#') > 0:
                key = key.split('#')[1]
            if key in _counter_:
                _counter_[key] += 1
            else:
                _counter_[key] = 1
        if newest is None:
            _str_ = ''
        else:
            _str_ = '%d*%s' % (_counter_[newest], newest)

        if not isinstance(_counter_, OrderedDict):

            def _counter_sort_(k1, k2):
                m1 = MaterialDict.get(k1)
                m2 = MaterialDict.get(k2)
                if sort_by_tm:
                    rc = m2.time_consuming - m1.time_consuming
                else:
                    rc = _counter_[k2] - _counter_[k1]
                if rc == 0:
                    rc = -1 if m1.seq < m2.seq else 0 if m1.seq == m2.seq else 1
                return rc

        for key in (_counter_ if isinstance(_counter_, OrderedDict) else sorted(_counter_, key=functools.cmp_to_key(_counter_sort_))):
            if newest is not None and key == newest:
                continue
            if _str_ != '':
                _str_ += ', '
            _str_ += '%d*%s' % (_counter_[key], key)
        return '%s%s%s' % (prefix, _str_, suffix)

    def extend(self, iterable) -> None:
        for m in iterable:
            if not isinstance(m, Material):
                if not isinstance(m, str):
                    raise Exception("无法解析的物料 %s" % m)
                if m.find('#') > 0:
                    m = m.split('#')[1]
                if not MaterialDict.has(m):
                    _m = MaterialDict.get_by_en_name(m)
                    if _m is None:
                        raise Exception("没有找到物料 %s" % m)
                    else:
                        self.append(_m)
                        continue
            self.append(m)

    def __sub__(self, other):
        if not isinstance(other, list):
            raise Exception('not supported operation on class: %s' % other.__class__.__name__)
        if not isinstance(other, MaterialList):
            other = MaterialList(other)
        res = MaterialList()
        for k in self.material_kinds:
            n = self.count(k) - other.count(k)
            if n > 0:
                for _ in range(n):
                    res.append(k)
        return res

    def count(self, name):
        t = 0
        for key in self:
            if isinstance(key, Material):
                key = key.cn_name
            if key.find('#') > 0:
                key = key.split('#')[1]
            if key == name:
                t += 1
        return t

    def products_count(self, name):
        t = 0
        for key in self:
            if isinstance(key, Product) and key.batch_id > 0:
                continue
            if isinstance(key, Material):
                key = key.cn_name
            if key.find('#') > 0:
                key = key.split('#')[1]
            if key == name:
                t += 1
        return t

    def __contains__(self, o: object) -> bool:
        for m in self:
            if isinstance(o, Product):
                if isinstance(m, Product) and o.pid == m.pid:
                    return True
            else:
                o_name = o.cn_name if isinstance(o, Material) else o
                if o_name.find('#') > 0:
                    o_name = o_name.split('#')[1]
                m_name = m.cn_name if isinstance(m, Material) else m
                if m_name.find('#') > 0:
                    m_name = m_name.split('#')[1]
                if m_name == o_name:
                    return True
        return False

    @property
    def material_kinds(self) -> list:
        _kinds_ = []
        for key in self:
            key = key.cn_name if isinstance(key, Material) else key
            if key.find('#') > 0:
                key = key.split('#')[1]
            if key not in _kinds_:
                _kinds_.append(key)
        return _kinds_

    # @property
    # def total_time(self):
    #     t = 0
    #     for key in self:
    #         if isinstance(key, str):
    #             key = MaterialDict.MATERIALS[key]
    #         t += key.time_consuming
    #     return t

    @property
    def max_time(self):
        t = 0
        for key in self:
            if isinstance(key, str):
                raise Exception('material %s is a `str` instance' % key)
                # key = MaterialDict.MATERIALS[key]
            if t < key.time_consuming:
                t = key.time_consuming
        return t

    @property
    def max_values(self):
        v = 0
        for m in self:
            if isinstance(m, str):
                if m.find('#') > 0:
                    m = m.split('#')[1]
                m = MaterialDict.get(m)
            if m is None or not isinstance(m, Material):
                continue
            v += m.max_value
        return v


class Product(Material):
    '''
      1、物料的生产实例
      2、根据工厂、商品的生产并行能力和物料的依赖关系, 构建生产链
    '''

    PT_SELL = 0        # 储备或出售
    PT_NPC = 1         # NPC
    PT_BUILDING = 2    # 建筑
    PT_CARGO_SHIP = 3  # 货运轮船
    PT_CARGO_AIR = 4   # 货运飞机

    PID = 0

    def __init__(self, material=None, parent=None, batch_id=0, depth=0, needs=None, prod_type=2, city=None, **kwargs):
        super().__init__(**kwargs)
        self._material = material

        if depth >= 0 and self._material is None:
            raise Exception('非生产批次对象[%d]但是没有指定生产物料' % depth)

        if 'p_pid' not in self:
            Product.PID += 1
            self['p_pid'] = Product.PID
        elif self['p_pid'] >= Product.PID:
            Product.PID = self['p_pid'] + 1

        self['depth'] = depth

        if needs is not None:
            if not isinstance(needs, MaterialList):
                needs = MaterialList(needs)
            self['needs'] = needs
            self['prod_type'] = prod_type

        self._parent = parent
        if parent is not None:
            self['p_ppid'] = parent.pid
            self['batch_id'] = parent.batch_id if batch_id == 0 else batch_id
        else:
            self['batch_id'] = batch_id

        if depth < 0:
            self['cn_name'] = "%d#ROOT" % abs(self.batch_id)
            _products = MaterialList()
            if 'products' in self:
                for p in self['products']:
                    _products.append(p)
            self['products'] = _products
        else:
            self['cn_name'] = self._material.cn_name

        self._city = city
        if self._city is None:
            _parent = self._parent
            while _parent is not None and self._city is None:
                self._city = _parent._city

        if not self.is_factory_material:
            _children = MaterialList()
            if 'children' in self:
                for d_child in self['children']:
                    if not MaterialDict.has(d_child['cn_name']):
                        raise Exception("没有找到物料 %s" % d_child['cn_name'])
                    child = Product(material=MaterialDict.get(d_child['cn_name']), parent=self, city=city, **d_child)
                    _children.append(child)
            self['children'] = _children

        self.raw_consumed = None
        self._children_changed = True

    def __str__(self):
        return '%s@%s' % (self._id_str_, fmt_time(self.latest_product_timing))

    def __repr__(self):
        return self._id_str_

    @property
    def _id_str_(self):
        return "%d#ROOT" % abs(self.batch_id) if self.depth < 0 else '%d.%d#%s' % (abs(self.batch_id), self.pid % 1000, self._material.cn_name if isinstance(self._material, Material) else self._material)

    @property
    def consumed_info(self):
        return '' if self.is_factory_material else ', 消耗 %s' % (self.raw_consumed if self.raw_consumed is not None else self.raw_materials)

    def get_child(self, pid):
        for child in self.children:
            if child.pid == pid or (child.pid % 1000) == pid:
                return child
            c = child.get_child(pid)
            if c is not None:
                return c
        return None

    def _get_undone(self, children, undone_list):
        for c in children:
            self._get_undone(c.children, undone_list)
            if c.depth == 0 and not c.is_done():
                undone_list.append(c)

    def get_undone_list(self):
        if self.is_for_sell:
            undone_list = MaterialList()
            self._get_undone(self.children, undone_list)
            return undone_list
        else:
            return self.needs - self.products

    def find_child(self, name, ret_prods: list):
        for child in self.children:
            # if child.cn_name == name and child.batch_id == self.batch_id:
            if child.cn_name == name and not child.is_done() and child.pid not in ret_prods:
                return child
        for child in self.children:
            c = child.find_child(name, ret_prods)
            if c is not None:
                return c
        return None

    @property
    def en_name(self):
        return self._material.en_name if self._material is not None else None

    @property
    def cn_name(self):
        return self._material.cn_name if self._material is not None else None

    @property
    def seq(self):
        return self._material.seq if self._material is not None else ''

    @property
    def max_value(self):
        return self._material.max_value if self._material is not None else 0

    @property
    def notified(self):
        return self.get('notified', False)

    @property
    def consumed(self):
        return self.get('consumed', False)

    @notified.setter
    def notified(self, n):
        self['notified'] = n

    @consumed.setter
    def consumed(self, c):
        self['consumed'] = c

    @property
    def in_warehouse(self):
        return self.get('in_warehouse', False)

    @in_warehouse.setter
    def in_warehouse(self, n):
        self['in_warehouse'] = n

    @property
    def time_consuming(self):
        if self._material is not None:
            if self._city is not None:
                if self.shop_name is not None:
                    _factory = self._city.get_shop(self.shop_name)
                else:
                    _factory = self._city.factories
                return _factory.adjust_time_consuming(self._material.time_consuming, self.start_timing)
            else:
                return self._material.time_consuming
        else:
            return 0

    @property
    def time_to_done(self):
        if self.start_timing >= 0:
            return max(self.time_consuming - (self._city.city_timing - self.start_timing), 0)
        elif self.start_timing == -1 and not self.is_factory_material:
            _time_to_done = 0
            _shop = self._city.get_shop(self.shop_name)
            for i in range(_shop.slot):
                p = _shop.factory_get(i)
                if p is None or p.is_done() or p.pid == self.pid:
                    break
                _time_to_done = p.time_to_done
            _time_to_done += self.time_consuming
            return _time_to_done
        else:
            return self.time_consuming

    @property
    def is_factory_material(self):
        return self._material.is_factory_material if self._material is not None else False

    @property
    def shop_name(self):
        return self._material.shop_name if self._material is not None else None

    @property
    def raw_materials(self):
        return self._material.raw_materials if self._material is not None else None

    @property
    def capacity_use(self):
        return 1 if self.raw_materials is None else 1 - len(self.raw_materials)

    @property
    def depth(self):
        return self['depth']

    @depth.setter
    def depth(self, d):
        self['depth'] = d

    @property
    def products(self):
        # TODO 检查仓库商品是否与需求一致
        if 'products' in self and (self.consumed or ('_products_timestamp' in self and (time.time() - self['_products_timestamp']) < 0.1 and not self._city.warehouse.changed)):
            return self['products']
        self['products'] = MaterialList()
        _products = []
        self._city.warehouse.consume(batch_id=self.batch_id, just_peek=True, consumed=_products)
        _products.sort(key=lambda _p: _p.batch_id)
        for i in range(len(_products)):
            p = _products[i]
            if p.batch_id < 0:
                self['products'].append(p._id_str_)
            else:
                break
        self['_products_timestamp'] = time.time()
        return self['products']

    @property
    def needs(self):
        return self['needs'] if 'needs' in self else None

    @property
    def root(self):
        if self._parent is None:
            return self
        p = self._parent
        while p.parent is not None:
            p = p.parent
        return p

    @property
    def prod_type(self):
        return self['prod_type'] if 'prod_type' in self else self.root['prod_type'] if 'prod_type' in self.root else Product.PT_BUILDING

    @prod_type.setter
    def prod_type(self, t):
        self['prod_type'] = t

    @property
    def use_warehouse(self):
        return self.prod_type != Product.PT_SELL

    @property
    def is_for_sell(self):
        return self.prod_type == Product.PT_SELL

    @property
    def prod_type_icon(self):
        # 🏠✈️🚢👤💰
        return '✈️' if self.prod_type == Product.PT_CARGO_AIR else \
            '🚢' if self.prod_type == Product.PT_CARGO_SHIP else \
            '👤' if self.prod_type == Product.PT_NPC else \
            '💰' if self.prod_type == Product.PT_SELL else '︎🏠'

    @property
    def arrange_timing(self):
        return self.root['arrange_timing']
        # return self['arrange_timing'] if 'arrange_timing' in self else 0

    @arrange_timing.setter
    def arrange_timing(self, t):
        self['arrange_timing'] = t

    @property
    def start_timing(self):
        # -3 初始（未排产）
        # -2 满足排产条件但未排产
        # -1 已安排生产（仅shop里待生产物品有效）
        # >=0 开始生产时间
        return self['start_timing'] if 'start_timing' in self else -3

    @start_timing.setter
    def start_timing(self, t):
        self['start_timing'] = t

    @property
    def batch_id(self):
        return self['batch_id']

    @batch_id.setter
    def batch_id(self, bid):
        self['batch_id'] = bid

    @property
    def pid(self):
        return self['p_pid']

    @property
    def children_is_done(self):
        for i in range(0, len(self.children)):
            c = self.children[i]
            if not c.is_done():
                logging.warning('parent[%s] is done, but [%s] NOT DONE', self, c)
                return False
            if not c.children_is_done:
                logging.warning('parent[%s] is done, but [%s]\'s children NOT DONE', self, c)
                return False
        return True

    def product_done(self):
        self['product_done'] = True
        if not self.children_is_done:
            self._city.cprint('\x1b[1;38;41m\x1b[5;38;41m[%s]\'s children HAVEN\'T done, cant detach from the chain\x1b[0m' % self, level=logging.WARNING)
        else:
            self._detach_chain()
        if self.depth == 0:
            if self.is_for_sell:
                self.batch_id = 0
            else:
                self.batch_id = -self.batch_id
        elif self._city.is_batch_done(self.batch_id):
            # 已经完成的批次的中间产品自动入仓库
            self.batch_id = 0

    def is_done(self):
        if 'product_done' in self:
            return self['product_done']
        if self.depth < 0:
            if (self.is_for_sell and len(self.children) == 0) or len(self.root.get_undone_list()) == 0:
                self['product_done'] = True
                return True
        return False

    def remove_child(self, child, mark_changed=True):
        for i in range(0, len(self.children)):
            c = self.children[i]
            if c.pid == child.pid:
                del self.children[i]
                if mark_changed:
                    self.children_changed = True
                break

    def _detach_chain(self):
        if self.parent is not None:
            if self.parent.is_done() and self.parent.children_is_done:
                self.parent._detach_chain()
            self.parent.remove_child(self, mark_changed=False)
            if self.has_children:
                self._remove_done_children()

    def _remove_done_children(self):
        if self.children_is_done:
            for i in range(0, len(self.children)):
                c = self.children.pop(0)
                c._remove_done_children()
                self._city.cprint('\x1b[1;38;42m[%s] detach for parent & children all DONE\x1b[0m', self)

    @property
    def children(self):
        return self['children'] if 'children' in self else []

    @property
    def has_children(self):
        return False if 'children' not in self else len(self['children']) > 0

    @property
    def children_changed(self):
        return self._children_changed

    @children_changed.setter
    def children_changed(self, cc):
        self._children_changed = cc
        if cc:
            p = self._parent
            while p is not None:
                p._children_changed = True
                p = p.parent

    @property
    def parent(self):
        return self._parent

    @parent.setter
    def parent(self, p):
        self._parent = p
        p.children.append(self)
        p.children_changed = True
        self.batch_id = p.batch_id
        self['arrange_timing'] = p.arrange_timing
        self['p_ppid'] = p.pid

    @property
    def all_product_time_consuming(self):
        if 'all_product_time_consuming' in self and self['all_product_time_consuming'] >= 0 and not self.children_changed:
            return self['all_product_time_consuming']
        if self._parent is None:
            t = 0
        else:
            t = self.time_consuming
        max_t = 0
        if not self.is_factory_material:
            for child in self.children:
                if max_t < child.all_product_time_consuming:
                    max_t = child.all_product_time_consuming
        self['all_product_time_consuming'] = max_t + t
        self._children_changed = False
        return self['all_product_time_consuming']

    @property
    def waiting_time(self):
        return self.all_product_time_consuming - self.time_consuming

    @property
    def latest_product_timing(self):
        if self._parent is None:
            self['latest_product_timing'] = self.all_product_time_consuming
        elif self.parent.parent is None:
            self['latest_product_timing'] = max(0, self.arrange_timing + self._parent.latest_product_timing - self.time_consuming)
        else:
            self['latest_product_timing'] = max(0, self._parent.latest_product_timing - self.time_consuming)
        return self['latest_product_timing']


class TimeoutQueue(asyncio.Queue):

    @asyncio.coroutine
    def get(self, block=True, timeout=-1):
        if not block or timeout == 0:
            return super().get_nowait()
        if timeout > 0:
            try:
                with Timeout(timeout):
                    return (yield from super().get())
            except asyncio.TimeoutError:
                raise asyncio.QueueEmpty()
        else:
            return (yield from super().get())


class raw(object):
    def __init__(self, stream):
        self.stream = stream
        self.fd = self.stream.fileno()

    def __enter__(self):
        self.original_stty = termios.tcgetattr(self.stream)
        tty.setcbreak(self.stream)

    def __exit__(self, type, value, traceback):
        termios.tcsetattr(self.stream, termios.TCSANOW, self.original_stty)


class nonblocking(object):
    def __init__(self, stream):
        self.stream = stream
        self.fd = self.stream.fileno()

    def __enter__(self):
        self.orig_fl = fcntl.fcntl(self.fd, fcntl.F_GETFL)
        fcntl.fcntl(self.fd, fcntl.F_SETFL, self.orig_fl | os.O_NONBLOCK)

    def __exit__(self, *args):
        fcntl.fcntl(self.fd, fcntl.F_SETFL, self.orig_fl)


def clear_stdin():
    stdin = sys.stdin
    if not hasattr(stdin, "fileno"):
        return
    try:
        with raw(stdin):
            with nonblocking(stdin):
                while True:
                    c = stdin.read(1)
                    if c:
                        print(repr(c))
                    else:
                        return
    except:
        return


if __name__ == '__main__':
    assert 1 == str2time('1')
    assert 3600 == str2time('1h')
    assert 3660 == str2time('1h1')
    assert 3660 == str2time('1h1m')
    assert 3661 == str2time('1h1m1')
    assert 3661 == str2time('1h1m1s')
    assert 3601 == str2time('1h1s')

    assert 3600 == str2time('1:')
    assert 3660 == str2time('1:1')
    assert 3660 == str2time('1:1:')
    assert 3661 == str2time('1:1:1')
    assert 3661 == str2time('1:1:1:')

    assert 60 == str2time('1m')
    assert 61 == str2time('1m1')
    assert 61 == str2time('1m1s')
    assert 60 == str2time('h1')
    assert 60 == str2time('h1m')
    assert 61 == str2time('h1m1')
    assert 61 == str2time('h1m1s')

    assert 1 == str2time('1s')
    assert 1 == str2time('m1')
    assert 1 == str2time('m1s')

