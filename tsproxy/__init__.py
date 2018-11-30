#!/usr/bin/env python3
#
# Copyright 2015-2017 WuHongqiang(Taige)
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
import os
import sys
from datetime import datetime

__all__ = ['lookup_conf_file', 'str_datetime', 'ts_print', '__version__', 'conf_path']


__version__ = "1.0.181130.5"

conf_path = []


def str_datetime(timestamp=None, fmt='%Y-%m-%d %H:%M:%S,%f', start=0, end=23):
    if timestamp:
        dt = datetime.fromtimestamp(timestamp)
    else:
        dt = datetime.now()
    return dt.strftime(fmt)[start:end]


def ts_print(*args, **kwargs):
    file = kwargs.get('file', None)
    kwargs.setdefault('flush', True)
    if file is not None and file is sys.stderr:
        print(str_datetime(), '[stderr]', *args, **kwargs)
    else:
        print(str_datetime(), '[stdout]', *args, **kwargs)


def lookup_conf_file(conf_file):
    global conf_path
    if len(conf_path) == 0:
        conf_path.append(os.getcwd())
        conf_path.append(os.getcwd() + '/conf')
        if __path__ is not None and len(__path__) > 0:
            conf_path.extend(__path__)
            conf_path.append(__path__[0] + '/conf')
        conf_path.extend(sys.path)
        ts_print('conf_path=%s' % conf_path, file=sys.stderr, flush=True)
    for path in conf_path:
        if os.path.isdir(path):
            full_path = conf_file if '/' in conf_file else path + '/' + conf_file
            if os.path.isfile(full_path):
                ts_print('lookup_conf_file: %s -> %s' % (conf_file, full_path), file=sys.stderr, flush=True)
                return full_path
    ts_print('lookup_conf_file not found: %s' % conf_file, file=sys.stderr, flush=True)
    return conf_file
