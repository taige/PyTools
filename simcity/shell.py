import signal
import json
import logging.config

import uvloop

from simcity import *
from simcity.city import SimCity


def main(args=None):

    if args is None:
        args = sys.argv[1:]

    base_conf = lookup_conf_file('simcity_conf.json')
    with open(base_conf, 'r') as _conf_f:
        conf = json.load(_conf_f)

    json_file = 'simcity.json'
    warehouse_materials = []

    new_city = False
    while len(args) > 0:
        need = args.pop(0)
        count = 1
        if need == '-c':
            json_file = args.pop(0)
            continue
        elif need == '-n':
            new_city = True
            continue
        elif need.find('*') > 0:
            buf = need.split('*')
            try:
                count = int(buf[0])
                need = buf[1]
            except ValueError:
                count = int(buf[1])
                need = buf[0]
        if count == 0:
            continue
        for c in range(0, count):
            warehouse_materials.append(need)

    if os.path.isfile(json_file):
        with open(json_file, 'r') as _city_f:
            conf.update(**json.load(_city_f))

    if new_city:
        for k in sorted(conf.keys()):
            if k.startswith('_'):  # _ startswith is running data
                conf.pop(k)

    logging_conf = lookup_conf_file(conf.get('logging_conf', 'simcity_logging.conf'))

    try:
        logging.config.fileConfig(logging_conf, disable_existing_loggers=False)
    except BaseException as ex_log:
        logging.basicConfig(format='%(asctime)s %(levelname)-5s [%(threadName)-14s] - %(message)s', level=logging.DEBUG, stream=sys.stderr)
        logging.exception('fileConfig(%s) fail: %s', logging_conf, ex_log)

    asyncio.set_event_loop(uvloop.new_event_loop())
    loop = asyncio.get_event_loop()

    def _dump_func(city_instance, file=None):
        if file is None:
            file = json_file
        if not file.endswith('.json'):
            file += '.json'
        _dir = os.path.dirname(file)
        if _dir and not os.path.exists(_dir):
            os.makedirs(_dir)
        with open(file+'.ing', "w") as _dump_f:
            json.dump(city_instance, _dump_f, indent=2, ensure_ascii=False, sort_keys=True)
        os.rename(file + '.ing', file)
        logging.info('dump to %s ok', file)
        return file

    city = SimCity(conf.pop('materials'), dump_func=_dump_func, loop=loop, **conf)

    if len(warehouse_materials) > 0:
        city.warehouse.extend(warehouse_materials)

    def term_handler(signum):
        from tsproxy.common import print_stack_trace
        if signum == signal.SIGQUIT:
            logging.debug('received SIGQUIT, print stack trace ...')
            print_stack_trace()
            return
        logging.debug('received %s, do graceful shutdowning ...',
                      'SIGTERM' if signum == signal.SIGTERM else
                      'SIGINT' if signum == signal.SIGINT else '%d' % signum)
        try:
            city.closed = True
            for t in asyncio.Task.all_tasks():
                t.cancel()
        except FileNotFoundError:
            pass
        except BaseException as ex1:
            logging.warning('term_handler error: %s(%s)', ex1.__class__.__name__, ex1)
        finally:
            loop.stop()

    for signame in ('SIGINT', 'SIGTERM', 'SIGQUIT'):
        signum = getattr(signal, signame)
        loop.add_signal_handler(signum, term_handler, signum)

    loop.create_task(city.city_mayor.job_watcher())
    loop.create_task(city.run_city())
    loop.run_until_complete(city.start())
    loop.run_forever()
    logging.info("graceful shutdown.")


if __name__ == '__main__':
    # main("帽子x2 水泥x1 布料x3 油漆x2 砖块x1 玉米x2 锤子x2 油漆x2 ".split())
    # main("沙冰x2".split())
    main()
