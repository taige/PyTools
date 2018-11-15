from distutils.core import setup

install_requires = ['uvloop', 'requests', 'async_timeout', 'aiohttp>=2.0', 'pyyaml', 'psutil', 'shadowsocks', 'dnspython3']

setup(
    name='PyTools',
    version='1.0',
    packages=['tsproxy', 'pyclda', 'simcity'],
    url='',
    license='Apache-2.0',
    install_requires=install_requires,
    package_data={
        'tsproxy': ['conf/router.yaml', 'conf/ss-proxy-logging.conf', 'conf/tsproxy.conf'],
        'simcity': ['conf/simcity_conf.json', 'conf/simcity_logging.conf']
    },
    entry_points="""
    [console_scripts]
    tsproxy = tsproxy.shell:main
    pyclda = pyclda:main
    schelp = simcity.shell:main
    """,
    author='taige',
    author_email='hongqiang.wu@gmail.com',
    description='Python Tools Collection'
)

