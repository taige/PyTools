from distutils.core import setup

install_requires = ['uvloop', 'requests', 'aiohttp>=2.0', 'pyyaml', 'psutil', 'shadowsocks', 'dnspython3']

setup(
    name='tsproxy',
    version='1.0',
    packages=['tsproxy'],
    url='',
    license='Apache-2.0',
    install_requires=install_requires,
    package_data={
        'tsproxy': ['conf/router.yaml', 'conf/ss-proxy-logging.conf']
    },
    entry_points="""
    [console_scripts]
    tsproxy = tsproxy.shell:main
    """,
    author='taige',
    author_email='hongqiang.wu@gmail.com',
    description='smart proxies router'
)
