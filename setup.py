from os.path import dirname, join

from setuptools import setup

import sapextractor


def read_file(filename):
    with open(join(dirname(__file__), filename)) as f:
        return f.read()


setup(
    name=sapextractor.__name__,
    version=sapextractor.__version__,
    description=sapextractor.__doc__.strip(),
    long_description=read_file('README.md'),
    author=sapextractor.__author__,
    author_email=sapextractor.__author_email__,
    py_modules=[sapextractor.__name__],
    include_package_data=True,
    packages=['sapextractor', 'sapextractor.algo', 'sapextractor.algo.o2c', 'sapextractor.algo.ap_ar',
              'sapextractor.utils', 'sapextractor.utils.blart', 'sapextractor.utils.dates', 'sapextractor.utils.tstct',
              'sapextractor.utils.vbtyp', 'sapextractor.utils.change_tables', 'sapextractor.utils.graph_building',
              'sapextractor.utils.string_matching', 'sapextractor.database_connection'],
    url='http://www.pm4py.org',
    license='GPL 3.0',
    install_requires=[
        "pm4py",
        "pm4pymdl",
        "stringdist",
        "cx_Oracle"
    ]
)
