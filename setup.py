from setuptools import setup

setup(
    name='kgx',
    version='0.0.1',
    packages=['kgx'],
    install_requires=['Click'],
    entry_points="""
        [console_scripts]
        kgx=kgx:cli
    """
)
