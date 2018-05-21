from setuptools import setup

setup(
    name='Knowledge Graph Exchange',
    version='0.0.1',
    packages=['kgx'],
    install_requires=['Click'],
    scripts=['bin/translator_kgx.py'],
    entry_points="""
        [console_scripts]
        kgx=translator_kgx:cli
    """
)
