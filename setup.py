from setuptools import setup, find_packages

setup(
    name="SnowCat",
    version="0.1",
    packages=find_packages(exclude=('test', 'examples')),
    install_requires=[
        'celery<3.2',
        'redis',
        'flask',
        'msgpack-python',
        'lockfile'
    ],
    zip_safe=False,

    author="Marco Dallagiacoma",
    author_email="dallagiac@fbk.eu",
    description="A tool to setup real-time categorizers in python",
    license="GPL",
    keywords="realtime categorizer",
    url="https://github.com/mpbaunofficial/snowcat",

    # could also include long_description, download_url, classifiers, etc.
)