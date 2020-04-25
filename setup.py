import os

from setuptools import setup, find_packages

# load versions
__version__ = None
exec(open("shmr/version.py").read())
assert __version__ is not None

with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "README.md"), "r", encoding="utf-8") as f:
    long_description = f.read()

setup(name="shmr",
      version=__version__,
      packages=find_packages(),
      description="A set of map-reduce high-order functions to use with parallel or xargs",
      long_description=long_description,
      long_description_content_type='text/markdown',
      author="Binh Vu",
      author_email="binh@toan2.com",
      url="https://github.com/binh-vu/shmr",
      python_requires='>3.6',
      license="MIT",
      install_requires=['orjson', 'tqdm', 'docstring-parser>=0.6', 'fastnumbers', 'cityhash'],
      scripts=["bin/shmr"],
      package_data={'': ['*.so', '*.pyd']})
