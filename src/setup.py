from setuptools import setup, find_packages

setup(name='tp2',
      version='0.1',
      description='7574_tp2',
      author='Alejandro Pernin',
      author_email='apernin@fi.uba.ar',
      packages=find_packages(exclude=('tests', 'documentation', 'tests.*', 'snippets', 'code_samples')),
      )
