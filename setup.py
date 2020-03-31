import setuptools
from setuptools import setup

base_url = 'https://gitlab.mobilityservices.io'

setup(name='fleet',
      version_format='{tag}.dev{commits}',
      setup_requires=['very-good-setuptools-git-version'],
      description='Data catalogue tools',
      url=base_url + '/am/roam/perception/data-catalogue',
      author='Harsimrat Sandhawalia',
      author_email='harsimrat_singh.sandhawalia@daimler.com',
      license='DAS Proprietry',
      packages=setuptools.find_packages(),
      scripts=['bin/data-catalogue'],
      test_suite='nose.collector',
      tests_require=['nose'],
      zip_safe=False)
