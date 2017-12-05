from setuptools import setup, find_packages

setup(name='get-user-info',
      version='1.0.0',
      description='Event Data Analyse project.',
      url='http://www.andpay.me',
      author='kesheng.wang',
      author_email='kesheng.wang@andpay.me',
      license='Private',
      packages=find_packages(),
      install_requires=['ti-srv-cfg-python', 'ti-daf-python', 'bi-common-util'],
      zip_safe=False,
      test_suite='nose.collector',
      tests_require=['nose'],
      scripts=['bin/get-user-info','bin/get-relative-phone','bin/get-reportid'],
      entry_points={
          'console_scripts': [],
      },
      include_package_data=True
      )
