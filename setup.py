from setuptools import setup, find_packages

setup(name='TrendCalculus',
      version='0.0.1',
      packages=find_packages(),
      install_requires=[
          'click'
      ],
      include_package_data=True,
      entry_points='''
        [console_scripts]
      ''',
      description="Fast bottom up streaming multiscale trend reversal detection algorithm, and analysis",
      author='Andrew J Morgan',
      author_email='andrew@bytesumo.com',
      license='GPL3',
      url="https://github.com/bytesumo/TrendCalculus"
      )
