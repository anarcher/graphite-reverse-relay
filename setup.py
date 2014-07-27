from setuptools import setup, find_packages

version = '0.0.1'

requires = [
    'Twisted',
    'simplejson',
]

setup(name='reverse-relay',
      version=version,
      description='graphite-reverse-reply',
      packages=find_packages(),
      include_package_data=True,
      zip_safe=False,
      install_requires=requires,
      #dependency_links = dependencies,
      tests_require=requires,
      entry_points="""
      [console_scripts]
      reverse-relay = reverse_relay.main:main
      """)
