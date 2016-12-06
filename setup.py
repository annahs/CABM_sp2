try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

config = {
    'description': 'CABM_sp2',
    'author': 'Sarah Hanna',
    'url': 'URL to get it at.',
    'download_url': 'Where to download it.',
    'author_email': 'sarah.hanna@gmail.com',
    'version': '0.1',
    'install_requires': ['nose'],
    'packages': ['CABM_sp2'],
    'scripts': [],
    'name': 'CABM_sp2'
}

setup(**config)