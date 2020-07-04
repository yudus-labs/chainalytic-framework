# -*- coding: utf-8 -*-
from setuptools import setup, find_packages

# All dependences
deps = {
    'chainalytic': [
        'ruamel.yaml==0.16.5',
        'msgpack==0.6.2',
        'plyvel==1.1.0',
        'jsonrpcserver==4.1.2',
        'jsonrpcclient[websockets]',
        'jsonrpcclient[requests]',
        'websockets==8.1',
        'aiohttp==3.6.2',
        'aiohttp-cors==0.7.0',
        'iconservice',
        'requests==2.22.0',
        'iconsdk',
    ],
    'test': [
        'pytest',
    ],
    'dev': [
        'tox',
        'pylint',
        'autopep8',
        'rope',
        'black',
    ]
}
deps['dev'] = (
    deps['chainalytic'] +
    deps['dev']
)
deps['test'] = (
    deps['chainalytic'] +
    deps['test']
)

install_requires = deps['chainalytic']
extra_requires = deps
test_requires = deps['test']

with open('README.adoc') as readme_file:
    long_description = readme_file.read()

setup(
    name='chainalytic',
    version='0.0.1',
    description='Data transform & aggregation framework for blockchain analytic',
    long_description=long_description,
    long_description_content_type='text/asciidoc',
    author='duyyudus - Yudus Lab',
    author_email='duyyudus@gmail.com',
    url='https://github.com/yudus-lab/chainalytic-framework',
    include_package_data=True,

    tests_require=test_requires,
    install_requires=install_requires,
    extras_require=extra_requires,

    license='MIT',
    zip_safe=False,
    keywords='blockchain data aggregation and analytic',
    python_requires='>=3.7',

    packages=find_packages(
        where='src',
        exclude=['tests', 'tests.*', '__pycache__', '*.pyc']
    ),
    package_dir={
        '': 'src',
    },
    package_data={
        '': ['**/*.yml']
    },

    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.7',
        'Operating System :: POSIX :: Linux'
    ],
)
