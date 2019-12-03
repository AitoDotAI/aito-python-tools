from setuptools import setup, find_packages
import os

install_requires = [
    'python-dotenv',
    'requests',
    'aiohttp',
    'pandas',
    'ndjson',
    'xlrd',
    'langdetect',
    'argcomplete'
]

if os.environ.get('CONVERT_README'):
    import pypandoc
    long_description = pypandoc.convert('README.md', 'rst')
else:
    long_description = ''

VERSION = "0.0.3.dev2"

setup(
    name='aitoai',
    version=VERSION,
    author='aito.ai',
    author_email='admin@aito.ai',
    description='A collection of python support tools and scripts for Aito.ai',
    long_description=long_description,
    url='https://github.com/AitoDotAI/aito-python-tools',
    packages=find_packages(exclude=['tests', 'tests.*']),
    install_requires=install_requires,
    classifiers=[
        "Programming Language :: Python :: 3.6",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    entry_points={'console_scripts': ['aito = aito.cli.main_parser:main']}
)
