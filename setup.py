from setuptools import setup, find_packages
import os

install_requires = [
    'pandas == 0.25.3; python_full_version<="3.6.0"',
    'pandas == 1.0.0; python_full_version>"3.6.0"',
    'python-dotenv == 0.11.0',
    'requests == 2.22.0',
    'aiohttp == 3.6.1; python_version >= "3.8"',
    'aiohttp == 3.6.0; python_version < "3.8"',
    'ndjson == 0.2.0',
    'langdetect == 1.0.7',
    'argcomplete == 1.11.1',
    'xlrd == 1.1.0'
]

default_description = "Please go to our Homepage at https://github.com/AitoDotAI/aito-python-tools " \
                      "for more detailed documentation.\n"
if os.environ.get('CONVERT_README'):
    import pypandoc
    long_description = default_description + pypandoc.convert('README.md', 'rst')
else:
    long_description = default_description

VERSION = "0.1.2"

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
    extra_requires={},
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    python_requires='>=3.6',
    entry_points={'console_scripts': ['aito = aito.cli.main_parser_wrapper:main']}
)
