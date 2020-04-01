from setuptools import setup, find_packages
import os
import re
from pathlib import Path

PROJECT_ROOT_PATH = Path(__file__).parent
VERSION_FILE_PATH = PROJECT_ROOT_PATH / 'aito' / '__init__.py'
REQUIREMENTS_FILE_PATH = PROJECT_ROOT_PATH / 'requirements.txt'


def find_current_version(version_file_path: Path) -> str:
    with version_file_path.open() as f:
        init_content = f.read()

    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]", init_content, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")


def fetch_requirements(requirements_file_path: Path):
    requirements = []
    with requirements_file_path.open() as in_f:
        for line in in_f:
            requirements.append(line.strip())
    return requirements

default_description = "Please go to our Homepage at https://github.com/AitoDotAI/aito-python-tools " \
                      "for more detailed documentation.\n"
if os.environ.get('CONVERT_README'):
    import pypandoc
    long_description = default_description + pypandoc.convert('README.md', 'rst')
else:
    long_description = default_description

setup(
    name='aitoai',
    version=find_current_version(VERSION_FILE_PATH),
    author='aito.ai',
    author_email='admin@aito.ai',
    description='A collection of python support tools and scripts for Aito.ai',
    long_description=long_description,
    url='https://github.com/AitoDotAI/aito-python-tools',
    packages=find_packages(exclude=['tests', 'tests.*']),
    install_requires=fetch_requirements(REQUIREMENTS_FILE_PATH),
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
