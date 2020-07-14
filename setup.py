import re
from pathlib import Path

from setuptools import setup, find_packages

PROJECT_ROOT_PATH = Path(__file__).parent
VERSION_FILE_PATH = PROJECT_ROOT_PATH / 'aito' / '__init__.py'
REQUIREMENTS_FILE_PATH = PROJECT_ROOT_PATH / 'requirements' / 'build.txt'


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


with (PROJECT_ROOT_PATH / 'README.rst').open() as f:
    long_description = f.read()

setup(
    name='aitoai',
    version=find_current_version(VERSION_FILE_PATH),
    author='aito.ai',
    author_email='admin@aito.ai',
    description='Aito.ai Python SDK',
    long_description=long_description,
    long_description_content_type="text/x-rst",
    url='https://github.com/AitoDotAI/aito-python-tools',
    project_urls={
        'Documentation': 'https://aito-python-sdk.readthedocs.io/en/latest/',
        'Source': 'https://github.com/AitoDotAI/aito-python-tools',
        'Tracker': 'https://github.com/AitoDotAI/aito-python-tools/issues',
    },
    packages=find_packages(exclude=['tests', 'tests.*']),
    install_requires=fetch_requirements(REQUIREMENTS_FILE_PATH),
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: MIT License',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX',
        'Operating System :: POSIX :: BSD',
        'Operating System :: POSIX :: Linux',
        'Operating System :: Microsoft :: Windows',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    python_requires='>=3.6',
    entry_points={
        'console_scripts': [
            'aito = aito.cli.main_parser:main'
        ]
    }
)
