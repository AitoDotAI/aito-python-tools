from setuptools import setup, find_packages

install_requires = []

setup(
    name='aitoai',
    version='0.0.1',
    author='aito.ai',
    author_email='support@aito.ai',
    description='A collection of python support tools and scripts for Aito.ai',
    url='https://github.com/AitoDotAI/aito-python-tools',
    packages=find_packages(exclude=['tests', 'tests.*']),
    install_requires=[
        'python-dotenv',
        'requests',
        'aiohttp',
        'pandas',
        'ndjson',
        'xlrd',
        'langdetect'
    ],
    classifiers=[
        "Programming Language :: Python :: 3.6",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    entry_points={'console_scripts': ['aito = aito.cli.main_parser:main']}
)
