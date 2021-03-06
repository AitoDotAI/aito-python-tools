# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys
sys.path.insert(0, os.path.abspath('../..'))


# -- Project information -----------------------------------------------------

project = 'aito-python-sdk'
copyright = '2020, aito.ai'
author = 'aito.ai'

# The full version, including alpha/beta/rc tags
release = __import__('aito').__version__
version = release.split(".dev")[0]


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.

SPHINX_DISABLE_MOCK_REQUIREMENTS = os.environ.get('SPHINX_DISABLE_MOCK_REQUIREMENTS', False)

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.githubpages',
    'sphinx.ext.autosummary',
    'sphinx.ext.doctest',
]
autodoc_mock_imports = [] if SPHINX_DISABLE_MOCK_REQUIREMENTS else [
    'pandas', 'dotenv', 'requests', 'aiohttp', 'ndjson', 'langdetect', 'argcomplete', 'xlrd', 'numpy', 'pyodbc',
    'jsonschema'
]
autodoc_default_flags = [
    "members",
    "show-inheritence",
    "inherited-members"
]
autoclass_content = "both"
autosummary_generate = True

# disable testing inline docstring (already covered in tests/inline_docs)
doctest_test_doctest_blocks = ''

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'sphinx_rtd_theme'

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']
html_logo = "_static/aito.svg"
github_url = "https://github.com/AitoDotAI/aito-python-tools"
