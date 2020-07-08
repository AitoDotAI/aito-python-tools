Aito Python SDK
===============

Version |release|

|PyPI| |PyPI version|


The Aito Python SDK is an open-source library that helps you to integrate your Python application
to Aito_ quicker and more efficiently.

The SDK also includes the :doc:`Aito Command Line Interface (CLI) <cli>` that enables you to interact with Aito
using commands in your command-line shell, e.g: infer a table schema from a file or upload a file to Aito.

Quick Installation
------------------

To install with pip, run: ``pip install aitoai``

For more installation options, please check :doc:`install`

Basic Usage
-----------

Aito Command Line Interface
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: console

  $ aito -h
    usage: aito [-h] [-V] [-v] [-q]

    optional arguments:
      -h, --help     show this help message and exit
      -V, --version  display the version of this tool
      -v, --verbose  display verbose messages
      -q, --quiet    display only error messages

    To see all available commands, you can run:
      aito list

    To see the help text, you can run:
      aito -h
      aito <command> -h
      aito <command> <subcommand> -h

.. note::

  For the database action, remember to :ref:`set up your Aito instance credentials<cliSetUpAitoCredentials>`.

For an additional guide of the CLI tool, see the :doc:`CLI documentations <cli>`.

.. toctree::
  :hidden:
  :caption: Getting started
  :name: sec-getting-started

  install
  quickstart

.. toctree::
  :hidden:
  :caption: Tutorials

  cli
  sdk
  sql

Aito SDK Overview
~~~~~~~~~~~~~~~~~

.. currentmodule:: aito
.. autosummary::
  :toctree: api_stubs
  :caption: API Documentation

  schema
  client
  utils.data_frame_handler
  utils.sql_connection

.. toctree::
  :hidden:
  :caption: Additional Materials

  changelog
  community


.. note::

  The Aito Python SDK is in the beta phase and we are already using it ourselves.
  This project is under constant development and is subject to change.
  Feel free to use the Aito SDK, and share any feedback with us via our `Slack channel`_ or
  our `Issue tracker`_


.. _Aito: https://aito.ai/
.. _Slack channel: https://aito.ai/join-slack
.. _Issue tracker: https://github.com/AitoDotAI/aito-python-tools/issues

.. |PyPI| image:: https://img.shields.io/pypi/v/aitoai?style=plastic
  :target: https://pypi.org/project/aitoai/
.. |PyPI version| image:: https://img.shields.io/pypi/pyversions/aitoai?style=plastic
  :target: https://github.com/AitoDotAI/aito-python-tools
