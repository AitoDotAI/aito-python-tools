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
  usage: aito [-h] <action> ...

  optional arguments:
    -h, --help          show this help message and exit

  action:
    action to perform

    <action>
      infer-table-schema
                        infer an Aito table schema from a file
      convert           convert a file into ndjson|json format
      database          perform operations with your Aito database instance

.. note::

  For the database action, remember to :ref:`set up your Aito instance credentials<cliSetUpAitoCredentials>`.

For an additional guide of the CLI tool, see the :doc:`CLI documentations <cli>`.

Using the Python SDK
~~~~~~~~~~~~~~~~~~~~

Some common features:

1. :ref:`sdkInferTableSchema`
2. :ref:`sdkCreateTable`
3. :ref:`sdkUploadData`
4. :ref:`sdkExecuteQuery`

.. toctree::
  :hidden:

  install
  quickstart
  cli
  sdk
  api
  sql
  changelog

.. note::

  The Aito Python SDK is an experimental project that we are already using ourselves.
  It might be a bit rough around the edges and is not yet production-grade.
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
