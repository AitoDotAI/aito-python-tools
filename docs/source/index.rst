Aito Python SDK
===============

|PyPI| |PyPI version|

Aito Python SDK is an useful library for Aito_ users containing.

The SDK also provides the :doc:`Aito Command Line Interface <cli/cli>`.

Quick Installation
------------------

To install with pip, run: ``pip install aitoai``

For more installation option, please check :doc:`generic/install`

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
  For the database action, remember to :ref:`set up your Aito instance credentials<setUpAitoCredentials>`.

For an additional guide of the CLI tool, see the :doc:`CLI documentations <cli/cli>`.

Using the Python SDK
~~~~~~~~~~~~~~~~~~~~

Some common features:

1. :ref:`sdkInferTableSchema`
2. :ref:`sdkCreateSchema`
3. :ref:`sdkUploadData`

.. toctree::
  :hidden:

  generic/install
  cli/cli
  sdk/quickstart
  sdk/api
  generic/sql
  generic/changelog

.. note::

  The Aito Python SDK is an experimental project that we are already using ourselves.
  It might be a bit rough around the edges and is not yet ready for production grade.
  This project is under constant development and is likely to be broken in the future.
  Feel free to use, and share any feedback with us via our `Slack channel`_ or
  our `Issue tracker`_

.. _Aito: https://aito.ai/
.. _Slack channel: https://aito.ai/join-slack
.. _Issue tracker: https://github.com/AitoDotAI/aito-python-tools/issues

.. |PyPI| image:: https://img.shields.io/pypi/v/aitoai?style=plastic
  :target: https://pypi.org/project/aitoai/
.. |PyPI version| image:: https://img.shields.io/pypi/pyversions/aitoai?style=plastic
  :target: https://github.com/AitoDotAI/aito-python-tools