Aito Python SDK
===============

|PyPI| |PyPI version|

Aito Python SDK is an useful library for Aito_ users containing.

The SDK also provides the Aito Command Line Interface.

Installation
------------

To install with pip, run: ``pip install aitoai``

For more installation option, please check :doc:`install`

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

**NOTE:** For the database action, remember to set up your Aito instance
credentials.

For an additional guide of the CLI tool, see the `CLI documentations`_

Using the Python SDK
~~~~~~~~~~~~~~~~~~~~

Most used features:

1. Infer Aito Schema from a Pandas DataFrame
2. Create a table
3. Upload data to a table

.. toctree::
   :hidden:

   install
   cli/cli

.. note::
   The Aito Python SDK is an experimental project that we are already using ourselves.
   It might be a bit rough around the edges and is not yet ready for production grade.
   This project is under constant development and is likely to be broken in the future.
   Feel free to use, and share any feedback with us via our `Slack channel`_ or
   our `Issue tracker`_

.. _Aito: https://aito.ai/
.. _CLI documentations: docs/cli.md
.. _Slack channel: https://aito.ai/join-slack
.. _Issue tracker: https://github.com/AitoDotAI/aito-python-tools/issues

.. |PyPI| image:: https://img.shields.io/pypi/v/aitoai?style=plastic
   :target: https://pypi.org/project/aitoai/
.. |PyPI version| image:: https://img.shields.io/pypi/pyversions/eve.svg?style=plastic
   :target: https://github.com/AitoDotAI/aito-python-tools
