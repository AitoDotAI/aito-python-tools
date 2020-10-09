.. _install:

Installation
============

Install with `pip <https://pip.pypa.io/en/stable/>`_:

To install the latest stable version with pip, run::

  pip install aitoai

To install this version with pip, run:

.. parsed-literal::

  pip install aitoai==\ |release|\

.. note::
  Aitoai is only available in Python version 3.6 or higher.

Development Version
--------------------
Aito Python SDK is actively developed on GitHub, where the code is `always available
<https://github.com/AitoDotAI/aito-python-tools>`_.
If you want to work with the development version, clone the git repo and run it in the development mode.

.. code-block:: console

  $ git clone https://github.com/AitoDotAI/aito-python-tools.git
  ...

  $ cd aito-python-tools
  $ python3 -m venv venv
  ...

  $ . venv/bin/activate
  $ pip install .
  ...
  Successfully installed ...


To get the development version without git, do this instead:

.. code-block:: console

  $ mkdir aito-python-tools
  $ cd aito-python-tools
  $ python3 -m venv venv
  $ . venv/bin/activate
  $ pip install git+https://github.com/AitoDotAI/aito-python-tools.git
  ...

And you're done!