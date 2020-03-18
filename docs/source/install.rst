.. _install:

Installation
============

Install with `pip <https://pip.pypa.io/en/stable/>`_:

.. code-block:: console

  $ pip install aitoai

Development Version
--------------------
Aito Python SDK is actively developed on GitHub, where the code is `always available
<https://github.com/AitoDotAI/aito-python-tools>`_.
If you want to work with the development version, clone the git repo and run in development mode.

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


To just get the development version without git, do this instead:

.. code-block:: console

  $ mkdir aito-python-tools
  $ cd aito-python-tools
  $ python3 -m venv venv
  $ . venv/bin/activate
  $ pip install git+https://github.com/AitoDotAI/aito-python-tools.git
  ...

And you're done!