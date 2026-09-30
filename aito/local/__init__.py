"""Run and reach a local Aito: ``aito serve`` and the profile store behind ``aito.Client()``

Standard library only. ``aito serve``, ``status``, ``logs``, ``stop``, ``keys`` and
``upgrade`` work on a bare ``pip install aitoai`` (no ``[cli]`` extra), and resolving
credentials from a profile adds nothing to ``import aito.v2``.

The design is in ``docs/adr/0001-local-server-cli.md``.
"""
