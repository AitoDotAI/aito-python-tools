"""Run and reach a local Aito: ``aito start`` and the profile store behind ``aito.Client()``

Standard library only. ``aito start``, ``stop``, ``status``, ``logs``, ``keys`` and
``upgrade`` work on a bare ``pip install aitoai`` (no ``[cli]`` extra), and resolving
credentials from a profile adds nothing to ``import aito.v2``.

The design is in ``docs/adr/0001-local-server-cli.md``.
"""
