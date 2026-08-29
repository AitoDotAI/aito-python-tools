# Minimal makefile for Sphinx documentation
#

# You can set these variables from the command line, and also
# from the environment for the first two.
SPHINXOPTS    ?=
SPHINXBUILD   ?= sphinx-build
SOURCEDIR     = source
BUILDDIR      = build

# Put it first so that "make" without argument is like "make help".
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

# `clean` has to remove the autosummary stubs too. autosummary_generate writes
# them into the SOURCE tree ($(SOURCEDIR)/api_stubs, gitignored), which
# `sphinx-build -M clean` never touches because it only empties $(BUILDDIR).
# A stub for a class that has since been renamed or removed therefore survives,
# and the next build fails under -W with "autodoc: failed to import" plus
# "document isn't included in any toctree". It only happens on a machine that
# has built the docs before, never on a fresh clone or in CI, which is what
# made it hard to see.
clean:
	rm -rf "$(SOURCEDIR)/api_stubs"
	@$(SPHINXBUILD) -M clean "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

.PHONY: help clean Makefile

# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
%: Makefile
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)
