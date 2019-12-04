#!/bin/bash

function usage {
  echo "usage: $(basename "$0") <location> <version>

location:
  test
    release to testpypi

  prod
    release to pypi

version:
  a version number
"
}

function check_git_tree {
  if [[ $(git status --porcelain) ]]
  then
    echo "tree is dirty, please commit changes before release"
    exit 1
  fi
}

function version_gt {
  test "$(printf '%s\n' "$@" | sort -V | head -n 1)" != "$1";
}

function bump_version_setup {
  old_version=$(sed -n 's/^VERSION *= \"\(.*\)\"/\1/p' setup.py)

  if [[ $NEW_VERSION == $old_version ]]
  then
    echo
    echo "error: new version $NEW_VERSION must be different from old_version $old_version"
    echo
    exit 1
  fi

  if version_gt $old_version $NEW_VERSION;
  then
    echo
    echo "error: new version $NEW_VERSION is smaller than old_version $old_version"
    echo
    exit 1
  fi

  sed -i -e "s/^VERSION *=.*/VERSION = \"$NEW_VERSION\"/" setup.py
}

function check_version_doc_change_logs {
  if ! grep -q "^#### $NEW_VERSION" docs/change_logs.md ;
  then
    echo
    echo "error: version $NEW_VERSION change log not found in docs/change_logs.md"
    echo
    exit 1
  fi
}

function git_bump_prod_version {
  git commit -am "Bump to $NEW_VERSION"
  git tag $NEW_VERSION
  git push
  git push --tags
}

function prepare_release_tools {
  echo "installing pandoc and pypandoc to convert markdown to rst"
  sudo apt-get install pandoc
  python3 -m pip install --user --upgrade pypandoc
  export CONVERT_README='true'

  echo "installing twine to release package"
  python3 -m pip install --user --upgrade twine
}

function build_package {
  python3 -m pip install --user --upgrade setuptools wheel
  python3 setup.py sdist bdist_wheel
}

if [[ "$1" == "" ]]
then
  echo
  echo "error: specify release to test or prod"
  echo
  usage
  exit 1
fi

NEW_VERSION=$2
if [[ "$NEW_VERSION" == "" ]]
then
  echo
  echo "error: missing release version"
  echo
  usage
  exit 1
fi

check_git_tree
bump_version_setup

if [[ "$1" == "prod" ]]
then
  check_version_doc_change_logs
  git_bump_prod_version
fi

prepare_release_tools
build_package

if [[ "$1" == "test" ]]
then
  twine check dist/* && python3 -m twine upload --repository-url https://test.pypi.org/legacy/ dist/*
else
  twine check dist/* && python3 -m twine upload dist/*
fi

exit $!
