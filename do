#!/bin/bash

function usage {
  echo "usage: $(basename "$0") [options] [command]

options:
  -v

  verbose mode

commands:
  -h, --help

  aito
    run aito-cli

  test
    run unittests

  release-test
    release to testpypi (change VERSION in setup.py first)

  release
    release to pypi (change VERSION in setup.py first)

"
}

function do-aito {
  python3 -m aito.cli.main_parser $@
}

function do-test {
  python3 -m tests.test_parser all
}

function prepare-release-tools {
  echo "installing pandoc and pypandoc to convert markdown to rst"
  sudo apt-get install pandoc
  python3 -m pip install --user pypandoc
  export CONVERT_README="TRUE"

  echo "installing twine to release package"
  python3 -m pip install --user --upgrade twine
}

function build-package {
  python3 -m pip install --user --upgrade setuptools wheel
  python3 setup.py sdist bdist_wheel
}

function git-bump-version {
  if [[ $(git status --porcelain) ]]
  then
    echo "tree is dirty, please commit changes before release"
    exit 1
  fi
  if [[ "$1" == "" ]]
  then
    echo
    echo "error: no release version"
    echo
    usage
    exit 1
  fi

  NEW_VERSION=$1
  sed -i -e "s/^VERSION *=.*/VERSION = \"$NEW_VERSION\"/" setup.py
  git commit -am "Bump to $VERSION"
  git tag $VERSION
  git push
  git push --tags
}

function do-release-test {
  git-bump-version $1
  prepare-release-tools
  build-package
  twine check dist/* && python3 -m twine upload --repository-url https://test.pypi.org/legacy/ dist/*
}

function do-release {
  git-bump-version $1
  prepare-release-tools
  build-package
  twine check dist/* && python3 -m twine upload https://test.pypi.org/legacy/ dist/*
}

if [[ "$1" == "" ]]
then
  echo
  echo "error: no command"
  echo
  usage
  exit 1
fi

_cmd=$1
shift

case $_cmd in
  aito)
    do-aito $@
    ;;
  test)
    do-test $@
    ;;
  release-test)
    do-release-test $@
    ;;
  release)
    do-release $@
    ;;
  -h|--help)
    usage
    ;;
  *)
      echo
      echo "error: unknown command '$_cmd'"
      echo
      usage
      exit 1
      ;;
esac

exit $!
