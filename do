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

  build-package
    generate distribution package

  publish-test
    publish to testpypi.org

  publish
    publish to pypi.org

"
}

function do-test {
  python3 -m tests.test_parser all
}

function do-build-package {
  python3 -m pip install --user --upgrade setuptools wheel
  python3 setup.py sdist bdist_wheel
}

function do-publish-test {
  export CONVERT_README="TRUE"
  echo "install pandoc"
  sudo apt-get install pandoc
  python3 -m pip install --user pypandoc
  do-build-package
  python3 -m pip install --user --upgrade twine
  twine check dist/*
  python3 -m twine upload --repository-url https://test.pypi.org/legacy/ dist/*
}

function do-publish {
  export CONVERT_README="TRUE"
  echo "install pandoc"
  sudo apt-get install pandoc
  python3 -m pip install --user pypandoc
  do-build-package
  python3 -m pip install --user --upgrade twine
  twine check dist/*
  python3 -m twine upload https://test.pypi.org/legacy/ dist/*
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
  test)
    do-test $@
    ;;
  build-package)
    do-build-package $@
    ;;
  publish-test)
    do-publish-test $@
    ;;
  publish)
    do-publish $@
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