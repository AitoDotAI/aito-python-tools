#!/bin/bash -e

function usage {
  echo "usage: $(basename "$0") <verion>

version:
  document version number
"
}

function check_git_tree {
  if [[ $(git status --porcelain) ]]
  then
    echo "tree is dirty, please commit changes before build docs"
    exit 1
  fi
}

function check_at_top {
  if [[ ! -f aito/__init__.py ]]; then
    echo "this script must be run from the project directory"
    exit 1
  fi
}

function clean_and_update_gh_pages_branch {
  if [[ "$(ls -A docs/build/html)" ]]
  then
    mv docs/build/html .
  else
    echo "built html folder is empty or not exist"
    exit 1
  fi
  git checkout gh-pages
  find . -maxdepth 1 -not -name '.git' -not -name 'html' -exec rm -rf {} \;
  mv html/* .
  touch .nojekyll
  rm -r html
}
if [[ "$1" == "" ]]
then
  echo
  echo "error: specify document version"
  echo
  usage
  exit 1
else
  VERSION=$1
fi

check_at_top
check_git_tree
clean_and_update_gh_pages_branch
git add .
git commit -m "publish doc $VERSION"
git push
exit $!
