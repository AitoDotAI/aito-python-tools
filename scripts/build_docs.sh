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
    exit 3
  fi
}

function sphinx_build_html {
  cd docs || return
  make clean html
  cd ..
  mv docs/build/html .
}

function clean_and_update_gh_pages_branch {
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

check_git_tree
check_at_top
sphinx_build_html
clean_and_update_gh_pages_branch
git add .
git commit -m "publish doc $VERSION"
git push
exit $!
