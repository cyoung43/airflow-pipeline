# This file will add the necessary precommit files to enable the autogeneration of the DBT data dictionary

mkdir .githooks || echo "Directory .githooks already exists"

directory=".git/hooks"
[ -d $directory ] && echo "directory exists" && rm -r $directory/* || mkdir $directory && echo "created directory"

ln -s .githooks $directory && echo "created link" || echo "failed to link"

pip3 install pyyaml
