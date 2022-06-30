# This file will add the necessary precommit files to enable the autogeneration of the DBT data dictionary

# How to create symlink between directories
# mkdir .githooks || echo "Directory .githooks already exists"

directory=".git/hooks"
# [ -d $directory ] && rm -r $directory/* || mkdir $directory && echo "created directory"
if [ -d $directory ]
then
    rm -r $directory/*
else
    mkdir $directory
    echo "created directory"
fi

# Uncomment next line if able to figure out symlink
# ln -s .githooks .git/hooks/ && echo "created link" || echo "failed to link"

pip3 install pyyaml

cd .git/hooks && wget https://raw.githubusercontent.com/cyoung43/airflow-pipeline/master/.githooks/generate_dbt_data_dictionary.py
wget https://raw.githubusercontent.com/cyoung43/airflow-pipeline/master/.githooks/pre-commit

chmod 777 pre-commit
chmod 777 generate_dbt_data_dictionary.py

echo "Setup complete"