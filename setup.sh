# This file will add the necessary precommit files to enable the autogeneration of the DBT data dictionary

mkdir .githooks || echo "Directory .githooks already exists"

directory=".git/hooks"
# [ -d $directory ] && rm -r $directory/* || mkdir $directory && echo "created directory"
if [ -d $directory ]
then
    rm -r $directory/*
else
    mkdir $directory
    echo "created directory"
fi

ln -s .githooks .git/hooks/ && echo "created link" || echo "failed to link"

pip3 install pyyaml

cd .githooks && wget https://raw.githubusercontent.com/cyoung43/airflow-pipeline/master/.githooks/generate_dbt_data_dictionary.py
wget https://raw.githubusercontent.com/cyoung43/airflow-pipeline/master/.githooks/pre-commit

echo "Setup complete"