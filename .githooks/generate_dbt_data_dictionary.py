#!/usr/bin/env python

# This Q&D script scans through SQL files in the models directory and outputs a yml doc with per-model columns, tables, descriptions, and tags.
# See docstring for generate_dbt_schema
# Meant to be run from repository root
# Special comment structure (below) used to recognize column & table descriptions
    # Table Descriptions: '--@ ' (must start on far left of script/code-line, All rows are combined into table description)
    # Table Tags: '--@tag ' (must start on far left of script/code-line; multiple entries accepted. Contents: comma-separated list of tags. All lines are combined into table tags)
    # Column Descriptions: '--@col ' (must be on the same line as the 'AS Col_name' resides)
    # Column Tags: '--@tag' (must start AFTER the --@col description)
    # All: Cannot start with a single or double quote (but can contain it). Ex. <'Account' or 'Lead' are the values> will not work, but <Values can be 'Account' or 'Lead'> will work

import os
from os import path
import re
import subprocess
import time
import yaml
import csv
from pprint import pprint as pp

class DbtDataDictionary:

    def __init__(self, models_dir):
        self.build_model(models_dir)
        pass

    def build_model(self, models_dir):
        auto_schema = {
            "version": 2,
            "quoting": None,
            "database": False,
            "schema": False,
            "identifier": False,
            "models": []

        }


        for dir, subdirs, all_files in os.walk(models_dir):
            sql_files = [file for file in all_files if file.lower().endswith('.sql')]
            for sql_file in sql_files:
                # iterate over the file
                # save all rows that look like a block comment
                # save all rows that look like a column comment
                # save all table reference rows
                # these should never happen overlapping, which makes this easy...

                file_path = path.join(dir, sql_file)
                print(f"Parsing: {file_path}")

                with open(file_path, 'r') as infile:
                    # header
                    model_name = sql_file[:len(sql_file)-4]

                    model = {"name": model_name}

                    # print(f"Model Name: {model_name}")

                    table_comments = []
                    table_tags = []
                    column_comments = []
                    table_refs = []

                    for line in infile:
                        line = line.strip()
                        #print(line)
                        
                        # Table comments and tags
                        if line.startswith('--@tag'):
                            table_tags.append(line[6:].strip())
                        elif line.startswith('--@'):
                            table_comments.append(line[3:].strip())
                        elif line.startswith('-- '):
                            # if whole line is commented out, ignore the line
                            continue
                        elif line.startswith('{#'):
                            # ignore line if jinja comment is present
                            continue

                        # column comments
                        # <from_col>(::<cast>) (AS <to_col>)(,) -- @col <comment>  
                        # using an iterative regex strategy because I'm not smart enough to write it 
                        # all at once...
                        col = re.search(r'[\s,]*(.+?)[\s,]*--\s*@col\s*((?:(?!--@tag).)*)(?:--@tag\s*(.*))?', line)
                        if col:
                            sql = col.group(1)
                            comment = col.group(2)
                            tags = col.group(3).split(',') if col.group(3) else []
                            rename = re.search(r'^(.*?)\s+(?:AS|as)?\s*(\w+)$', sql)
                            if rename:
                                src = rename.group(1)
                                target = rename.group(2)
                            else:
                                src = sql
                                target = None
                            cast = re.search(r'(.*)::(.*)', src)
                            if cast:
                                src = cast.group(1)
                                cast = cast.group(2)
                            if len(src) >= 30:
                                src = "_complex_"
                            column_comments.append({"from_col": src,
                                "cast": cast,
                                "to_col": target,
                                "comment": comment,
                                "tags": tags})

                        # table refs
                        res = re.search(r"{{\s+ref\('(\w+)'\)\s+}}", line)
                        if res:
                            table_refs.append(res.group(1))

                    model["description"] = '\n'.join(table_comments)
                    model["tags"] = ','.join(table_tags)

                    model["columns"] = []

                    # f.write("\n  columns:")
                    for cc in column_comments:
                        col_name = cc.get('to_col', None) or cc.get('from_col', None)
                        col_comment = cc.get('comment', None)
                        col_tags = cc.get('tags', None)
                        

                        column = {"name": col_name, "description": col_comment, "tags": col_tags}
                        model["columns"].append(column)
                        
                    auto_schema["models"].append(model)
        self.auto_schema = auto_schema
        return auto_schema

    def flatten_model(self, model):
        output=[]

        for table in model["models"]:
            table_name = table["name"]
            table_description = table["description"]
            for column in table["columns"]:
                column_name = column["name"]
                column_description = column["description"]

                if column["tags"]:
                    for tag in column["tags"]:
                        output.append({
                            "table_name": table_name, 
                            "table_description": table_description, 
                            "column_name": column_name, 
                            "column_description": column_description,
                            "tag": tag
                            })

                else:
                    output.append({
                        "table_name": table_name, 
                        "table_description": table_description, 
                        "column_name": column_name, 
                        "column_description": column_description
                        })
        return output


    def generate_dbt_schema(self, schema_file_path):
        '''

        '''
        with open(schema_file_path, "w") as f:
            yaml.dump(self.auto_schema, f, allow_unicode=True, sort_keys=False)
        print(f"Generated file {schema_file_path} at {time.ctime()}")
        
        subprocess.check_output(["git", "add", schema_file_path]) # Stage the file
        print(f"Staged file {schema_file_path} at {time.ctime()}")


    def generate_dbt_seed(self, seed_file_path):
        '''
        
        '''
        flat_model = self.flatten_model(self.auto_schema)
        keys = sorted({k for d in flat_model for k in d.keys()}) # Get all columns in flatted list

        with open(seed_file_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, keys)
            writer.writeheader()
            writer.writerows(flat_model)
        print(f"Generated file {seed_file_path} at {time.ctime()}")

        subprocess.check_output(["git", "add", seed_file_path]) # Stage the file
        print(f"Staged file {seed_file_path} at {time.ctime()}")

