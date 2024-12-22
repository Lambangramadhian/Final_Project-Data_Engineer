import os
import shutil
import glob

def single_file_output_to_temp(table, name:str):
    temp_dir = "/home/hadoop/final_project/csv_temp"
    os.makedirs(temp_dir, exist_ok=True)
    table_path = os.path.join(temp_dir, name)
    table_csv = os.path.join(temp_dir, name + ".csv")
    table.coalesce(1).write.mode("overwrite").option('header', 'true').csv(table_path)
    table_file = glob.glob(os.path.join(table_path, "part-*.csv"))[0]
    shutil.move(table_file, table_csv)
    shutil.rmtree(table_path)

def single_file_output_to_output(table, name:str):
    temp_dir = "/home/hadoop/final_project/csv_output/eda_output"
    os.makedirs(temp_dir, exist_ok=True)
    table_path = os.path.join(temp_dir, name)
    table_csv = os.path.join(temp_dir, name + ".csv")
    table.coalesce(1).write.mode("overwrite").option('header', 'true').csv(table_path)
    table_file = glob.glob(os.path.join(table_path, "part-*.csv"))[0]
    shutil.move(table_file, table_csv)
    shutil.rmtree(table_path)