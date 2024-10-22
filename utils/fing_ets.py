"""
Extracts, Transforms, and Saves data into csv files; preprocessing for training and data analysis
"""
import subprocess
import sys

# Install cbor2 if not already installed
try:
    import cbor2
    print("cbor2 is already installed.")
    print()
except ImportError:
    print("cbor2 not found, installing...")
    print()
    subprocess.check_call([sys.executable, "-m", "pip", "install", "cbor2"])


import argparse
import os
import json
import pandas as pd

def load_data(main_data_dir, training=True):
    """
    Loads data from cbor/json to csv
    """
    if training:
        data_dir = main_data_dir + "/training"
    else:
        data_dir = main_data_dir + "/testing"

    data_files = os.listdir(data_dir)
    data_dict = {"json":[], "cbor":[]}

    for file_name in data_files:

        if file_name.endswith(".cbor"):
            file_path = os.path.join(data_dir, file_name)

            with open(file_path, 'rb') as cbor_file:
                cbor_data = cbor2.load(cbor_file)
                data_dict["cbor"].append(cbor_data)

        if file_name.endswith('.json'):
            file_path = os.path.join(data_dir, file_name)

            with open(file_path, 'rb') as json_file:
                json_data = json.load(json_file)
                data_dict["json"].append(json_data)

    return data_dict


def __process_cbor_data(data_dict, save_path, save=True):
    """
    Helper function for loading cbor data into csv
    """
    sensor_info_list = data_dict["cbor"][0]["payload"]["sensors"]
    sensor_data_features = [sensor_info_list[i]["name"] for i in range(len(sensor_info_list))]

    sensor_data_df = {"cbor_file": [], "ms":[]}
    for feature in sensor_data_features:
        sensor_data_df[feature] = []


    for cbor_file_num in range(1, len(data_dict["cbor"])):
        sensor_data_values = data_dict["cbor"][cbor_file_num]["payload"]["values"]

        time_ms = 0
        for value in sensor_data_values:
            sensor_data_df["cbor_file"].append(cbor_file_num)
            sensor_data_df["ms"].append(time_ms)
            time_ms += 16

            for feature_ind, feature in enumerate(sensor_data_features):
                sensor_data_df[feature].append(value[feature_ind])

    sensor_data_df = pd.DataFrame.from_dict(sensor_data_df)

    print("cbor data was processed successfully")
    print()

    if save:
        __save_df_to_csv(sensor_data_df, save_path)



    return sensor_data_df

def __process_json_data(data_dict, save_path, save=True):
    """
    Helper function for loading json data into csv
    """
    sensor_info_list = data_dict["json"][0]["payload"]["sensors"]
    sensor_data_features = [sensor_info_list[i]["name"] for i in range(len(sensor_info_list))]

    sensor_data_df = {"json_file": [], "ms":[]}
    for feature in sensor_data_features:
        sensor_data_df[feature] = []

    for json_file_num in range(1, len(data_dict["json"])):
        sensor_data_values = data_dict["json"][json_file_num]["payload"]["values"]

        time_ms = 0
        for value in sensor_data_values:
            sensor_data_df["json_file"].append(json_file_num)
            sensor_data_df["ms"].append(time_ms)
            time_ms += 16

            for feature_ind, feature in enumerate(sensor_data_features):
                sensor_data_df[feature].append(value[feature_ind])

    sensor_data_df = pd.DataFrame.from_dict(sensor_data_df)

    print("json data was processed successfully")
    print()

    if save:
        __save_df_to_csv(sensor_data_df, save_path, cbor=False)

def __save_df_to_csv(df, save_path, cbor=True):
    if not os.path.exists(save_path):
        os.makedirs(save_path)
        print(f"Creating {save_path}")
        print()

    if cbor:
        save_path += "data_cbor.csv"
    else:
        save_path += "data_json.csv"


    df.to_csv(save_path)
    print(f"dataframe saved to {save_path}")
    print()





if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-path_to_data', '--main_data_directory',
                        type=str, required=True, help='Main path to data')
    parser.add_argument('-save_path', '--save_path', type=str, required=False, help='Save path')
    args = parser.parse_args()

    input_dir = os.path.basename(args.main_data_directory)

    combined_data_dict = load_data(input_dir)

    if not args.save_path:
        args.save_path = "./"

    # Building csv for cbor and json data
    if combined_data_dict["cbor"]:
        __process_cbor_data(combined_data_dict, save_path=args.save_path)

    if combined_data_dict["json"]:
        __process_json_data(combined_data_dict, save_path=args.save_path)
