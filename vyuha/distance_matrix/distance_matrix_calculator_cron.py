from vyuha.distance_matrix.calculate_distance_matrix import *
import argparse

msg = "Adding description" 
# Initialize parser
parser = argparse.ArgumentParser(description=msg)
parser.add_argument("-o", "--Output", help = "Output file name")
parser.add_argument("-u", "--Unitname", help = "Unit Name")
args = parser.parse_args()

output_filename = args.Output
unit_name = args.Unitname

def calculate(output_filename, unit_name):
    input_file = 'vyuha/distance_matrix/input_files/{}'.format(output_filename)
    df = pd.read_csv(input_file)

    dist_matrix = distance_matrix()
    result = dist_matrix.distance(df, unit_name)