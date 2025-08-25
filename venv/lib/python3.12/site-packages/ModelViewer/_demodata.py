import pkg_resources
import pandas as pd

def get_taxi_data():
    data_path = pkg_resources.resource_filename(__name__, "data/taxi_full_metadata_and_scatterplot_coordinates.csv")
    dataframe = pd.read_csv(data_path,index_col=False)
    return dataframe