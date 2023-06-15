from liiatools.datasets.s251 import (
    file_creator,
)
from liiatools.datasets.s251.data_generator import sample_data


def generate_sample(output):
    """
    Export a sample file for testing

    :param output: string containing the desired location and name of sample file
    :return: .csv sample file in desired location
    """
    stream = sample_data.generate_sample_s251_file()
    stream = file_creator.save_stream(stream, output=output)
    list(stream)
