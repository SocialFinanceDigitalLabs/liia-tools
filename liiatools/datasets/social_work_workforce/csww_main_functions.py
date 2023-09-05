from liiatools.datasets.social_work_workforce.lds_csww_data_generator.sample_data import (
    generate_sample_csww_file,
)
from liiatools.datasets.social_work_workforce.lds_csww_data_generator.stream import (
    consume,
)
from liiatools.datasets.social_work_workforce.lds_csww_clean.parse import etree, to_xml


def generate_sample(output: str):
    """
    Export a sample file for testing

    :param output: string containing the desired location and name of sample file
    :return: .xml sample file in desired location
    """

    stream = generate_sample_csww_file()
    builder = etree.TreeBuilder()
    stream = to_xml(stream, builder)
    consume(stream)

    element = builder.close()
    element = etree.tostring(element, encoding="utf-8", pretty_print=True)
    try:
        with open(output, "wb") as FILE:
            FILE.write(element)
    except FileNotFoundError:
        print("The file path provided does not exist")
