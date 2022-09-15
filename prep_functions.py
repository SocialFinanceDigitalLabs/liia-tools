import logging
import pandas as pd
from pathlib import Path
from lxml import etree
import cchardet as chardet


class XML_Validator:

    def __init__(self, xsd_path: str):
        xmlschema_doc = etree.parse(
            xsd_path)
        self.xmlschema = etree.XMLSchema(
            xmlschema_doc)

    def validate(
            self, xml_path: str) -> bool:
        """_summary_ purpose checking for validated issues,
        post download cycle on London Datastore,

        :param xml_path: path to an xml file.
        :type xml_path: str
        :return:
        :rtype: bool
        """
        xml_doc = etree.parse(xml_path)
        result = self.xmlschema.validate(
            xml_doc)

        return result


def file_encoding_audit(
        data_folder: Path, ) -> pd.DataFrame:
    """_summary_

    :param data_folder: _description_
    :type data_folder: Path
    :return: _description_
    :rtype: pd.DataFrame
    """
    # TODO - check csv encoding type of file, save to utf-8
    # TODO - check xml encoding type of file,
    # save as an acceptable format,
    result_out = []
    #
    for cdf in data_folder.glob("**/*"):
        if cdf.is_file() and 'log' not in cdf.root:
            with open(cdf, "rb") as f:
                msg = f.read()
                result = chardet.detect(msg)
                out = (
                    f"{cdf.parts[-3]}, {cdf.stem}, {result}")
                # this is messy.
                outt = out.replace(
                    '}', '').replace('{', '').replace('confidence', '').replace('encoding', '').replace("'':", "")
                result_out.append(outt)
    #
    encoding_series = pd.Series(result_out)
    #
    encoding_df = pd.DataFrame(
        encoding_series,
        columns=['file_name'])
    # split out dataframe
    encoding_df[['local_authority', 'file_name', 'encoding', 'confidence']] = encoding_df.file_name.str.split(
        ',', expand=True)
    # filter out log files and drop high
    # confidence files types,
    encoded_df = encoding_df[~encoding_df["file_name"].str.contains(
        'Logs') & ~(encoding_df["confidence"].str.contains('1.0'))]

    encoded_df.to_csv(
        'encoding_audit.csv',
        encoding='utf-8')
    return encoded_df


def drop_empty_rows(
        infile: Path, outfile: Path):
    """_summary_ csv drop empty rows at top of file, save output

    :param file_path: _description_
    :type file_path: _type_
    :param new_file_path: _description_
    :type new_file_path: _type_
    """
    #
    data = pd.read_csv(
        infile, skip_blank_lines=True)
    logging.INFO(
        f'removing blank rows in {infile.stem}')
    data.dropna(how="all", inplace=True)
    data.to_csv(
        outfile,
        header=True,
        encoding='utf-8')


def delete_unrequired_files(
        data_folder: Path, drop_file_list: list):
    """_summary_ deletes files if they match a substring name from list.

    :param data_folder: _description_
    :type data_folder: Path
    :param drop_file_list: _description_
    :type drop_file_list: list
    """
    for fn in data_folder.glob("**/*"):
        if fn.is_file():
            for dfl in drop_file_list:
                if dfl in fn.stem:
                    # logged in datapipe
                    # logs.
                    logging.INFO(
                        f"{fn.stem} removing file from processing not required.")
                    fn.unlink()


def main():

    # assumed location of data folder.
    data_folder = Path(
        f'{Path.cwd()}/data/liia_data_exchange/')
    # list of commonly submitted unneeded
    # files.
    drop_file_list = ['Extended Review',
                      'Pupil Premium Children',
                      'Children Ceasing to be looked after for other reasons',
                      'Distance and Placement Extended',
                      'Extended Adoption',
                      'Children Ceased Care During the Year',
                      'Children Looked After on 31st March',
                      'Children Started Care During the Year']

    delete_unrequired_files(
        data_folder=data_folder,
        drop_file_list=drop_file_list)

    # run to
    file_encoding_audit(
        data_folder=data_folder)

    # add path to xml schema
    validator = XML_Validator(
        "path/to/scheme.xsd")

    # The directory with XML files
    XML_DIR = Path("path/to/directory")

    for fn in os.listdir(XML_DIR):
        print(f'{fn}: '.format(), end='')

        file_path = f'{XML_DIR}/{fn}'

        if validator.validate(file_path):
            print(f'Valid: {fn}')
        else:
            print(f'Not valid: {fn}')

    # remove first row of csv files.
    for csvfile in data_folder.rglob(
            '**/*'):
        if csvfile.suffix == '.csv':
            drop_empty_rows(
                csvfile.stem, csvfile.stem)


if __name__ == "__main__":
    main()
