from fs import open_fs
from sfdata_stream_parser.events import StartContainer

from liiatools.common.data import FileLocator
from liiatools.common.stream_filters import tablib_parse
from liiatools.annex_a_pipeline.spec.samples import DIR as DIR_AA
from liiatools.ssda903_pipeline.spec.samples import DIR as DIR_903


def test_parse_tabular_csv():
    samples_fs = open_fs(DIR_903.as_posix())

    locator = FileLocator(samples_fs, "SSDA903_2020_episodes.csv")
    stream = tablib_parse(locator)

    stream = list(stream)
    assert stream
    assert stream[0] == StartContainer(filename="SSDA903_2020_episodes.csv")


def test_parse_tabular_xlsx():
    samples_fs = open_fs(DIR_AA.as_posix())

    locator = FileLocator(samples_fs, "Annex_A.xlsx")
    stream = tablib_parse(locator)

    stream = list(stream)
    assert stream
    assert stream[0] == StartContainer(filename="Annex_A.xlsx", sheetname="List 1")


def test_parse_with_alternative_name():
    samples_fs = open_fs(DIR_903.as_posix())

    locator = FileLocator(
        samples_fs, "SSDA903_2020_episodes.csv", original_path="/year/2020/episodes.csv"
    )
    stream = tablib_parse(locator)

    stream = list(stream)
    assert stream
    assert stream[0] == StartContainer(filename="/year/2020/episodes.csv")
