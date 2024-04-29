from fs import open_fs

from liiatools.common.constants import SessionNames
from liiatools.common.data import FileLocator
from liiatools.common.pipeline import (
    create_session_folder,
    discover_year,
    move_files_for_processing,
    restore_session_folder,
    discover_term,
    Term,
)
from liiatools.annex_a_pipeline.spec.samples import DIR as DIR_AA
from liiatools.school_census_pipeline.spec.samples import DIR as DIR_SC


def test_create_session_folder():
    dest_fs = open_fs("mem://")
    session_folder = create_session_folder(dest_fs)
    assert session_folder


def test_move_files_for_processing():
    source_fs = open_fs("mem://")
    source_fs.makedir("2019")
    source_fs.writetext("2019/file1.txt", "foo")
    source_fs.writetext("2019/file2.txt", "bar")

    session_fs = open_fs("mem://")
    session_fs.makedir(SessionNames.INCOMING_FOLDER)
    file_locators = move_files_for_processing(source_fs, session_fs)
    file_locators = list(file_locators)

    assert len(file_locators) == 2

    files = list(session_fs.walk.files())
    assert len(files) == 4

    file_info = {f.meta["path"]: f for f in file_locators}

    file1_uid = file_info["/2019/file1.txt"].meta["uuid"]

    assert session_fs.exists(f"{SessionNames.INCOMING_FOLDER}/{file1_uid}")
    assert session_fs.exists(f"{SessionNames.INCOMING_FOLDER}/{file1_uid}_meta.yaml")

    assert (
        file_info["/2019/file1.txt"].meta["sha256"]
        == "2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae"
    )

    with file_info["/2019/file1.txt"].open("rt") as FILE:
        assert FILE.read() == "foo"


def test_restore_session_folder():
    session_folder = open_fs("mem://")
    incoming_folder = session_folder.makedirs(SessionNames.INCOMING_FOLDER)
    incoming_folder.writetext(
        "UUID1_meta.yaml", "path: /2019/file1.txt\nuuid: UUID1\nsha256: sha-1"
    )
    incoming_folder.writetext("UUID1", "foo")
    incoming_folder.writetext(
        "UUID2_meta.yaml", "path: /2019/file2.txt\nuuid: UUID2\nsha256: sha-2"
    )
    incoming_folder.writetext("UUID2", "bar")

    file_locators = restore_session_folder(session_folder)
    file_info = {f.name: f for f in file_locators}

    assert file_info["/2019/file1.txt"].meta["uuid"] == "UUID1"
    assert file_info["/2019/file2.txt"].meta["uuid"] == "UUID2"
    assert file_info["/2019/file1.txt"].meta["sha256"] == "sha-1"
    assert file_info["/2019/file2.txt"].meta["sha256"] == "sha-2"

    with file_info["/2019/file1.txt"].open("rt") as FILE:
        assert FILE.read() == "foo"


def test_discover_year_no_year():
    samples_fs = open_fs(DIR_AA.as_posix())
    locator = FileLocator(samples_fs, "Annex_A.xlsx")
    assert discover_year(locator) is None


def test_discover_year_dir_year():
    samples_fs = open_fs(DIR_AA.as_posix())
    locator = FileLocator(
        samples_fs, "Annex_A.xlsx", original_path="/2020/Annex_A.xlsx"
    )
    assert discover_year(locator) == 2020


def test_discover_year_file_year():
    samples_fs = open_fs(DIR_AA.as_posix())
    locator = FileLocator(
        samples_fs, "Annex_A.xlsx", original_path="/Annex_A-2022-23.xlsx"
    )
    assert discover_year(locator) == 2023


def test_discover_year_dir_and_file_year():
    samples_fs = open_fs(DIR_AA.as_posix())
    locator = FileLocator(
        samples_fs, "Annex_A.xlsx", original_path="/2021/Annex_A-2022-23.xlsx"
    )
    assert discover_year(locator) == 2021


def test_discover_term_no_term():
    samples_fs = open_fs(DIR_SC.as_posix())
    locator = FileLocator(
        samples_fs, "addressesonroll.csv", original_path="/addressesonroll.csv"
    )
    assert discover_term(locator) is None


def test_discover_term_dir_term():
    samples_fs = open_fs(DIR_SC.as_posix())
    locator = FileLocator(
        samples_fs,
        "addressesonroll.csv",
        original_path="/October_15/addressesonroll.csv",
    )
    assert discover_term(locator) == Term.OCT.value


def test_discover_term_file_term():
    samples_fs = open_fs(DIR_SC.as_posix())
    locator = FileLocator(
        samples_fs, "addressesonroll.csv", original_path="/October_addressesonroll.csv"
    )
    assert discover_term(locator) == Term.OCT.value


def test_discover_term_dir_and_file_term():
    samples_fs = open_fs(DIR_SC.as_posix())
    locator = FileLocator(
        samples_fs,
        "addressesonroll.csv",
        original_path="/October_15/January_addressesonroll.csv",
    )
    assert discover_term(locator) == Term.OCT.value
