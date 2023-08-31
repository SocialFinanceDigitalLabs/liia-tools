from fs import open_fs

from liiatools.common.pipeline import create_session_folder, restore_session_folder


def test_create_session_folder():
    source_fs = open_fs("mem://")
    source_fs.makedir("2019")
    source_fs.writetext("2019/file1.txt", "foo")
    source_fs.writetext("2019/file2.txt", "bar")

    dest_fs = open_fs("mem://")

    session_folder, file_locators = create_session_folder(source_fs, dest_fs)

    assert session_folder
    assert len(file_locators) == 2

    files = list(session_folder.walk.files())
    assert len(files) == 4

    file_info = {f.meta["path"]: f for f in file_locators}

    file1_uid = file_info["/2019/file1.txt"].meta["uuid"]

    assert session_folder.exists(f"incoming/{file1_uid}")
    assert session_folder.exists(f"incoming/{file1_uid}_meta.yaml")

    assert (
        file_info["/2019/file1.txt"].meta["sha256"]
        == "2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae"
    )

    with file_info["/2019/file1.txt"].open("rt") as FILE:
        assert FILE.read() == "foo"


def test_restore_session_folder():
    source_fs = open_fs("mem://")
    source_fs.makedir("2019")
    source_fs.writetext("2019/file1.txt", "foo")
    source_fs.writetext("2019/file2.txt", "bar")

    dest_fs = open_fs("mem://")

    session_folder, _ = create_session_folder(source_fs, dest_fs)

    file_locators = restore_session_folder(session_folder)
    file_info = {f.meta["path"]: f for f in file_locators}

    file1_uid = file_info["/2019/file1.txt"].meta["uuid"]

    assert session_folder.exists(f"incoming/{file1_uid}")
    assert session_folder.exists(f"incoming/{file1_uid}_meta.yaml")

    assert (
        file_info["/2019/file1.txt"].meta["sha256"]
        == "2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae"
    )

    with file_info["/2019/file1.txt"].open("rt") as FILE:
        assert FILE.read() == "foo"
