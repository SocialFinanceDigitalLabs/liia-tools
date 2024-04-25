import logging
from datetime import datetime
from typing import Iterable, List, Literal, Tuple

import pandas as pd
from fs.base import FS

from liiatools.common.data import DataContainer, PipelineConfig, TableConfig

logger = logging.getLogger(__name__)


def _normalise_table(df: pd.DataFrame, table_spec: TableConfig) -> pd.DataFrame:
    """
    Normalise the dataframe to match the table spec.
    """
    df = df.copy()

    # Add any columns that are in the table spec but not in the dataframe
    for c in table_spec.columns:
        if c.id not in df.columns:
            df[c.id] = None

    # Reorder and limit the columns to match the table spec
    df = df[[c.id for c in table_spec.columns]]

    return df


def _create_unique_folder(fs: FS, suffix: str = "") -> Tuple[FS, str]:
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    existing_snapshots = fs.listdir(".")
    snap_index = 0

    if len(existing_snapshots) > 0:
        # Remove suffixes
        existing_snapshots = [s[:20] for s in existing_snapshots]
        existing_snapshots.sort()
        max_snapshot = existing_snapshots[-1]
        if timestamp in max_snapshot:
            snap_index = int(max_snapshot[16:20]) + 1

    filename = f"{timestamp}-{snap_index:04}{suffix}"
    return fs.makedir(filename), filename


_rollup_suffix = "-rollup"


class DataframeArchive:
    """
    The dataframe archive is a collection of dataframes that are stored in a filesystem.

    Every time a set of dataframes are added, a new 'snapshot' is created. The complete archive
    is created by combining all the snapshots in chronological order.

    Snapshots can be 'rolled-up' to create a complete archive of the dataframes at a given point in time. When restoring
    at a point in time, the process will find the latest roll-up, and then apply the snapshots after that point.

    Only tables and columns defined in the pipeline config are stored in the archive.

    Because files are not always loaded in chronological order, the 'primary keys' and 'sort' configurations are used
    to ensure that the dataframes are deduplicated in the right order.
    """

    def __init__(self, fs: FS, config: PipelineConfig):
        self.fs = fs
        self.config = config

    def add(self, data: DataContainer) -> str:
        """
        Add a new snapshot to the archive.
        """

        snap_dir, snap_name = _create_unique_folder(self.fs)

        logger.debug(
            "Adding snapshot of tables %s to archive %s", list(data.keys()), snap_name
        )

        for table_spec in self.config.table_list:
            if table_spec.id in data:
                self._add_table(snap_dir, table_spec, data[table_spec.id])

        return snap_name

    def _add_table(self, snap_dir: FS, table_spec: TableConfig, df: pd.DataFrame):
        """
        Add a table to the archive.
        """
        with snap_dir.open(table_spec.id + ".parquet", "wb") as f:
            df = _normalise_table(df, table_spec)
            df.to_parquet(f, index=False)

    def list_snapshots(self) -> List[str]:
        """
        List the snapshots in the archive.
        """
        return sorted(self.fs.listdir("/"))

    def list_rollups(self) -> List[str]:
        """
        List the rollups in the archive.
        """
        folders = self.list_snapshots()
        return [f for f in folders if f.endswith(_rollup_suffix)]

    def list_current_session(self) -> List[str]:
        """
        List the snapshots in the current session.

        This is the snapshots that have been added since the last rollup. If there are no rollups, then all snapshots are returned.
        """
        folders = self.list_snapshots()
        rollups = self.list_rollups()
        if len(rollups) == 0:
            return folders
        else:
            return [s for s in folders if s >= rollups[-1]]

    def delete_snapshot(self, *snap_ids: str, allow_rollups: bool = False):
        """
        Deletes one or more snapshots from the archive.
        """
        assert len(snap_ids) > 0, "At least one snapshot must be specified"

        if not allow_rollups:
            rollups = [s for s in snap_ids if _rollup_suffix in s]
            if len(rollups) > 0:
                raise Exception(f"Rollups cannot be deleted: {rollups}")

        for snap_id in snap_ids:
            self.fs.removetree(snap_id)

    def current(self) -> DataContainer:
        """
        Get the current session as a datacontainer.
        """
        snap_ids = self.list_current_session()
        return self.combine_snapshots(snap_ids)

    def rollup(self):
        """
        Roll up the snapshots in the archive.

        This combines all the snapshots into a single dataframe, and then saves it as a new snapshot (tagged as a roll-up)
        """
        snap_ids = self.list_current_session()

        combined = self.combine_snapshots(snap_ids)

        snap_dir, snap_name = _create_unique_folder(self.fs, _rollup_suffix)

        snap_dir.writetext("snapshots.txt", "\n".join(snap_ids))

        for table_spec in self.config.table_list:
            if table_spec.id in combined:
                self._add_table(snap_dir, table_spec, combined[table_spec.id])

        return snap_name

    def load_snapshot(self, snap_id) -> DataContainer:
        """
        Load a snapshot from the archive.
        """
        snap_dir = self.fs.opendir(snap_id)
        data = {}
        for table_spec in self.config.table_list:
            if snap_dir.exists(table_spec.id + ".parquet"):
                with snap_dir.open(table_spec.id + ".parquet", "rb") as f:
                    df = pd.read_parquet(f)
                    df = _normalise_table(df, table_spec)
                    data[table_spec.id] = df

        return data

    def combine_snapshots(
        self, snap_ids: Iterable[str], deduplicate_mode: Literal["E", "A", "N"] = "E"
    ) -> DataContainer:
        """
        Combine a list of snapshots into a single dataframe.

        The deduplicate_mode parameter controls how the dataframes are deduplicated. The options are:
        * E: Deduplicate after each snapshot is added
        * A: Deduplicate after all snapshots are added
        * N: Do not deduplicate

        """
        assert deduplicate_mode in ["E", "A", "N"]

        combined = DataContainer()
        for snap_id in snap_ids:
            combined = self._combine_snapshots(
                combined,
                self.load_snapshot(snap_id),
                deduplicate=deduplicate_mode == "E",
            )

        if deduplicate_mode == "A":
            combined = self.deduplicate(combined)

        return combined

    def deduplicate(self, data: DataContainer) -> DataContainer:
        """
        Deduplicate the dataframes in the container.

        If a dataframe has a 'sort' configuration, then the dataframe is sorted by the specified columns before deduplication.
        """
        for table_spec in self.config.table_list:
            if table_spec.id in data:
                sort_keys = table_spec.sort_keys

                df = data[table_spec.id]
                if sort_keys:
                    df = df.sort_values(by=sort_keys, ascending=True)
                df = df.drop_duplicates(
                    subset=[c.id for c in table_spec.columns if c.unique_key]
                    if [c.id for c in table_spec.columns if c.unique_key]
                    else None,
                    keep="last",
                )
                data[table_spec.id] = df

        return data

    def _combine_snapshots(
        self, *sources: DataContainer, deduplicate: bool = True
    ) -> DataContainer:
        """
        Combine a new snapshot into an existing set of dataframes.
        """
        data = DataContainer()

        for table_spec in self.config.table_list:
            table_id = table_spec.id
            all_sources = []
            for source in sources:
                if table_id in source:
                    all_sources.append(source[table_id])

            if len(all_sources) == 0:
                continue
            elif len(all_sources) == 1:
                data[table_id] = all_sources[0].copy()
            else:
                data[table_id] = pd.concat(all_sources, ignore_index=True)

        if deduplicate:
            data = self.deduplicate(data)

        return data
