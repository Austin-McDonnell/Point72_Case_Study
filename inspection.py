import pandas as pd
from P72.utils import logger
from pathlib import Path
from typing import List


class InspectData:

    def __init__(self, file_path: Path):
        self.file_path = file_path
        self.data_set_name = self.file_path.stem
        logger.info(f"Running Inspection on: {self.data_set_name}")
        self.data = None

    def __repr__(self):
        return f"InspectData(file_path={self.file_path})"

    def initial_inspection(self):
        # Size
        shape = f"Shape:{self.data.shape}"
        columns = f"Column Names:\n{self.data.columns.tolist()}"
        describe = f"Simple Stats:\n{self.data.describe()}"
        data_types = f"Column Dtypes:\n{self.data.dtypes.apply(lambda x: x.name).to_dict()}"
        transformed_head = f"Snapshot:\n{self.data.head().T}"

        inspections = [shape, columns, describe, data_types, transformed_head]

        logger.info("\n".join(inspections))

    def read_file(self, **kwargs):
        df = pd.read_csv(self.file_path, **kwargs)
        self.data = df
        return df

    @property
    def data(self) -> pd.DataFrame:
        return self._data

    @data.setter
    def data(self, data: pd.DataFrame):
        if data is None:
            self._data = data
        else:
            self._data = data


def get_csv_files() -> List[Path]:
    dir_name = Path(r"C:\Users\austi\Documents\GitHub_Repos\Public_Projects\Case Studies\P72\data")
    return list(dir_name.glob("*.csv"))


def initialized_csv_inspections() -> List[InspectData]:
    data_files = get_csv_files()

    data_inspections = []
    for file in data_files:
        inspection = InspectData(file)
        inspection.read_file()
        inspection.initial_inspection()
        data_inspections.append(inspection)
    return data_inspections


if __name__ == '__main__':

    print('Done')