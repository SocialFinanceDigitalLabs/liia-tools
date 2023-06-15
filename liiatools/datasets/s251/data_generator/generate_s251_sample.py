import datetime
from liiatools.datasets.s251.data_generator.children_generator import ChildrenGenerator
from liiatools.datasets.s251.data_generator.census import snapshot_children_for_period
from liiatools.datasets.s251.data_generator.csv import create_csv
from pathlib import Path

start_date = datetime.datetime(2017, 1, 1)
end_date = datetime.datetime(2023, 12, 31)

gen = ChildrenGenerator(start_date=start_date, end_date=end_date)

all_children = gen.generate(num_children=10)

census_start = datetime.datetime(2019, 4, 1)
census_end = datetime.datetime(2020, 3, 31)
children = snapshot_children_for_period(census_start, census_end, all_children)

output_path = Path(__file__).parent / "../output"
output_path.mkdir(parents=True, exist_ok=True)
create_csv(children, output_path)
