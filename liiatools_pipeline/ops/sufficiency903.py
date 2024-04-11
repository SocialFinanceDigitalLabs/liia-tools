from sufficiency_data_transform.all_dim_and_fact import (
    create_dimONSArea,
    create_dimLookedAfterChild,
    create_dimOfstedProvider,
    create_dimPostcode,
    create_factEpisode,
    create_factOfstedInspection,
    create_dim_tables,
)

from dagster import op


@op
def ons_area():
    create_dimONSArea()
