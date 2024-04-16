from sufficiency_data_transform.all_dim_and_fact import (
    create_dim_tables,
    create_dimONSArea,
    create_dimLookedAfterChild,
    create_dimOfstedProvider,
    create_dimPostcode,
    create_factEpisode,
    create_factOfstedInspection,
)

from dagster import op


@op
def dim_tables():
    create_dim_tables()

@op
def ons_area():
    create_dimONSArea()


@op
def looked_after_child():
    create_dimONSArea()
    create_dimLookedAfterChild()

@op
def ofsted_provider():
    create_dimONSArea()
    create_dimOfstedProvider()

@op
def postcode():
    create_dimPostcode()

@op
def episode():
    create_dim_tables()
    create_dimONSArea()
    create_dimLookedAfterChild()
    create_dimPostcode()
    create_dimOfstedProvider()
    create_factEpisode()

@op
def ofsted_inspection():
    create_dim_tables()
    create_dimOfstedProvider()
    create_factOfstedInspection()

