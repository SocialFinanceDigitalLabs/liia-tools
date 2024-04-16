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
    return True


@op
def ons_area():
    create_dimONSArea()
    return True


@op
def looked_after_child(area):
    create_dimLookedAfterChild()
    return True


@op
def ofsted_provider(area):
    create_dimOfstedProvider()
    return True


@op
def postcode():
    create_dimPostcode()
    return True


@op
def episode(area, lac, pc, prov, dim):
    create_factEpisode()


@op
def ofsted_inspection(dim, prov):
    create_factOfstedInspection()
