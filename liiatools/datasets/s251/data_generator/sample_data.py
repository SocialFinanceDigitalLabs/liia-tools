import random
import string
from datetime import date, datetime, timedelta

from sfdata_stream_parser.events import StartTable, StartRow, Cell, EndRow, EndTable

from liiatools.datasets.s251.lds_s251_clean.configuration import Config

s251_columns = [
    "Child ID",
    "Date of birth",
    "Gender",
    "Ethnicity",
    "Disability",
    "Category of need",
    "Date of last assessment",
    "Does the child have an EHCP",
    "Is the child UASC",
    "Number of missing episodes in current period of care",
    "Legal status",
    "Placement start date",
    "Placement end date",
    "Date of start of current care period",
    "Reason for placement change",
    "Number of placements in last 12 months",
    "Number of placements in current care period",
    "Placement type",
    "Provider type",
    "Procurement platform",
    "Procurement framework",
    "Ofsted URN",
    "Home postcode",
    "Placement postcode",
    "LA of placement",
    "Total committed cost accrued in FY to date",
    "Contribution from social care provider/s",
    "Contribution from health provider/s",
    "Contribution from education provider/s",
]


def generate_sample_s251_file():
    """
    Generate a sample s251 file

    :return: stream of generators containing information required to create a csv file
    """
    yield StartTable(expected_columns=s251_columns)
    for _ in range(10000):
        yield from generate_s251_child()
    yield EndTable()


def random_chance_0_1(prob_0=0.25, prob_1=0.75):
    """
    Create a random value of 0 or 1 using given probabilities of each occurring

    :param prob_0: probability of 0 occurring (default is 0.25)
    :param prob_1: probability of 1 occurring (default is 0.75)
    :return: integer of 0 or 1
    """
    assert prob_0 + prob_1 == 1
    return random.choices([0, 1], [prob_0, prob_1])[0]


def generate_s251_child(configuration=None):
    """
    Generate information for the CSWWWorker yaml element

    :param configuration: configuration .yml file used to create elements matching the .yml schema
    :return: stream of generators containing <CSWWWorker> information required to create an csv file
    """
    if not configuration:
        configuration = Configuration()

    dob = date.today() - timedelta(days=random.uniform(0, 365 * 18))

    gender = configuration.random_gender
    ethnicity = configuration.random_ethnicity
    disability = configuration.random_disability
    category_of_need = configuration.random_category_of_need

    date_of_last_assessment = dob + timedelta(days=random.uniform(0, 365 * 18))
    if date_of_last_assessment > date.today():
        date_of_last_assessment = date.today()

    ehcp = configuration.random_does_the_child_have_an_EHCP
    uasc = configuration.random_is_the_child_UASC
    legal_status = configuration.random_legal_status

    placement_start_date = date.today() - timedelta(days=random.uniform(0, 365))
    if placement_start_date < dob:
        placement_start_date = dob

    placement_end_date = placement_start_date + timedelta(days=random.uniform(0, 365))
    if placement_end_date > date.today():
        placement_end_date = ""

    current_care_period_start_date = placement_start_date - timedelta(
        days=random.uniform(0, 365)
    )
    if current_care_period_start_date < dob:
        current_care_period_start_date = dob

    reason_placement_change = configuration.random_reason_for_placement_change

    placements_in_last_12_months = random.choices(
        [1, 2, 3, 4], [0.75, 0.15, 0.05, 0.05]
    )[0]
    placements_in_current_care_period = (
        placements_in_last_12_months
        + random.choices([1, 2, 3, 4], [0.75, 0.15, 0.05, 0.05])[0]
    )

    placement_type = configuration.random_placement_type
    provider_type = configuration.random_provider_type
    procurement_platform = configuration.random_procurement_platform

    yield StartRow()

    yield Cell(header="Child ID", cell=f"{random.randint(100000, 999999)}")
    yield Cell(header="Date of birth", cell=dob.strftime("%Y-%m-%d"))
    yield Cell(header="Gender", cell=gender)
    yield Cell(header="Ethnicity", cell=ethnicity)
    yield Cell(header="Disability", cell=disability)
    yield Cell(header="Category of need", cell=category_of_need)
    yield Cell(header="Date of last assessment", cell=date_of_last_assessment)
    yield Cell(header="Does the child have EHCP", cell=ehcp)
    yield Cell(header="Is the child UASC", cell=uasc)
    yield Cell(
        header="Number of missing episodes in current period of care",
        cell=random.choices([0, 1, 2, 3], [0.75, 0.15, 0.05, 0.05])[0],
    )
    yield Cell(header="Legal status", cell=legal_status)
    yield Cell(header="Placement start date", cell=placement_start_date)
    yield Cell(header="Placement end date", cell=placement_end_date)
    yield Cell(
        header="Date of start of current care period",
        cell=current_care_period_start_date,
    )
    yield Cell(header="Reason for placement change", cell=reason_placement_change)
    yield Cell(
        header="Number of placements in last 12 months",
        cell=placements_in_last_12_months,
    )
    yield Cell(
        header="Number of placements in current care period",
        cell=placements_in_current_care_period,
    )
    yield Cell(header="Placement type", cell=placement_type)
    yield Cell(header="Provider type", cell=provider_type)
    yield Cell(header="Procurement platform", cell=procurement_platform)
    yield Cell(header="Procurement framework", cell="")
    yield Cell(header="Ofsted URN", cell="")
    yield Cell(header="Home postcode", cell="")
    yield Cell(header="Placement postcode", cell="")
    yield Cell(header="LA of placement", cell="")
    yield Cell(header="Total committed cost accrued FY to date", cell="")
    yield Cell(header="Contribution from social care provider/s", cell="")
    yield Cell(header="Contribution from health provider/s", cell="")
    yield Cell(header="Contribution from education provider/s", cell="")

    yield EndRow()

    # role_start_date = dob + timedelta(days=random.uniform(365 * 18, 365 * 50))
    # if role_start_date > date.today():
    #     role_start_date = date.today()

    # role_end_date_count = (
    #     random.choices([0, 1])[0] if role_start_date < date.today() else 0
    # )
    # role_end_date = role_start_date + timedelta(days=random.uniform(365 * 18, 365 * 50))
    # if role_end_date > date.today():
    #     role_end_date = date.today()

    # yield StartElement(tag="CSWWWorker")
    # yield from TextElement(tag="AgencyWorker", text=agency_worker)
    # yield from TextElement(
    #     tag="SWENo",
    #     text=f"{''.join(random.choices(string.ascii_letters, k=2))}"
    #     f"{random.randint(1000000000, 9999999999)}",
    # )

    # text = 0 if role_end_date_count == 1 else round(random.uniform(0, 1), 6)
    # yield from TextElement(tag="FTE", text=text)

    # if agency_worker == 0:
    #     yield from TextElement(tag="PersonBirthDate", text=dob.strftime("%Y-%m-%d"))
    #     yield from TextElement(tag="GenderCurrent", text=configuration.random_gender)
    #     yield from TextElement(tag="Ethnicity", text=configuration.random_ethnicity)
    #     yield from TextElement(tag="QualInst", text="Institution Name")
    #     yield from TextElement(tag="QualLevel", text=configuration.random_qual)
    #     yield from TextElement(tag="StepUpGrad", text=configuration.random_yesno)
    #     yield from TextElement(tag="OrgRole", text=configuration.random_role)
    #     yield from TextElement(
    #         tag="RoleStartDate", text=role_start_date.strftime("%Y-%m-%d")
    #     )
    #     yield from TextElement(tag="StartOrigin", text=configuration.random_origin)
    #     if role_end_date_count == 1:
    #         yield from TextElement(
    #             tag="RoleEndDate", text=role_end_date.strftime("%Y-%m-%d")
    #         )
    #         yield from TextElement(
    #             tag="LeaverDestination", text=configuration.random_leaver
    #         )
    #         yield from TextElement(tag="ReasonLeave", text=configuration.random_reason)
    #     yield from TextElement(tag="FTE30", text=round(random.uniform(0, 1), 6))
    #     yield from TextElement(tag="Cases30", text=random.randint(0, 100))
    #     yield from TextElement(
    #         tag="WorkingDaysLost", text=round(random.uniform(0, 100), 2)
    #     )
    #     yield from TextElement(
    #         tag="ContractWeeks", text=round(random.uniform(0, 500), 1)
    #     )
    #     yield from TextElement(tag="FrontlineGrad", text=configuration.random_yesno)

    # else:
    #     if random_chance_0_1() == 1:
    #         yield from TextElement(tag="PersonBirthDate", text=dob.strftime("%Y-%m-%d"))
    #     if random_chance_0_1() == 1:
    #         yield from TextElement(
    #             tag="GenderCurrent", text=configuration.random_gender
    #         )
    #     if random_chance_0_1() == 1:
    #         yield from TextElement(tag="Ethnicity", text=configuration.random_ethnicity)
    #     if random_chance_0_1() == 1:
    #         yield from TextElement(tag="QualInst", text="Institution Name")
    #     if random_chance_0_1() == 1:
    #         yield from TextElement(tag="QualLevel", text=configuration.random_qual)
    #     if random_chance_0_1() == 1:
    #         yield from TextElement(tag="StepUpGrad", text=configuration.random_yesno)
    #     if random_chance_0_1() == 1:
    #         yield from TextElement(tag="OrgRole", text=configuration.random_role)
    #     if random_chance_0_1() == 1:
    #         yield from TextElement(
    #             tag="RoleStartDate", text=role_start_date.strftime("%Y-%m-%d")
    #         )
    #     if random_chance_0_1() == 1:
    #         yield from TextElement(tag="StartOrigin", text=configuration.random_origin)
    #     if role_end_date_count == 1:
    #         yield from TextElement(
    #             tag="RoleEndDate", text=role_end_date.strftime("%Y-%m-%d")
    #         )
    #         yield from TextElement(
    #             tag="LeaverDestination", text=configuration.random_leaver
    #         )
    #         yield from TextElement(tag="ReasonLeave", text=configuration.random_reason)
    #     if random_chance_0_1() == 1:
    #         yield from TextElement(tag="FTE30", text=round(random.uniform(0, 1), 6))
    #     if random_chance_0_1() == 1:
    #         yield from TextElement(tag="Cases30", text=random.randint(0, 100))
    #     if random_chance_0_1() == 1:
    #         yield from TextElement(
    #             tag="WorkingDaysLost", text=round(random.uniform(0, 100), 2)
    #         )
    #     if random_chance_0_1() == 1:
    #         yield from TextElement(
    #             tag="ContractWeeks", text=round(random.uniform(0, 500), 1)
    #         )
    #     if random_chance_0_1() == 1:
    #         yield from TextElement(tag="FrontlineGrad", text=configuration.random_yesno)

    # if random_chance_0_1(0.8, 0.2) == 1:
    #     yield from TextElement(tag="Absat30Sept", text=configuration.random_yesno)
    #     yield from TextElement(tag="ReasonAbsence", text=configuration.random_absence)

    # if random_chance_0_1(0.5, 0.5) == 1:
    #     yield from TextElement(tag="CFKSSstatus", text=configuration.random_cfkss)

    # yield EndElement(tag="CSWWWorker")


class Configuration:
    def __init__(self, config=None):
        if config:
            self._config = config
        else:
            self._config = Config()

    def __getattr__(self, item: str):
        """
        Generate random value from a given element in the config

        :param item: name of element we want to get a random value from
        :return: random value from a given element
        """
        if item.startswith("random_"):
            header = (
                item[7:].capitalize().replace("_", " ")
            )  # Change formatting to match config
            values = self._config["placement_costs"][header]["category"]
            if values is not None:
                return random.choice(values)["code"]
        raise AttributeError(f"{item} not found.")
