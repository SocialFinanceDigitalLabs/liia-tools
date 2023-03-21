import random
import string
from datetime import date, datetime, timedelta
import pytz

from sfdata_stream_parser.events import StartElement, EndElement, TextNode

from liiatools.datasets.social_work_workforce.schema import Schema


def TextElement(tag: str, text):
    """
    Create a complete TextNode sandwiched between a StartElement and EndElement

    :param tag: XML tag
    :param text: text to be stored in the given XML tag, could be a string, integer, float etc.
    :return: StartElement and EndElement with given tags and TextNode with given text
    """
    yield StartElement(tag=tag)
    yield TextNode(text=str(text))
    yield EndElement(tag=tag)


def generate_sample_csww_file(**options):
    """
    Generate a sample children's social work workforce census file

    :return: stream of generators containing information required to create an XML file
    """
    yield StartElement(tag="Message")
    yield from generate_sample_header(**options)

    yield StartElement(tag="LALevelVacancies")
    yield from generate_la_level_vacancies()
    yield EndElement(tag="LALevelVacancies")

    for worker in range(random.randint(1, 50)):
        yield from generate_csww_worker()

    yield EndElement(tag="Message")


def generate_sample_header(**opts):
    """
    Generate information for the <Header> XML element

    :return: stream of generators containing <Header> information required to create an XML file
    """
    yield StartElement(tag="Header")
    yield StartElement(tag="CollectionDetails")
    yield from TextElement(tag="Collection", text="CSWW")
    yield from TextElement(tag="Year", text=opts.get("year", date.today().year))
    yield from TextElement(
        tag="ReferenceDate", text=opts.get("year", date.today().strftime("%Y-%m-%d"))
    )
    yield EndElement(tag="CollectionDetails")
    yield StartElement(tag="Source")
    yield from TextElement(tag="SourceLevel", text="L")
    if opts.get("lea"):
        yield from TextElement(tag="LEA", text=opts["lea"])
    else:
        yield from TextElement(tag="LEA", text=f"{random.randint(100, 999):03}")

    yield from TextElement(tag="SoftwareCode", text=__name__)
    yield from TextElement(
        tag="DateTime",
        text=datetime.now().astimezone(pytz.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
    )

    yield EndElement(tag="Source")
    yield EndElement(tag="Header")


def generate_la_level_vacancies():
    """
    Generate information for the <LALevelVacancies> XML element

    :return: stream of generators containing <LALevelVacancies> information required to create an XML file
    """
    yield from TextElement(
        tag="NumberOfVacancies", text=round(random.uniform(0, 100), 2)
    )
    yield from TextElement(tag="NoAgencyFTE", text=round(random.uniform(0, 100), 2))
    yield from TextElement(tag="NoAgencyHeadcount", text=random.randint(0, 100))


def random_chance_0_1(prob_0=0.25, prob_1=0.75):
    """
    Create a random value of 0 or 1 using given probabilities of each occurring

    :param prob_0: probability of 0 occurring (default is 0.25)
    :param prob_1: probability of 1 occurring (default is 0.75)
    :return: integer of 0 or 1
    """
    assert prob_0 + prob_1 == 1
    return random.choices([0, 1], [prob_0, prob_1])[0]


def generate_csww_worker(configuration=None, **opts):
    """
    Generate information for the <CSWWWorker> XML element

    :param configuration: configuration .xsd file used to create elements matching the .xsd schema
    :return: stream of generators containing <CSWWWorker> information required to create an XML file
    """
    if not configuration:
        configuration = Configuration()

    if opts.get("seed"):
        random.seed(opts["seed"])

    dob = opts.get(
        "dob", date.today() - timedelta(days=random.uniform(365 * 18, 365 * 66))
    )

    agency_worker = configuration.random_agencyworker

    role_start_date = dob + timedelta(days=random.uniform(365 * 18, 365 * 50))
    if role_start_date > date.today():
        role_start_date = date.today()

    role_end_date_count = (
        random.choices([0, 1])[0] if role_start_date < date.today() else 0
    )
    role_end_date = role_start_date + timedelta(days=random.uniform(365 * 18, 365 * 50))
    if role_end_date > date.today():
        role_end_date = date.today()

    yield StartElement(tag="CSWWWorker")
    yield from TextElement(tag="AgencyWorker", text=agency_worker)
    yield from TextElement(
        tag="SWENo",
        text=f"{''.join(random.choices(string.ascii_letters, k=2))}"
        f"{random.randint(1000000000, 9999999999)}",
    )

    text = 0 if role_end_date_count == 1 else round(random.uniform(0, 1), 6)
    yield from TextElement(tag="FTE", text=text)

    if agency_worker == 0:
        yield from TextElement(tag="PersonBirthDate", text=dob.strftime("%Y-%m-%d"))
        yield from TextElement(tag="GenderCurrent", text=configuration.random_gender)
        yield from TextElement(tag="Ethnicity", text=configuration.random_ethnicity)
        yield from TextElement(tag="QualInst", text="Institution Name")
        yield from TextElement(tag="QualLevel", text=configuration.random_qual)
        yield from TextElement(tag="StepUpGrad", text=configuration.random_yesno)
        yield from TextElement(tag="OrgRole", text=configuration.random_role)
        yield from TextElement(
            tag="RoleStartDate", text=role_start_date.strftime("%Y-%m-%d")
        )
        yield from TextElement(tag="StartOrigin", text=configuration.random_origin)
        if role_end_date_count == 1:
            yield from TextElement(
                tag="RoleEndDate", text=role_end_date.strftime("%Y-%m-%d")
            )
            yield from TextElement(
                tag="LeaverDestination", text=configuration.random_leaver
            )
            yield from TextElement(tag="ReasonLeave", text=configuration.random_reason)
        yield from TextElement(tag="FTE30", text=round(random.uniform(0, 1), 6))
        yield from TextElement(tag="Cases30", text=random.randint(0, 100))
        yield from TextElement(
            tag="WorkingDaysLost", text=round(random.uniform(0, 100), 2)
        )
        yield from TextElement(
            tag="ContractWeeks", text=round(random.uniform(0, 500), 1)
        )
        yield from TextElement(tag="FrontlineGrad", text=configuration.random_yesno)

    else:
        if random_chance_0_1() == 1:
            yield from TextElement(tag="PersonBirthDate", text=dob.strftime("%Y-%m-%d"))
        if random_chance_0_1() == 1:
            yield from TextElement(
                tag="GenderCurrent", text=configuration.random_gender
            )
        if random_chance_0_1() == 1:
            yield from TextElement(tag="Ethnicity", text=configuration.random_ethnicity)
        if random_chance_0_1() == 1:
            yield from TextElement(tag="QualInst", text="Institution Name")
        if random_chance_0_1() == 1:
            yield from TextElement(tag="QualLevel", text=configuration.random_qual)
        if random_chance_0_1() == 1:
            yield from TextElement(tag="StepUpGrad", text=configuration.random_yesno)
        if random_chance_0_1() == 1:
            yield from TextElement(tag="OrgRole", text=configuration.random_role)
        if random_chance_0_1() == 1:
            yield from TextElement(
                tag="RoleStartDate", text=role_start_date.strftime("%Y-%m-%d")
            )
        if random_chance_0_1() == 1:
            yield from TextElement(tag="StartOrigin", text=configuration.random_origin)
        if role_end_date_count == 1:
            yield from TextElement(
                tag="RoleEndDate", text=role_end_date.strftime("%Y-%m-%d")
            )
            yield from TextElement(
                tag="LeaverDestination", text=configuration.random_leaver
            )
            yield from TextElement(tag="ReasonLeave", text=configuration.random_reason)
        if random_chance_0_1() == 1:
            yield from TextElement(tag="FTE30", text=round(random.uniform(0, 1), 6))
        if random_chance_0_1() == 1:
            yield from TextElement(tag="Cases30", text=random.randint(0, 100))
        if random_chance_0_1() == 1:
            yield from TextElement(
                tag="WorkingDaysLost", text=round(random.uniform(0, 100), 2)
            )
        if random_chance_0_1() == 1:
            yield from TextElement(
                tag="ContractWeeks", text=round(random.uniform(0, 500), 1)
            )
        if random_chance_0_1() == 1:
            yield from TextElement(tag="FrontlineGrad", text=configuration.random_yesno)

    if random_chance_0_1(0.8, 0.2) == 1:
        yield from TextElement(tag="Absat30Sept", text=configuration.random_yesno)
        yield from TextElement(tag="ReasonAbsence", text=configuration.random_absence)

    if random_chance_0_1(0.5, 0.5) == 1:
        yield from TextElement(tag="CFKSSstatus", text=configuration.random_cfkss)

    yield EndElement(tag="CSWWWorker")


class Configuration:
    def __init__(self, schema=None):
        if schema:
            self._schema = schema
        else:
            self._schema = Schema().schema

    def __getattr__(self, item: str):
        """
        Generate random value from a given element in the schema

        :param item: name of element we want to get a random value from
        :return: random value from a given element
        """
        if item.startswith("random_"):
            values = getattr(self, item[7:])
            if values is not None:
                return random.choice(values)
        else:
            value = self._schema.types[f"{item}type"]
            if value is not None:
                return value.enumeration
        raise AttributeError(f"{item} not found.")
