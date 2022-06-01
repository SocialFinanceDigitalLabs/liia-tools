import random
from datetime import date, datetime, timedelta
from functools import cached_property
import pytz

from sfdata_stream_parser.events import StartElement as S, EndElement as E, TextNode as T

from csdatatools.datasets.cincensus.schema import Schema


def TE(tag, text):
    yield S(tag=tag)
    yield T(text=str(text))
    yield E(tag=tag)


def generate_sample_census_file(**options):
    yield S(tag='Message')
    yield from generate_sample_header(**options)

    yield S(tag='Children')
    for child_id in range(random.randint(1, 500)):
        yield from generate_child()
    yield E(tag='Children')

    yield E(tag='Message')


def generate_sample_header(**opts):
    yield S(tag='Header')
    yield S(tag='CollectionDetails')
    yield from TE(tag='Collection', text='CIN')
    yield from TE(tag='Year', text=opts.get('year', date.today().year))
    yield from TE(tag='ReferenceDate', text=opts.get('year', date.today().strftime('%Y-%m-%d')))
    yield E(tag='CollectionDetails')
    yield S(tag='Source')
    yield from TE(tag='SourceLevel', text='L')
    if opts.get('lea'):
        yield from TE(tag='LEA', text=opts['lea'])
    yield from TE(tag='SoftwareCode', text=__name__)
    # yield from TE(tag='Release', text="")
    # yield from TE(tag='SerialNo', text="")
    yield from TE(tag='DateTime', text=datetime.now().astimezone(pytz.UTC).strftime('%Y-%m-%dT%H:%M:%SZ'))

    yield E(tag='Source')
    yield E(tag='Header')


def generate_child(configuration=None, **opts):
    if not configuration:
        configuration = ChildConfiguration()

    if opts.get('seed'):
        random.seed(opts['seed'])

    dob = opts.get('dob', date.today() - timedelta(days=365*18*random.random()))
    age_out = dob + timedelta(days=365*18*random.random())
    if age_out > date.today():
        age_out = date.today()

    is_dead = random.random() < 0.05
    death_date = dob + timedelta(days=365*18*random.random()) if is_dead else None
    disability_count = random.choices([0, 1, 2, 3], [.75, .15, .05, .05])[0]
    disabilities = random.choices(configuration.disability, k=disability_count)

    cin_count = random.choices([1, 2, 3, 4], [.75, .15, .05, .05])[0]
    cin_details = []

    start_after = dob
    end_by = age_out if not death_date else death_date
    for i in range(cin_count):
        details = CinDetails(start_after, end_by)
        cin_details.append(details)
        if not details.end:
            break
        start_after = details.end

    yield S(tag='Child')
    yield S(tag='ChildIdentifiers')

    yield from TE(tag='LAchildID', text=f"RND{random.randint(5000, 999999999):012}")
    yield from TE(tag='UPN', text=f"A{''.join([str(random.randint(0,9)) for i in range(12)])}")

    yield from TE(tag='PersonBirthDate', text=dob.strftime('%Y-%m-%d'))
    yield from TE(tag='GenderCurrent', text=configuration.random_gender)
    if death_date:
        yield from TE(tag='PersonDeathDate', text=dob.strftime('%Y-%m-%d'))


    yield E(tag='ChildIdentifiers')
    yield S(tag='ChildCharacteristics')

    yield from TE(tag='Ethnicity', text=configuration.random_ethnicity)
    if disability_count > 0:
        yield S(tag='Disabilities')
        for d in disabilities:
            yield from TE(tag='Disability', text=d)
        yield E(tag='Disabilities')
    yield E(tag='ChildCharacteristics')

    for d in cin_details:
        yield S(tag='CINdetails')
        yield from TE(tag='CINreferralDate', text=d.start.strftime('%Y-%m-%d'))
        yield from TE(tag='ReferralSource', text=configuration.random_referralsource)
        yield from TE(tag='PrimaryNeedCode', text=configuration.random_primaryneedcode)
        if d.end:
            yield from TE(tag='CINclosureDate', text=d.end.strftime('%Y-%m-%d'))
            yield from TE(tag='ReasonForClosure', text=configuration.random_reasonforclosure)

        yield from TE(tag='DateOfInitialCPC', text=d.initial_cpc.strftime('%Y-%m-%d'))

        yield E(tag='CINdetails')

    yield E(tag='Child')


class ChildConfiguration:

    def __init__(self, schema=None):
        if schema:
            self._schema = schema
        else:
            self._schema = Schema().schema

    def __getattr__(self, item:str):
        if item.startswith('random_'):
            values = getattr(self, item[7:])
            if values is not None:
                return random.choice(values)
        else:
            value = self._schema.types[f'{item}type']
            if value is not None:
                return value.enumeration
        raise AttributeError(f"{item} not found.")


def random_delta(delta):
    minutes = delta / timedelta(minutes=1)
    return timedelta(minutes=minutes*random.random())


class CinDetails:

    def __init__(self, must_start_after, must_end_by):
        self.start = must_start_after + random_delta(must_end_by - must_start_after)
        self.end = self.start + random_delta((must_end_by - self.start) + timedelta(days=180))
        if self.end >= must_end_by:
            self.end = None
        else:
            must_end_by = self.end

        self.initial_cpc = self.start + random_delta(timedelta(days=90))
        if self.initial_cpc >= must_end_by:
            self.initial_cpc = None
