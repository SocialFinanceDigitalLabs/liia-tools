import datetime
from dataclasses import dataclass, field
from typing import Dict


@dataclass
class Child:
    child_id: int
    dob: datetime.datetime
    gender: int
    ethnicity: str
    disability: str
    category_of_need: str
    date_last_assessment: datetime.datetime
    ehcp: str
    uasc: str
    missing_episodes: int
    legal_status: str
    placement_start_date: datetime.datetime
    placement_end_date: datetime.datetime
    current_care_period_start_date: datetime.datetime
    placement_change_reason: str
    placements_in_last_12_months: int
    placements_in_current_care_period: int
    placement_type: str
    provider_type: str
    procurement_platform: str
    procurement_framework: str
    ofsted_urn: str
    home_postcode: str
    placement_postcode: str
    placement_la: str
    committed_cost: float
    social_care_contribution: float
    health_contribution: float
    education_contribution: float


@dataclass
class Probabilities:
    """
    Probability set for generating the data.

    The default values are estimated (roughly) from real 903 data.

    :param is_uasc: The probability that any given child in care is UASC.
    :param is_mother: The probability that a female in care will be a mother at any point.
    :param daily_episode_ending: The daily probability of a LAC period ending.
    :param daily_episode_changing: The daily probability of a LAC period changing (placement/legal status/carer)
    :param extra_episode_rate: The average rate of extra periods of care (beyond 1), modelled as 1 + Poisson(rate)
    :param reason_for_episode_change: A dict with the episode change keys, and associated probability weights (sum to 1)
    :param review_frequency: The daily frequency of reviews
    """
    is_uasc: float = 0.05
    is_mother: float = 0.01
    is_adopted: float = 0.05
    is_missing: float = 0.05
    daily_episode_ending: float = 1 / 1000  # Average care length is around 1000 days
    daily_episode_changing: float = 1 / 300   # Average episode length is around 300 days
    average_extra_episode_rate: float = 0.15
    reason_for_episode_change: Dict[str, int] = field(default_factory=lambda: {
            'B': 0.02,
            'L': 0.27,
            'P': 0.605,
            'T': 0.1,
            'U': 0.005,
        })
    review_frequency: float = 1 / 365
