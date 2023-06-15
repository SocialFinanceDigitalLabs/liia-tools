import datetime
import random
from typing import List
from .data_types import Probabilities, Child
from .generators import (
    generate_child_id,
    generate_dob,
    generate_attribute,
    generate_date_of_last_assessment,

)


class ChildrenGenerator:
    def __init__(self, start_date: datetime.datetime, end_date: datetime.datetime, probabilities: Probabilities = None):
        self.start_date = start_date
        self.end_date = end_date

        if probabilities is None:
            # Use the defaults in the class
            probabilities = Probabilities()

        self.probabilities = probabilities

    def generate(self, num_children: int) -> List[Child]:
        child_id_generator = generate_child_id()

        children = []
        for _ in range(num_children):
            child_id = next(child_id_generator)
            dob = generate_dob(self.start_date)
            date_last_assessment = generate_date_of_last_assessment(self.start_date, dob)
            placement_start_date =
            placement_end_date =
            current_care_period_start_date =
            placement_change_reason =
            placements_in_last_12_months =
            placements_in_current_care_period =
            placement_type =
            provider_type =
            procurement_platform =
            procurement_framework =
            ofsted_urn =
            home_postcode =
            placement_postcode =
            placement_la =
            committed_cost =
            social_care_contribution =
            health_contribution =
            education_contribution =

            child = Child(
                child_id=child_id,
                dob=dob,
                gender=generate_attribute("Gender"),
                ethnicity=generate_attribute("Ethnicity"),
                disability=generate_attribute("Disability"),
                category_of_need=generate_attribute("Category of need"),
                date_last_assessment=date_last_assessment,
                ehcp=generate_attribute("Does the child have an EHCP?"),
                uasc=generate_attribute("Is the child UASC?"),
                missing_episodes=random.choices([0, 1, 2, 3], [.75, .15, .05, .05])[0],
                legal_status=generate_attribute("Legal status"),
                placement_start_date=
                placement_end_date=
                current_care_period_start_date=
                placement_change_reason=generate_attribute("Reason for placement change"),
                placements_in_last_12_months=
                placements_in_current_care_period=
                placement_type=generate_attribute("Placement type"),
                provider_type=generate_attribute("Provider type"),
                procurement_platform=generate_attribute("Procurement platform"),
                procurement_framework=
                ofsted_urn=
                home_postcode=
                placement_postcode=
                placement_la=
                committed_cost=
                social_care_contribution=
                health_contribution=
                education_contribution=
            )

            children.append(child)

        return children
