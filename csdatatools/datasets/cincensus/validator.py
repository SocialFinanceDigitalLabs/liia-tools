import logging
from pathlib import Path

import xmlschema

from csdatatools.spec import cin

log = logging.getLogger(__name__)


class CinValidator:

    def __init__(self, version='2022'):
        self._version = version
        schema_file = Path(cin.__file__).parent / f"cin-{version}.xsd"
        self._schema = xmlschema.XMLSchema(schema_file)

    def validate(self, filename):
        log.info("Validating %s", filename)
        errors = list(self._schema.iter_errors(filename))

        for error in errors:
            log.error(error)

        if len(errors) == 0:
            log.info(f"{filename} is valid")
