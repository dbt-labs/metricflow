import re

from jsonschema import Draft7Validator, ValidationError
from jsonschema._utils import extras_msg
from jsonschema.validators import extend


def custom_find_additional_properties(instance, schema):  # type: ignore[no-untyped-def]
    """Return the set of additional properties for the given ``instance``.

    NOTE: This is a modified copy of the ``find_additional_properties`` method
    defined in jsonschema/_utils.py jsonschema 3.2.0 found here
    https://github.com/python-jsonschema/jsonschema/blob/2f734a7ce1395ac4ff2d394583fd46a2e8833a9b/jsonschema/_utils.py#L84

    Weeds out properties that should have been validated by ``properties`` and
    / or ``patternProperties``. Additionally, completely ignores any properties
    matching ``^__(.+)__$``

    Assumes ``instance`` is dict-like already.
    """
    properties = schema.get("properties", {})
    schema_patterns = schema.get("patternProperties", {})
    # we ignore all things that match "^__(.+)__$" in all objects
    schema_patterns["^__(.+)__$"] = {}
    patterns = "|".join(schema_patterns)
    for property in instance:
        if property not in properties:
            if patterns and re.search(patterns, property):
                continue
            yield property


def customAdditionalProperties(validator, aP, instance, schema):  # type: ignore[no-untyped-def]
    """Validator for checking if a schema has additionalProperties when it shouldn't.

    NOTE: This is a modified copy of the ``additionalProperties`` method
    defined in jsonschema/_validators.py of jsonschema 3.2.0 found here
    https://github.com/python-jsonschema/jsonschema/blob/2f734a7ce1395ac4ff2d394583fd46a2e8833a9b/jsonschema/_validators.py#L41
    """
    if not validator.is_type(instance, "object"):
        return

    extras = set(custom_find_additional_properties(instance, schema))

    if validator.is_type(aP, "object"):
        for extra in extras:
            for error in validator.descend(instance[extra], aP, path=extra):
                yield error
    elif not aP and extras:
        if "patternProperties" in schema:
            patterns = sorted(schema["patternProperties"])
            if len(extras) == 1:
                verb = "does"
            else:
                verb = "do"
            error = "%s %s not match any of the regexes: %s" % (
                ", ".join(map(repr, sorted(extras))),
                verb,
                ", ".join(map(repr, patterns)),
            )
            yield ValidationError(error)
        else:
            error = "Additional properties are not allowed (%s %s unexpected)"
            yield ValidationError(error % extras_msg(extras))


# Extend takes a given validator, and overrides/adds the specified validators for the validator
# Thus here we are overriding Draft7Validator's `additionalProperties` validator
SchemaValidator = extend(validator=Draft7Validator, validators={"additionalProperties": customAdditionalProperties})
