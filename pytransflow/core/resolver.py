"""
Defines classes and methods related to ``Resolver``
"""

import re
from typing import Optional
from pytransflow.core.configuration import TransflowConfiguration
from pytransflow.core.flow.variables import FlowVariables


class Resolver:
    """Implements methods for resolving dynamic configuration"""

    @staticmethod
    def resolve_condition(expression: str, variables: Optional[FlowVariables]) -> str:
        """Resolves conditions for record fields and flow variables

        Args:
            expression: Expression
            variables: Flow Variables

        Returns:
            Resolved expression

        """
        expression = Resolver.resolve_field_records(expression)
        if variables is not None:
            expression = Resolver.resolve_flow_variables(expression, variables)
        return expression

    @staticmethod
    def resolve_field_records(expression: str) -> str:
        """Resolves expression field record variable names

        Args:
            expression: Expression

        Returns:
            Resolved expression

        """
        result = expression
        path_separator = TransflowConfiguration().path_separator
        regex = re.compile(r"(\@[\w" + re.escape(path_separator) + r"]+)")
        for match in re.findall(regex, expression):
            match = match.replace("@", "")
            split = match.split(path_separator)
            joined = "".join(f"['{x}']" for x in split)
            result = result.replace(f"@{match}", f"record{joined}")
        return result

    @staticmethod
    def resolve_flow_variables(expression: str, variables: FlowVariables) -> str:
        """Resolves expression flow variable values

        Args:
            expression: Expression
            variables: Flow Variables

        Returns:
            Resolved expressions

        """
        result = expression
        regex = re.compile(r"(\!\:[\w]+)")
        for match in re.findall(regex, expression):
            match = match.replace("!:", "")
            value = variables.get_variable(match)
            if isinstance(value, str):
                result = result.replace(f"!:{match}", f"'{value}'")
            if isinstance(value, (int, float)):
                result = result.replace(f"!:{match}", str(value))
        return result
