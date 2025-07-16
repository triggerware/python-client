import re
from typing import Literal


class TriggerwareClientException(Exception):
    def __init__(self, message: str):
        super().__init__(message)
        self.message = message
    pass


class InvalidQueryException(TriggerwareClientException):
    pass


class PolledQueryException(TriggerwareClientException):
    pass


class PreparedQueryException(TriggerwareClientException):
    pass


class SubscriptionException(TriggerwareClientException):
    pass


class RelDataElement:
    name: str
    signature_names: list[str]
    signature_types: list[str]
    usage: str
    no_idea: list[str]
    description: str


class RelDataGroup:
    name: str
    symbol: str
    elements: list[RelDataElement]


class PolledQueryControlParameters:
    report_unchanged: bool 
    report_initial: str
    delay: bool

    def __init__(
        self,
        report_unchanged: bool = False,
        report_initial: Literal["none"] | Literal["with delta"] | Literal["without delta"] = "none",
        delay: bool = False,
    ):
        self.report_unchanged = report_unchanged
        self.report_initial = report_initial
        self.delay = delay


class PolledQueryCalendarSchedule:
    days: str
    hours: str
    minutes: str
    months: str
    timezone: str
    weekdays: str
    _TIMEZONE_REGEX = re.compile(r"^[A-Za-z]+(?:_[A-Za-z]+)*(?:/[A-Za-z]+(?:_[A-Za-z]+)*)*$")

    def __init__(
        self,
        days: str = "*",
        hours: str = "*",
        minutes: str = "*",
        months: str = "*",
        timezone: str = "UTC",
        weekdays: str = "*",
    ):
        self.days = days
        self.hours = hours
        self.minutes = minutes
        self.months = months
        self.timezone = timezone
        self.weekdays = weekdays

    @staticmethod
    def _validate_time(unit: str, value: str, min: int, max: int):
        if value == "*":
            return

        parts = [y for x in re.split(",", value) for y in re.split("-", x)]
        for part in parts:
            try:
                parsed = int(part)
            except ValueError:
                raise PolledQueryException(f"Invalid {unit} value: {part}")
            if parsed < min or parsed > max:
                raise PolledQueryException(f"{unit} value out of range: {part}")

    def validate(self):
        self._validate_time("day", self.days, 1, 31)
        self._validate_time("hour", self.hours, 0, 23)
        self._validate_time("minute", self.minutes, 0, 59)
        self._validate_time("month", self.months, 1, 12)
        self._validate_time("weekday", self.weekdays, 0, 6)
        if not self._TIMEZONE_REGEX.match(self.timezone):
            raise PolledQueryException("Invalid timezone format")

type PolledQuerySchedule = int | PolledQueryCalendarSchedule | list[PolledQuerySchedule]
