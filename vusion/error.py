class VusionError(Exception):
    pass


class MissingData(VusionError):
    pass


class SendingDatePassed(VusionError):
    pass


class MissingTemplate(VusionError):
    pass


class MissingField(VusionError):
    pass
