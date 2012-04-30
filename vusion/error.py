class VusionError(Exception):
    pass


class MissingData(VusionError):
    pass


class SendingDatePassed(VusionError):
    pass
