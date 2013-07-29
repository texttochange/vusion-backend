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


class InvalidField(VusionError):
    pass


class FailingModelUpgrade(VusionError):
    pass


class WrongModelInstanciation(VusionError):
    pass


class MissingProperty(VusionError):
    pass


class MissingCode(MissingProperty):
    pass


class MissingLocalTime(MissingProperty):
    pass
