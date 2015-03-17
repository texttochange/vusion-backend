class VusionError(Exception):
    pass


class MissingData(VusionError):

    failed_content = None

    def __init__(self, error, failed_content=None):
        super(MissingData, self).__init__(error)
        self.failed_content = failed_content

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
