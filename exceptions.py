class ValidationException(Exception):
    def __init__(self, message=""):
        self.message = message
        super().__init__(self.message)

class ReservationException(Exception):
    def __init__(self, message=""):
        self.message = message
        super().__init__(self.message)

class StructureException(Exception):
    def __init__(self, message=""):
        self.message = message
        super().__init__(self.message)
