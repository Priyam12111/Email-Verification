class EngineCreationFailed(Exception):
    pass

class ElementNotFound(Exception):
    pass

class AbortProcess(Exception):
    def __init__(self, message="INFO: Abort processing ..."):
        self.message = message
        print(self.message)
        super().__init__(self.message)