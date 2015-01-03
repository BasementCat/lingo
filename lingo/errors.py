class LingoError(Exception):
    pass

class DatabaseError(LingoError):
    pass

class ModelError(LingoError):
    pass

class ValidationError(ModelError):
    pass