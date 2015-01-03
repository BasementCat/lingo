class LingoError(Exception):
    pass

class DatabaseError(LingoError):
    pass

class NotFoundError(DatabaseError):
    pass

class ModelError(LingoError):
    pass

class ValidationError(ModelError):
    pass