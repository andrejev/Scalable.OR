class SORGlobalException(Exception):
    def __init__(self, message, cmd, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)

        # Remember the OR command where the error occurred
        self.cmd = cmd
        self.message = message


class SOROperationException(Exception):
    def __init__(self, message, cmd, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)

        # Remember the OR command where the error occurred
        self.cmd = cmd
        self.message = message


class SORLocalException(Exception):
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)
