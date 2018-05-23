class MessagedException(Exception):
    """
    Exception with message
    """
    def __init__(self, message, *args):
        self.message = message
        super(MessagedException, self).__init__(message, *args)
