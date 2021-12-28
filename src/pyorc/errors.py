class ORCError(Exception):
    """ General pyorc error. """


class ParseError(ORCError):
    """ Parse error while processing an ORC file. """

