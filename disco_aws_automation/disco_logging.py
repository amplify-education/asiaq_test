'''Utility function for logging'''
import logging
import sys


def configure_logging(debug, silent=False):
    '''Sets the default logger and the boto logger to appropriate levels of chattiness.'''
    logger = logging.getLogger('')
    boto_logger = logging.getLogger('boto')
    botocore_logger = logging.getLogger('botocore')

    # If there are any handlers on the root logger, remove them so that if this function is called more
    # than once, we don't get the same statement logged multiple times.
    for handler in logger.handlers:
        logger.removeHandler(handler)

    if silent and debug:
        raise Exception('Debug and silent logging options are mutually exclusive')

    if silent:
        logging.disable(logging.CRITICAL)
    elif debug:
        logger.setLevel(logging.DEBUG)
        boto_logger.setLevel(logging.INFO)
        botocore_logger.setLevel(logging.DEBUG)
        debug_stream_handler = logging.StreamHandler(sys.__stderr__)
        debug_stream_handler.setLevel(logging.DEBUG)
        debug_stream_handler.setFormatter(logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s'))
        logger.addHandler(debug_stream_handler)
        boto_logger.addHandler(debug_stream_handler)
        botocore_logger.addHandler(debug_stream_handler)
    else:
        logger.setLevel(logging.INFO)
        boto_logger.setLevel(logging.CRITICAL)
        botocore_logger.setLevel(logging.CRITICAL)

    stream_handler = logging.StreamHandler(sys.__stdout__)
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s'))
    logger.addHandler(stream_handler)
