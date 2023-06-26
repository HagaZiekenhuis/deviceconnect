import os
from flask import jsonify
from flask import request
from functools import wraps
import logging

log = logging.getLogger(__name__)


def token_required(f):
    @wraps(f)
    def decorator(*args, **kwargs):
        if 'API_KEY' in request.headers:
            if request.headers['API_KEY'] == os.environ.get('API_KEY'):
                log.debug('API key match')
                return f(*args, **kwargs)
            else:
                log.debug('API key %s does not match API key')
                return jsonify({'message': 'A valid API key is missing'})
        else:
            log.debug('API key is missing')
            return jsonify({'message': 'A valid API key is missing'})
    return decorator


def generate_control_number(device_id: str):
    log.debug('Device ID: %s' % device_id)
    # Convert ID into numeric ID
    numeric_id = ''
    for char in device_id:
        if char.isnumeric():
            numeric_id += str(char)
        else:
            numeric_id += str(ord(char) - 64)
    log.debug('Numeric ID: %s' % numeric_id)

    # Split numeric ID in even and odd lists
    even_chars = []
    odd_chars = []

    for i in range(1, len(numeric_id)+1):
        if i % 2 == 0:
            even_chars.append(int(numeric_id[i-1]))
        else:
            odd_chars.append(int(numeric_id[i-1]))
    log.debug('Even chars: %s' % even_chars)
    log.debug('Odd chars: %s' % odd_chars)

    # Sum all odd position numbers
    # Sum all even position numbers and multiply this by 3
    # The control digit is 10 - the last digit, unless the last digit is 0. In this case 0 is the control digit.
    control_digit = 10 - ((sum(odd_chars) + (sum(even_chars) * 3)) % 10)
    if control_digit == 10:
        control_digit = 0
    log.debug('Control digit: %s' % str(control_digit))
    return control_digit
