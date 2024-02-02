import base64
import re
from typing import List, Union
from urllib.parse import unquote


def join_url_params(url, params):
    """ 
    Joins a url string with a dictionary of parameters.
    """
    if not params:
        return url

    if not isinstance(params, dict):
        raise ValueError("Error: Invalid Params Type")

    url += "?" + "&".join([f"{key}={value}" for key, value in params.items()])
    return url


def clean_text(input_text: Union[str, List[str]]) -> str:
    """
    Clean a string or each string in a list of strings by removing all tabs, whitespace, newlines, and carriage returns,
    and return a single cleaned string.

    Args:
    - input_text (str or list): The text or list of texts to be cleaned.

    Returns:
    - str: The cleaned text.
    """

    def clean_string(text):
        # Replace tabs, newlines, and carriage returns with a single space
        cleaned_text = re.sub(r'\s+', ' ', text)
        # Remove spaces at the beginning and end of the text
        cleaned_text = cleaned_text.strip()
        return cleaned_text

    if isinstance(input_text, str):
        return clean_string(input_text)
    elif isinstance(input_text, list):
        # Join the list into a single string with a space, then clean it
        joined_text = " ".join(input_text)
        return clean_string(joined_text)
    else:
        raise ValueError("Input must be a string or a list of strings")


def decode_base64_string(encoded_value):
    encoded_value = unquote(encoded_value)
    return base64.b64decode(encoded_value).decode('utf-8', 'ignore')


def encoded_base64_string(decoded_value):
    from urllib.parse import quote
    return quote(base64.b64encode(decoded_value.encode('utf-8')))
