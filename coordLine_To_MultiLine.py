import re


def format_string(input_string):
    description, coord_string = input_string.split(' 1: ')
    coord_string = '1: ' + coord_string
    coord_string = re.sub('\d+:', '\n\g<0>', coord_string)
    formatted_string = description + coord_string
    return formatted_string


input_string = input("Enter input string: ").replace('\n', '')
formatted_string = format_string(input_string.strip())
print(formatted_string)
