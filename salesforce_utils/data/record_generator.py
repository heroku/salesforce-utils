__author__ = 'spersinger'
import re
import pdb
import string
import random
import os.path
from enum import Enum
from itertools import imap
from functools import partial
from datetime import datetime, timedelta

# Field types

def load_file(name):
    return [line.rstrip() for line in open(os.path.join(os.path.dirname(__file__), "sources/%s" % name))]

NAMES = load_file("names.txt")
PLACES = load_file("place_names.txt")
COUNTRIES = load_file("countries.txt")
DOMAINS = load_file("domains.txt")
WORDS = load_file("english_words.csv")
STATES = load_file("states.txt")

STREET_TYPES = ["Street","Avenue","Boulevard","Camino","Lane"]


types = """
    name
    words
    titlewords
    string
    date
    float
    int
    bool
    city
    street
    state
    country
    email
    zip
    phone
    datetime
    datenow
    datetimenow
    datepast
    datetimepast
    datefuture
    datetimefuture
    website
"""

TYPES = Enum(*types.split())
FIELD_VALUE_FUNCS = dict()

def define_lookup(type_name, random_choices=None, callable = None):
    if random_choices:
        FIELD_VALUE_FUNCS[type_name] = partial(random.choice, random_choices)
    elif callable:
        FIELD_VALUE_FUNCS[type_name] = callable


def mock_record(field_list, defaults={}, count=None):
    """Generates fake data to create a record. You pass in the list of fields you
        want and a dict is returned with sample data. A field can be either a
        string or a dict mapping the field name to a type. If you just supply
        the field name then we will try to guess the right data to supply for
        that field. Substrings like 'name', 'id', 'date', 'count' will be recognized
        to infer the type. Otherwise you can supply one of the following types:
            field_name => type

        where 'type' can be:
           name: A name word (will choose randomly from first or last)
           words: A sequence of 2-4 random words
           titlewords: Titlecase words
           string:[len] : A random string between 10 and 40 chars, or the length given
           datenow             :Current date
           datetimenow         :Current date and time
           datepast            :A date from the past
           datetimepast        :A date and time from the past
           datefuture          :A date in the future
           datetimefuture      :A date and time in the future
           float               :A random floating point number
           int                 :A random integer
           city                :A city name
           street              :A street name
           state               :A state name
           country             :A country code
           zip                 :A zip code
           phone               :A phone number
           email               :An email address
           bool                :True or False
           website             :Random web site

        Note that you can use more than one type connected by separators:

           "int street, city" -> 500 Main Street, San Francisco
    """
    if isinstance(field_list, dict):
        return dict([generate_field({key:value}) for key,value in field_list.iteritems()])
    else:
        return dict([generate_field(field, defaults) for field in field_list])

def mock_records(field_list, defaults={}, count=None):
    for x in xrange(count):
        yield mock_record(field_list, defaults=defaults)


MODIFIERS = {
    "upper":lambda x: x.upper(),
    "lower":lambda x: x.lower()
}

def generate_field(field_spec, defaults = {}):
    if "=>" in field_spec:
        name, type = field_spec.split("=>")
        name = name.strip()
        type = type.strip()
    elif isinstance(field_spec, basestring):
        name = field_spec
        type = guess_field_type(name)
        #print "Guessed field '%s' is type %s" % (name, type)
    else:
        name = field_spec.keys()[0]
        type = field_spec[name]

    if 'name' in defaults:
        value = defaults['name']
    elif type in TYPES:
        value = field_value(type)
    else:
        result = []
        for m in re.finditer(r"([\w|]+)([ ,\.;\-_\t]*)", type):
            result.append(field_value(m.group(1)))
            if m.group(2):
                result.append(m.group(2))
        value = result[0] if len(result) == 1 else "".join(imap(str, result))

    return name, value


def guess_field_type(name):
    down = name.lower()

    if name.startswith("Is"):
        return TYPES.bool
    elif name.endswith("Id"):
        return "string:18"
    elif 'name' in down:
        return TYPES.name
    elif 'date' in down:
        return TYPES.datetime
    elif 'street' in down:
        return TYPES.street
    elif 'city' in down:
        return TYPES.city
    elif 'state' in down:
        return TYPES.state
    elif 'country' in down:
        return TYPES.country
    elif 'email' in down:
        return TYPES.email
    elif 'phone' in down:
        return TYPES.phone
    else:
        return TYPES.string

def lookup_type(type):
    try:
        return [e for e in TYPES if e == type or str(e) == type or
                                (isinstance(type,basestring) and str(e) == type.split("|")[0])][0]
    except IndexError:
        return type


def field_value(type):
    len = None
    modifiers = []
    if isinstance(type, basestring) and "|" in type:
        parts = type.split("|")
        type = parts[0]
        modifiers = parts[1:]

    try:
        val = FIELD_VALUE_FUNCS[lookup_type(type)]()
        for m in modifiers:
            if m in MODIFIERS:
                val = MODIFIERS[m](val)
            else:
                if isinstance(val, (int, float, basestring)):
                    val = str(val)[0:int(m)]
                else:
                    val = " ".join(val[0:int(m)])
        return val
    except (KeyError, IndexError):
        return type
    except TypeError:
        print "Error trying to get field of type '{0}'".format(type)
        raise

def typegen(type):
    def save_func(f):
        FIELD_VALUE_FUNCS[type] = f
        return f
    return save_func

@typegen(TYPES.string)
def genstring():
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(4000))

@typegen(TYPES.int)
def genint():
    return random.randint(0, 99999999)

@typegen(TYPES.float)
def genfloat():
    return random.random()*99999999.0

@typegen(TYPES.name)
def genname():
    return random.choice(NAMES)

@typegen(TYPES.words)
def genwords():
    return [random.choice(WORDS) for x in xrange(10)]

@typegen(TYPES.titlewords)
def genwords():
    return [random.choice(WORDS).capitalize() for x in xrange(5000)]

@typegen(TYPES.city)
def gencity():
    return random.choice(PLACES)

@typegen(TYPES.street)
def genstreet():
    return random.choice(PLACES) + " " + random.choice(STREET_TYPES)

@typegen(TYPES.phone)
def genphone():
    return "%03d-%03d-%04d" % (random.randint(100,999), random.randint(100,999), random.randint(1000,9999))

@typegen(TYPES.datetime)
def gendatetime():
    offset = random.randint(-100000, 0)
    return (datetime.now() + timedelta(hours=offset)).strftime('%Y-%m-%dT%H:%M:%S.000Z')

@typegen(TYPES.date)
def gendate():
    offset = random.randint(-100000, 0)
    return (datetime.now() + timedelta(hours=offset)).strftime("%Y-%m-%d")

@typegen(TYPES.country)
def genname():
    return random.choice(COUNTRIES)

@typegen(TYPES.state)
def genstate():
    return random.choice(STATES)

@typegen(TYPES.email)
def genemail():
    return random.choice(NAMES).lower() + "." + random.choice(NAMES).lower() + "@" + random.choice(DOMAINS)

@typegen(TYPES.bool)
def genbool():
    return random.choice(["1","0"])

@typegen(TYPES.website)
def genwebsite():
    return "http://www." + random.choice(DOMAINS)
