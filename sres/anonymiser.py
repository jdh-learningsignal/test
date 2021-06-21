from flask import session
from random import choice
from hashlib import sha1

# names generated from www.behindthename.com

FIRST_NAMES = [
    "Aki",
    "Logan",
    "Kondwani",
    "Zoja",
    "Regan",
    "Kastor",
    "Loki",
    "Jamyang",
    "Marianna",
    "Carles",
    "Zemfira",
    "Channah",
    "Gronw",
    "Viktorie",
    "Daina",
    "Genevra",
    "Padmini",
    "Aineias",
    "Nerva",
    "Nilima",
    "Halim",
    "Walter",
    "Iolanda",
    "Konjit",
    "Dražen",
    "Casey",
    "Miloslav",
    "Prasad",
    "Aldegonda",
    "Salomon",
    "Nikolaj",
    "Mozhgan",
    "Benoît",
    "Eros",
    "Helmo",
    "Zane",
    "Yefim",
    "Dori",
    "Urania",
    "Elli",
    "Pali",
    "Malati",
    "Aage",
    "Elena",
    "Septimus",
    "Sadeq",
    "Petroula",
    "Timaeus",
    "Damjana",
    "Brando",
    "Coilean",
    "Kerneels",
    "Mohini",
    "Plinius",
    "Timur",
    "Swati",
    "Enid",
    "Reinald",
    "Bermet",
    "Theodor",
    "Leif",
    "Yves",
    "Wangi",
    "Mariapia",
    "Livia",
    "Natálie",
    "Jenifer",
    "Arkadiy",
    "Valerija",
    "Natália",
    "Dominik",
    "Andon",
    "Frederica",
    "Arya",
    "Natanail",
    "Tekla",
    "Bratoslav",
    "Platon",
    "Rachel",
    "Branden",
    "Azubah",
    "Iairus",
    "Burcu",
    "Chloé",
    "Lukas",
    "Sara",
    "Kasper",
    "Gwendoline",
    "Terenti",
    "Leocádia",
    "Hideki",
    "Jirou",
    "Kichirou",
    "Gorou",
    "Kazuko",
    "Kenzō",
    "Katsu",
    "Tarou",
    "Michiko",
    "Zhen",
    "Yawen",
    "Mei",
    "Xiulan",
    "Yin",
    "Yun",
    "Ju",
    "Shui",
    "Lian",
    "Da",
    "Bo",
    "Wen",
    "Rong",
    "Huang",
    "Jingyi",
    "Xiang",
    "Zedong",
    "Marjory",
    "Nereida",
    "Luciano",
    "Maya",
    "Lindsey",
    "Anh",
    "Tuyến",
    "Andra",
    "Roxane",
    "Rosalyn",
    "Hường",
    "Mai",
    "Beverly",
    "Dillon",
    "Ning",
    "Darell",
    "Russell",
    "Lady",
    "Jacqueline",
    "Esmee"
]

SURNAMES = [
    "Adesso",
    "Janssens",
    "Kumar",
    "Vollan",
    "Beumer",
    "Brunetti",
    "Uccello",
    "Conner",
    "Guerrero",
    "Wirth",
    "Verona",
    "Simon",
    "Batts",
    "Brankovič",
    "Ray",
    "Torosyan",
    "Zsoldos",
    "Abel",
    "Slusser",
    "Du",
    "Borisov",
    "Fitzpatrick",
    "Isaksson",
    "O'Callaghan",
    "Pottinger",
    "Poingdestre",
    "Mac Maghnuis",
    "MacChruim",
    "Costa",
    "Fresel",
    "Grenville",
    "Trapani",
    "Murray",
    "Pohl",
    "Jennings",
    "Beneš",
    "Cavanaugh",
    "Krause",
    "Porcher",
    "Myška",
    "Ward",
    "Yu",
    "Abney",
    "Lazzari",
    "Gupta",
    "Blum",
    "Abbasi",
    "Crouch",
    "Valenta",
    "Geelen",
    "Chaplin",
    "Papke",
    "Piontek",
    "Armati",
    "Winston",
    "Nevin",
    "Franco",
    "Barrett",
    "Kalmár",
    "O'Boyle",
    "Bjarnesen",
    "Bernard",
    "Gilbert",
    "Chow",
    "Parisi",
    "Otten",
    "Gori",
    "Ruggeri",
    "Pace",
    "Pavia",
    "Akerman",
    "MacBride",
    "Devine",
    "Rimmer",
    "Ó Donndubháin",
    "Neumann",
    "Boyadzhiev",
    "Duffy",
    "Jóhannsson",
    "Fukui",
    "Elmer",
    "Sargent",
    "Blanchet",
    "Kopitar",
    "Wirner",
    "Tyson",
    "Stone",
    "Bazzoli",
    "Tanaka",
    "Takeuchi",
    "Arima",
    "Kobayashi",
    "Itō",
    "Moriyama",
    "Yuan",
    "Kanda",
    "Ogawa",
    "Man",
    "Chen",
    "Ng",
    "Hou",
    "Yuen",
    "Wang",
    "Han",
    "Chu",
    "Kwok",
    "Bai",
    "Fan",
    "Zheng",
    "Song",
    "Chong",
    "Lim",
    "Kuang",
    "Liao",
    "Downer",
    "Street",
    "Colton",
    "Cason",
    "Hayter",
    "Bone",
    "Mills",
    "Nguyen",
    "Mitchell",
    "Everly",
    "Merrill",
    "Kerry",
    "Paredes",
    "Elwyn",
    "Danielson",
    "Nelson",
    "Huddleston",
    "Salmon",
    "Anson",
    "Fabian"
]

def get_random_firstname():
    return choice(FIRST_NAMES)

def get_random_surname():
    return choice(SURNAMES)

def name_to_pseudo(firstname=None, surname=None):
    _firstname = None
    if firstname is not None:
        _hash = sha1(firstname.encode('UTF-8')).hexdigest()
        i = int( int(_hash[0:2], 16) / 2 )
        _firstname = FIRST_NAMES[i]
    _surname = None
    if surname is not None:
        _hash = sha1(surname.encode('UTF-8')).hexdigest()
        i = int( int(_hash[0:2], 16) / 2 )
        _surname = SURNAMES[i]
    if _firstname and _surname:
        return f"{_firstname} {_surname}"
    elif _firstname:
        return _firstname
    elif _surname:
        return _surname
    else:
        return ''

def _mask(s):
    s = str(s)
    try:
        return s[0] + ('*' * (len(s) - 1))
    except:
        return '**??**'

def name_to_mask(names):
    names = names.split(' ')
    ret = []
    for name in names:
        try:
            ret.append(_mask(name))
        except:
            pass
    return ' '.join(ret)

def email_to_mask(email):
    parts = email.split('@')
    try:
        return f"{_mask(parts[0])}@{''.join(parts[1:])}"
    except:
        return '***@***.***'

def identifier_to_hash(identifier):
    _hash = sha1(str(identifier).encode('UTF-8')).hexdigest()
    return _hash[0:len(identifier)+1]

def is_identity_anonymiser_active():
    try:
        return session.get('identity_anonymiser_active', False)
    except:
        return False

def anonymise_identifier(identifier):
    if '@' in identifier:
        return email_to_mask(identifier)
    else:
        return identifier_to_hash(identifier)
    
def anonymise(field_name, original_data):
    if field_name in ['preferred_name', 'given_names']:
        return name_to_pseudo(firstname=original_data)
    elif field_name == 'full_name':
        split_name = original_data.split(' ')
        if len(split_name) == 0:
            return '**'
        elif len(split_name) == 1:
            return name_to_pseudo(firstname=split_name[0])
        else:
            return name_to_pseudo(firstname=split_name[0]) + ' ' + name_to_pseudo(surname=split_name[-1])
    elif field_name == 'surname':
        return name_to_pseudo(surname=original_data)
    elif field_name in ['email', 'display_email']:
        return email_to_mask(original_data)
    elif field_name in ['sid', 'display_sid', 'username', 'alternative_id1', 'alternative_id2']:
        return identifier_to_hash(original_data)
    else:
        # not an anonymisable field
        return original_data

def anonymise_within_content(content, student_data):
    """Tries to anonymise strings within supplied content.
        
        content (str)
        student_data (StudentData) instantiated.
    """
    from sres.studentdata import NAME_FIELDS, IDENTIFIER_FIELDS

    for field in NAME_FIELDS + IDENTIFIER_FIELDS:
        original_value = student_data.config.get(field, '')
        if original_value and original_value in content:
            replacement_value = anonymise(field, original_value)
            content = content.replace(original_value, replacement_value)
    return content
        
    


