import logging

logger = logging.getLogger()
from pydicom.multival import MultiValue

def serialize_sets(obj):

    if (type(obj) is bytes):
        return ""
    # pydicom.multival.MultiValue type into serialize list
    elif isinstance(obj, MultiValue):
        return obj._list
    else:
        return str(obj)


def validateDCMKeyword(elm):
  reg = r",\ |\)"
  try:      
    if elm.keyword != "":
        keyword = elm.keyword
    else:
        raise ValueError("Tag: {} Found Empty DICOM Keyword: {}  with Name: {} ".format(
            elm, elm.keyword, elm.name))
    return keyword    
  except Exception as e:
          logger.error(e)

def flatPN(elm):
    # http://dicom.nema.org/dicom/2013/output/chtml/part05/sect_6.2.html
    try:
        # For human use, the five components in their order of occurrence are: family name complex, given name complex, middle name, name prefix, name suffix.

        pnMap = {}
        if (elm.is_empty == False):
            pnMap = {
                "FamilyName": elm._value.family_name,
                "GivenName": elm._value.given_name,
                "Ideographic": elm._value.ideographic,
                "MiddleName": elm._value.middle_name,
                "NamePrefix": elm._value.name_prefix,
                "NameSuffix": elm._value.name_suffix,
                "Phonetic": elm._value.phonetic,
            }
        else:
            pnMap = {
                "FamilyName": ""
            }

        return pnMap

    except Exception as e:
        logger.error(
            "Unable to Parse Value Representation: PN for Tag {} in DCM".format(elm.tag))
        logger.exception(e)