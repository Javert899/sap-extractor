DOC_SEP = "@#@#@#"
MAX_CASE_SIZE = 130

ORACLE_ARRAYSIZE = 10000

TIMESTAMP_FORMAT = "%d.%m.%Y %H:%M:%S"


def set_documents(content):
    ret = []
    content = content.split(DOC_SEP)
    for x in content:
        if str(x).lower() != "nan":
            ret.append(x)
    return ret


INVOLVED_TABLES = [
    "T003T",
    "CDHDR",
    "CDPOS",
    "TSTCT",
    "DD07T",
    "BKPF",
    "BSEG",
    "VBFA",
    "EBAN",
    "EKKO",
    "EKPO",
    "MKPF",
    "MSEG"
]
