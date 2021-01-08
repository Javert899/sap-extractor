DOC_SEP = "@#@#@#"
MAX_CASE_SIZE = 130

ORACLE_OWNER = "SAPSR3"
ORACLE_ARRAYSIZE = 10000

TIMESTAMP_FORMAT = "%Y.%m.%d %H:%M:%S"


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
