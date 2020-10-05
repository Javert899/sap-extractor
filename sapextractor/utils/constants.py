DOC_SEP = "@#@#@#"
MAX_CASE_SIZE = 130


def set_documents(content):
    ret = []
    content = content.split(DOC_SEP)
    for x in content:
        if str(x).lower() != "nan":
            ret.append(x)
    return ret
