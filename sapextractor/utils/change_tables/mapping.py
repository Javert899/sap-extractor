from dateutil import parser


def perform_mapping(tabname, fname, value_old, value_new, chngind, fnames, return_however=True):
    if fname not in fnames or fnames[fname] is None:
        fnames[fname] = fname
    if fname == "KOSTK" and value_new == "B":
        return "Picking: Partially Processed", 1
    elif fname == "KOSTK" and value_new == "C":
        return "Picking: Completely Processed", 1
    elif fname == "GBSTK" and value_new == "B":
        return "Set Order Status: Partially Processed", 1
    elif fname == "GBSTK" and value_new == "C":
        return "Set Order Status: Completely Processed", 1
    elif fname == "KOQUK" and value_new == "B":
        return "Pick Confirmation: Partially Processed", 1
    elif fname == "KOQUK" and value_new == "C":
        return "Pick Confirmation: Completely Processed", 1
    elif fname == "LVSTK" and value_new == "B":
        return "Warehouse Management: Partially Processed", 1
    elif fname == "LVSTK" and value_new == "C":
        return "Warehouse Management: Completely Processed", 1
    elif fname == "WBSTK" and value_new == "B":
        return "Total Goods Movement: Partially processed", 1
    elif fname == "WBSTK" and value_new == "C":
        return "Total Goods Movement: Completely processed", 1
    elif fname == "TRSTA" and value_new == "B":
        return "Transportation Status: Partially processed", 1
    elif fname == "TRSTA" and value_new == "C":
        return "Transportation Status: Completely processed", 1
    elif fname == "SPE_IMWRK" and value_new == "X":
        return "Item in Plant", 1
    elif fname == "MPROK" and value_new == "A":
        return "Manual price change carried out", 1
    elif fname == "MPROK" and value_new == "B":
        return "Condition manually deleted", 1
    elif fname == "MPROK" and value_new == "C":
        return "Manual price change released", 1
    elif fname == "SPE_REV_VLSTK" and value_new == "A":
        return "Distribution Status: Relevant", 1
    elif fname == "SPE_REV_VLSTK" and value_new == "B":
        return "Distribution Status: Distributed", 1
    elif fname == "SPE_REV_VLSTK" and value_new == "C":
        return "Distribution Status: Confirmed", 1
    elif fname == "SPE_REV_VLSTK" and value_new == "D":
        return "Distribution Status: Planned for Distribution", 1
    elif fname == "SPE_REV_VLSTK" and value_new == "E":
        return "Distribution Status: Delivery split was performed locally", 1
    elif fname == "SPE_REV_VLSTK" and value_new == "F":
        return "Distribution Status: Change Management Switched Off", 1
    elif fname == "VLSTK" and value_new == "A":
        return "Distribution Status: Relevant", 1
    elif fname == "VLSTK" and value_new == "B":
        return "Distribution Status: Distributed", 1
    elif fname == "VLSTK" and value_new == "C":
        return "Distribution Status: Confirmed", 1
    elif fname == "VLSTK" and value_new == "D":
        return "Distribution Status: Planned for Distribution", 1
    elif fname == "VLSTK" and value_new == "E":
        return "Distribution Status: Delivery split was performed locally", 1
    elif fname == "VLSTK" and value_new == "F":
        return "Distribution Status: Change Management Switched Off", 1
    elif fname == "SPE_GEN_ELIKZ" and value_new == "X":
        return "Delivery Completed", 1
    elif fname == "IMWRK" and value_new == "X":
        return "Delivery in Plant", 1
    elif fname == "UVVLS" and value_new == "A":
        return "Delivery Uncompletion: Not yet processed", 1
    elif fname == "UVVLS" and value_new == "B":
        return "Delivery Uncompletion: Partially processed", 1
    elif fname == "UVVLS" and value_new == "C":
        return "Delivery Uncompletion: Completely processed", 1
    elif fname == "FKDAT":
        try:
            value_new = parser.parse(value_new).timestamp()
        except:
            value_new = 0
        try:
            value_old = parser.parse(value_old).timestamp()
        except:
            value_old = 0
        if value_old == 0:
            return "Set Billing Date", 1
        elif value_new == 0:
            return "Remove Billing Date", 1
        elif value_new <= value_old:
            return "Anticipate Billing Date", 1
        else:
            return "Postpone Billing Date", 1
    elif fname == "FAKSK":
        if value_new is None:
            return "Remove Billing Block", 1
        else:
            return "Set Billing Block", 1
    elif fname == "SKFBP":
        value_new = float(value_new)
        value_old = float(value_old)
        if value_new <= value_old:
            return "Decrease Cash Discount", 1
        else:
            return "Increase Cash Discount", 1
    elif fname == "UVALL" and value_new == "A":
        return "Order Uncompletion: Not yet processed", 1
    elif fname == "UVALL" and value_new == "B":
        return "Order Uncompletion: Partially processed", 1
    elif fname == "UVALL" and value_new == "C":
        return "Order Uncompletion: Completely processed", 1
    elif fname == "LISPL" and value_new == "A":
        return "Delivery is for single warehouse", 1
    elif fname == "LISPL" and value_new == "B":
        return "Delivery is not for single warehouse", 1
    elif fname == "WADAT_IST":
        try:
            value_new = parser.parse(value_new).timestamp()
        except:
            value_new = 0
        try:
            value_old = parser.parse(value_old).timestamp()
        except:
            value_old = 0
        if value_old == 0:
            return "Set Actual Goods Movement Date", 1
        elif value_new == 0:
            return "Remove Actual Goods Movement Date", 1
        elif value_new <= value_old:
            return "Anticipate Actual Goods Movement Date", 1
        else:
            return "Postpone Actual Goods Movement Date", 1
    elif fname == "KEY" and tabname == "FPLT" and chngind == "I":
        return "Insert Billing Plan", 2
    elif fname == "KEY" and tabname == "FPLT" and chngind == "D":
        return "Remove Billing Plan", 2
    elif fname == "KEY" and tabname == "VBEP" and chngind == "D":
        return "Remove Schedule Line", 2
    elif fname == "KEY" and tabname == "VBEP" and chngind == "I":
        return "Insert Schedule Line", 2
    elif fname == "KEY" and tabname == "VBAK" and chngind == "D":
        return "Cancel Order", 2
    elif fname == "KEY" and tabname == "VBAP" and chngind == "D":
        return "Remove Order Item", 2
    elif fname == "KEY" and tabname == "VBAP" and chngind == "I":
        return "Insert Order Item", 2
    elif fname == "KEY" and tabname == "LIPS" and chngind == "D":
        return "Remove Delivery Item", 2
    elif fname == "KEY" and tabname == "LIPS" and chngind == "I":
        return "Insert Delivery Item", 2
    else:
        if return_however:
            return "Changed "+fnames[fname], 0
    return None, None

