from dateutil import parser


def perform_mapping(tabname, fname, value_old, value_new, chngind, fnames, return_however=True):
    if fname not in fnames or fnames[fname] is None:
        fnames[fname] = fname
    if fname == "KOSTK" and value_new == "B":
        return "Picking: Partially Processed"
    elif fname == "KOSTK" and value_new == "C":
        return "Picking: Completely Processed"
    elif fname == "GBSTK" and value_new == "B":
        return "Set Order Status: Partially Processed"
    elif fname == "GBSTK" and value_new == "C":
        return "Set Order Status: Completely Processed"
    elif fname == "KOQUK" and value_new == "B":
        return "Pick Confirmation: Partially Processed"
    elif fname == "KOQUK" and value_new == "C":
        return "Pick Confirmation: Completely Processed"
    elif fname == "LVSTK" and value_new == "B":
        return "Warehouse Management: Partially Processed"
    elif fname == "LVSTK" and value_new == "C":
        return "Warehouse Management: Completely Processed"
    elif fname == "WBSTK" and value_new == "B":
        return "Total Goods Movement: Partially processed"
    elif fname == "WBSTK" and value_new == "C":
        return "Total Goods Movement: Completely processed"
    elif fname == "TRSTA" and value_new == "B":
        return "Transportation Status: Partially processed"
    elif fname == "TRSTA" and value_new == "C":
        return "Transportation Status: Completely processed"
    elif fname == "SPE_IMWRK" and value_new == "X":
        return "Item in Plant"
    elif fname == "MPROK" and value_new == "A":
        return "Manual price change carried out"
    elif fname == "MPROK" and value_new == "B":
        return "Condition manually deleted"
    elif fname == "MPROK" and value_new == "C":
        return "Manual price change released"
    elif fname == "SPE_REV_VLSTK" and value_new == "A":
        return "Distribution Status: Relevant"
    elif fname == "SPE_REV_VLSTK" and value_new == "B":
        return "Distribution Status: Distributed"
    elif fname == "SPE_REV_VLSTK" and value_new == "C":
        return "Distribution Status: Confirmed"
    elif fname == "SPE_REV_VLSTK" and value_new == "D":
        return "Distribution Status: Planned for Distribution"
    elif fname == "SPE_REV_VLSTK" and value_new == "E":
        return "Distribution Status: Delivery split was performed locally"
    elif fname == "SPE_REV_VLSTK" and value_new == "F":
        return "Distribution Status: Change Management Switched Off"
    elif fname == "VLSTK" and value_new == "A":
        return "Distribution Status: Relevant"
    elif fname == "VLSTK" and value_new == "B":
        return "Distribution Status: Distributed"
    elif fname == "VLSTK" and value_new == "C":
        return "Distribution Status: Confirmed"
    elif fname == "VLSTK" and value_new == "D":
        return "Distribution Status: Planned for Distribution"
    elif fname == "VLSTK" and value_new == "E":
        return "Distribution Status: Delivery split was performed locally"
    elif fname == "VLSTK" and value_new == "F":
        return "Distribution Status: Change Management Switched Off"
    elif fname == "SPE_GEN_ELIKZ" and value_new == "X":
        return "Delivery Completed"
    elif fname == "IMWRK" and value_new == "X":
        return "Delivery in Plant"
    elif fname == "UVVLS" and value_new == "A":
        return "Delivery Uncompletion: Not yet processed"
    elif fname == "UVVLS" and value_new == "B":
        return "Delivery Uncompletion: Partially processed"
    elif fname == "UVVLS" and value_new == "C":
        return "Delivery Uncompletion: Completely processed"
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
            return "Set Billing Date"
        elif value_new == 0:
            return "Remove Billing Date"
        elif value_new <= value_old:
            return "Anticipate Billing Date"
        else:
            return "Postpone Billing Date"
    elif fname == "FAKSK":
        if value_new is None:
            return "Remove Billing Block"
        else:
            return "Set Billing Block"
    elif fname == "SKFBP":
        value_new = float(value_new)
        value_old = float(value_old)
        if value_new <= value_old:
            return "Decrease Cash Discount"
        else:
            return "Increase Cash Discount"
    elif fname == "UVALL" and value_new == "A":
        return "Order Uncompletion: Not yet processed"
    elif fname == "UVALL" and value_new == "B":
        return "Order Uncompletion: Partially processed"
    elif fname == "UVALL" and value_new == "C":
        return "Order Uncompletion: Completely processed"
    elif fname == "LISPL" and value_new == "A":
        return "Delivery is for single warehouse"
    elif fname == "LISPL" and value_new == "B":
        return "Delivery is not for single warehouse"
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
            return "Set Actual Goods Movement Date"
        elif value_new == 0:
            return "Remove Actual Goods Movement Date"
        elif value_new <= value_old:
            return "Anticipate Actual Goods Movement Date"
        else:
            return "Postpone Actual Goods Movement Date"
    elif fname == "KEY" and tabname == "FPLT" and chngind == "I":
        return "Insert Billing Plan"
    elif fname == "KEY" and tabname == "FPLT" and chngind == "D":
        return "Remove Billing Plan"
    elif fname == "KEY" and tabname == "VBEP" and chngind == "D":
        return "Remove Schedule Line"
    elif fname == "KEY" and tabname == "VBEP" and chngind == "I":
        return "Insert Schedule Line"
    elif fname == "KEY" and tabname == "VBAK" and chngind == "D":
        return "Cancel Order"
    elif fname == "KEY" and tabname == "VBAP" and chngind == "D":
        return "Remove Order Item"
    elif fname == "KEY" and tabname == "VBAP" and chngind == "I":
        return "Insert Order Item"
    elif fname == "KEY" and tabname == "LIPS" and chngind == "D":
        return "Remove Delivery Item"
    elif fname == "KEY" and tabname == "LIPS" and chngind == "I":
        return "Insert Delivery Item"
    else:
        if return_however:
            return "Changed "+fnames[fname]

