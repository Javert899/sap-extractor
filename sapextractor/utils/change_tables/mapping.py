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
    elif fname == "FAKSK":
        if value_new is None:
            return "Remove Billing Block", 2
        else:
            return "Set Billing Block", 2
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
    elif fname == "PROCSTAT" and value_new == "5":
        return "Purchasing document processing state: Release completed", 1
    elif fname == "PROCSTAT" and value_new == "3":
        return "Purchasing document processing state: In release", 1
    elif fname == "ELIKZ" and value_new == "X":
        return "Delivery Completed Indicator: Yes", 1
    elif fname == "STATU" and value_new == "A" and tabname == "EKKO":
        return "Status of Purchasing Document: RFQ with Quotation", 1
    elif fname == "STATU" and value_new == "A" and tabname == "EKPO":
        return "Status of Purchasing Document: Quotation Exists in Case of RFQ Item", 1
    elif fname == "PROCSTAT" and value_new == "2":
        return "Purchasing document processing state: Active", 1
    elif fname == "PROCSTAT" and value_new == "1":
        return "Purchasing document processing state: Version in process", 1
    elif fname == "RBSTAT" and value_new == "5":
        return "Invoice document status: Posted", 1
    elif fname == "RBSTAT" and value_new == "B":
        return "Invoice document status: Parked and completed", 1
    elif fname == "RBSTAT" and value_new == "B":
        return "Invoice document status: Parked", 1
    elif fname == "WEUNB" and value_new == "B":
        return "Goods Receipt, Non-Valuated: Yes", 1
    elif fname == "XMWST" and value_new == "B":
        return "Calculate tax automatically: Yes", 1
    elif tabname == "EKPO" and fname == "AEDAT":
        return "Change Date of the Last Document Change", 1
    elif tabname == "EKPO" and fname == "LOEKZ":
        return "Change Deletion indicator in purchasing document", 1
    elif tabname == "EBAN" and fname == "LOEKZ":
        return "Change Deletion indicator in purchasing document", 1
    elif tabname == "EKKO" and fname == "FRGKE":
        return "Change Release Indicator: Purchasing Document", 1
    elif tabname == "EKKO" and fname == "FRGZU":
        return "Change Release State", 1
    elif tabname == "EKKO" and fname == "RLWRT":
        return "Change Total value at time of release", 1
    elif tabname == "EBAN" and fname == "FRGZU":
        return "Change Release State", 1
    elif tabname == "EBAN" and fname == "RLWRT":
        return "Change Total value at time of release", 1
    elif tabname == "EBAN" and fname == "FRGKZ":
        return "Change Indicator: Release Required", 1
    elif tabname == "EBAN" and fname == "MENGE":
        return "Change Quantity", 1
    elif tabname == "EKPO" and fname == "FIPOS":
        return "Change Commitment Item", 1
    elif tabname == "EKPO" and fname == "WERKS":
        return "Change Plant", 1
    elif tabname == "EKPO" and fname == "MENGE":
        return "Change Quantity", 1
    elif tabname == "EKET" and fname == "MENGE":
        return "Change Quantity", 1
    elif tabname == "EKPO" and fname == "BPRME":
        return "Change Order Price Unit (Purchasing)", 1
    elif tabname == "EKPO" and fname == "KTWRT":
        return "Change Target Value for Header Area per Distribution", 1
    elif tabname == "EBAN" and fname == "MEINS":
        return "Change Base Unit of Measure", 1
    elif tabname == "EBKN" and fname == "MENGE":
        return "Change Quantity", 1
    elif tabname == "EKKN" and fname == "MENGE":
        return "Change Quantity", 1
    elif tabname == "EKKO" and fname == "ZTERM":
        return "Change Terms of Payment Key", 1
    elif tabname == "EBAN" and fname == "EKORG":
        return "Change Purchasing Organization", 1
    elif fname == "XRUEB":
        return "Previous Period Posting", 1
    elif fname == "SUBMI":
        return "Collective Number Information", 1
    elif fname == "MWSKZ":
        return "Tax code information", 1
    elif fname == "MEMORYTYPE":
        return "Category of Incompleteness", 1
    elif fname == "MEMORY":
        return "Completeness Status", 1
    elif fname == "LABNR":
        return "Order Acknowledgement", 1
    elif fname == "MENGE":
        return "Changed Quantity", 1
    elif fname == "KTWRT":
        return "Target Val. Information", 1
    elif fname == "KDATE":
        return "Validity Period End", 1
    elif fname == "FRGSX":
        return "Release Strategy Information", 1
    elif fname == "EREKZ":
        return "Final Invoice information", 1
    elif fname == "BKLAS":
        return "Valuation Class information", 1
    elif fname == "WRBTR":
        return "Changed Amount", 1
    elif fname == "PROCSTAT":
        return "Change in Process Status", 1
    elif fname == "BVTYP":
        return "Change in Partner Bank Type", 1
    elif fname == "RBSTAT":
        return "Change Invoice Document Status", 1
    elif fname == "SPGRM":
        return "Change Blocking Reason", 1
    elif fname == "WMWST1":
        return "Change Tax Amount", 1
    elif fname == "ZTERM":
        return "Change Terms of Payment", 1
    elif fname == "PRCTR":
        return "Change Profit Center", 1
    elif fname == "SHKZG":
        return "Change Debit/Credit Indicator", 1
    elif fname == "STAPO":
        return "Indicated Deletion of an Item", 1
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
    elif fname == "KEY" and tabname == "EKPA" and chngind == "I":
        return "Associate Partner", 2
    elif fname == "KEY" and tabname == "EKET" and chngind == "I":
        return "Scheduling Agreement", 2
    elif fname == "KEY" and tabname == "RBTX" and chngind == "D":
        return "Remove Taxation", 2
    elif fname == "KEY" and tabname == "EKPO" and chngind == "I":
        return "Insert Purchase Order Item", 2
    elif fname == "KEY" and tabname == "EKKO" and chngind == "I":
        return "Update Purchase Order", 2
    elif fname == "KEY" and tabname == "EKKN" and chngind == "I":
        return "Assign PO Account", 2
    elif fname == "KEY" and tabname == "EKBN" and chngind == "I":
        return "Assign PR Account", 2
    elif fname == "KEY" and tabname == "EBAN" and chngind == "I":
        return "Update Purchase Requisition", 2
    elif fname == "KEY" and tabname == "EKES" and chngind == "I":
        return "Vendor Confirmation", 2
    elif fname == "KEY" and tabname == "EKKN" and chngind == "D":
        return "Remove PO Account", 2
    elif fname == "KEY" and tabname == "RBTX" and chngind == "I":
        return "Insert Taxation", 2
    elif fname == "KEY" and chngind == "I":
        return "Insert Row in "+tabname, 2
    elif fname == "KEY" and chngind == "D":
        return "Deleted Row in "+tabname, 2
    elif fname == "KEY" and chngind == "U":
        return "Updated Row in "+tabname, 2
    else:
        if return_however:
            return "Changed "+fnames[fname], 0
    return None, None
