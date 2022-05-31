import os.path
import sys

sys.path.insert(0, "../..")

import json

parameters = json.load(open("parameters.json", "r"))

import pandas as pd
import pm4py
from pm4py.objects.ocel.obj import OCEL


events = []
objects = []
relations = []

added_objects = set()


def read_dataframe_po():
    dataframe = pd.read_csv("dataframe_po.csv", dtype=str)
    dataframe["AEDAT"] = pd.to_datetime(dataframe["AEDAT"], format=parameters["date_column_format"], errors="coerce")
    dataframe = dataframe.dropna(subset=["AEDAT"])
    dataframe["EID"] = "EKKOROW_" + dataframe.index.astype(str)
    dataframe["EBELN"] = "EBELN_" + dataframe["MANDT"] + "_" + dataframe["EBELN"]
    dataframe["EBELNEBELP"] = "EBELNEBELP_" + dataframe["MANDT"] + "_" +dataframe["EBELNEBELP"]
    dictio_ebelns = dataframe.groupby("EID")["EBELN"].apply(set).to_dict()
    dictio_ebelnebelps = dataframe.groupby("EID")["EBELNEBELP"].apply(set).to_dict()
    ordering = 2

    for index, row in dataframe.iterrows():
        activity = "Create Purchase Order Item"
        timestamp = row["AEDAT"]
        eid = row["EID"]

        this_objects = {}
        for ob in dictio_ebelns[eid]:
            obtype = ob.split("_")[0]
            this_objects[ob] = obtype
        for ob in dictio_ebelnebelps[eid]:
            obtype = ob.split("_")[0]
            this_objects[ob] = obtype

        events.append({"ocel:eid": eid, "ocel:activity": activity, "ocel:timestamp": timestamp, "ocel:omap": list(this_objects), "ordering": ordering})

        for ob in this_objects:
            if ob not in added_objects:
                added_objects.add(ob)
                objects.append({"ocel:oid": ob, "ocel:type": this_objects[ob]})
            relations.append({"ocel:eid": row["EID"], "ocel:activity": activity, "ocel:timestamp": timestamp, "ocel:oid": ob, "ocel:type": this_objects[ob], "ordering": ordering})


def read_dataframe_pr():
    dataframe = pd.read_csv("dataframe_pr.csv", dtype=str)
    dataframe.columns = [x.split(".")[-1] for x in dataframe.columns]
    dataframe["BADAT"] = pd.to_datetime(dataframe["BADAT"], format=parameters["date_column_format"], errors="coerce")
    dataframe = dataframe.dropna(subset=["BADAT"])
    dataframe["EID"] = "EBANROW_" + dataframe.index.astype(str)
    dataframe["EBELN"] = "EBELN_" + dataframe["MANDT"] + "_" +dataframe["EBELN"]
    dataframe["EBELNEBELP"] = "EBELNEBELP_" + dataframe["MANDT"] + "_" +dataframe["EBELNEBELP"]
    dataframe["BANFN"] = "BANFN_" + dataframe["MANDT"] + "_" +dataframe["BANFN"]
    dataframe["BANFNBNFPO"] = "BANFNBNFPO_" + dataframe["MANDT"] + "_" +dataframe["BANFNBNFPO"]
    dictio_ebelns = dataframe.dropna(subset=["EBELN"]).groupby("EID")["EBELN"].apply(set).to_dict()
    dictio_ebelnebelps = dataframe.dropna(subset=["EBELNEBELP"]).groupby("EID")["EBELNEBELP"].apply(set).to_dict()
    dictio_banfns = dataframe.groupby("EID")["BANFN"].apply(set).to_dict()
    dictio_banfnbnfpos = dataframe.groupby("EID")["BANFNBNFPO"].apply(set).to_dict()
    ordering = 1

    for index, row in dataframe.iterrows():
        activity = "Create Purchase Requisition Item"
        timestamp = row["BADAT"]
        eid = row["EID"]

        this_objects = {}
        if eid in dictio_ebelns:
            for ob in dictio_ebelns[eid]:
                obtype = ob.split("_")[0]
                this_objects[ob] = obtype
            for ob in dictio_ebelnebelps[eid]:
                obtype = ob.split("_")[0]
                this_objects[ob] = obtype
        for ob in dictio_banfns[eid]:
            obtype = ob.split("_")[0]
            this_objects[ob] = obtype
        for ob in dictio_banfnbnfpos[eid]:
            obtype = ob.split("_")[0]
            this_objects[ob] = obtype

        events.append({"ocel:eid": eid, "ocel:activity": activity, "ocel:timestamp": timestamp, "ocel:omap": list(this_objects), "ordering": ordering})

        for ob in this_objects:
            if ob not in added_objects:
                added_objects.add(ob)
                objects.append({"ocel:oid": ob, "ocel:type": this_objects[ob]})
            relations.append({"ocel:eid": row["EID"], "ocel:activity": activity, "ocel:timestamp": timestamp, "ocel:oid": ob, "ocel:type": this_objects[ob], "ordering": ordering})


def read_dataframe_invp():
    dataframe = pd.read_csv("dataframe_invp.csv", dtype=str)
    dataframe.columns = [x.split(".")[-1] for x in dataframe.columns]
    dataframe["BUDAT"] = pd.to_datetime(dataframe["BUDAT"], format=parameters["date_column_format"], errors="coerce")
    dataframe = dataframe.dropna(subset=["BUDAT"])
    dataframe["EID"] = "EBANROW_" + dataframe.index.astype(str)
    dataframe["EBELN"] = "EBELN_" + dataframe["MANDT"] + "_" +dataframe["EBELN"]
    dataframe["EBELNEBELP"] = "EBELNEBELP_" + dataframe["MANDT"] + "_" +dataframe["EBELNEBELP"]
    dataframe["BELNRGJAHR"] = "BELNRGJAHR_" + dataframe["MANDT"] + "_" + +dataframe["BELNRGJAHR"]
    dataframe["BELNRBUZEIGJAHR"] = "BELNRBUZEIGJAHR_" + dataframe["MANDT"] + "_" + dataframe["BELNRBUZEIGJAHR"]
    dictio_ebelns = dataframe.dropna(subset=["EBELN"]).groupby("EID")["EBELN"].apply(set).to_dict()
    dictio_ebelnebelps = dataframe.dropna(subset=["EBELNEBELP"]).groupby("EID")["EBELNEBELP"].apply(set).to_dict()
    dictio_belnrgjahr = dataframe.groupby("EID")["BELNRGJAHR"].apply(set).to_dict()
    dictio_belnrbuzeigjahr = dataframe.groupby("EID")["BELNRBUZEIGJAHR"].apply(set).to_dict()
    ordering = 4

    for index, row in dataframe.iterrows():
        activity = "Create Invoice Item"
        timestamp = row["BUDAT"]
        eid = row["EID"]

        this_objects = {}
        if eid in dictio_ebelns:
            for ob in dictio_ebelns[eid]:
                obtype = ob.split("_")[0]
                this_objects[ob] = obtype
            for ob in dictio_ebelnebelps[eid]:
                obtype = ob.split("_")[0]
                this_objects[ob] = obtype
        for ob in dictio_belnrgjahr[eid]:
            obtype = ob.split("_")[0]
            this_objects[ob] = obtype
        for ob in dictio_belnrbuzeigjahr[eid]:
            obtype = ob.split("_")[0]
            this_objects[ob] = obtype

        events.append({"ocel:eid": eid, "ocel:activity": activity, "ocel:timestamp": timestamp, "ocel:omap": list(this_objects), "ordering": ordering})

        for ob in this_objects:
            if ob not in added_objects:
                added_objects.add(ob)
                objects.append({"ocel:oid": ob, "ocel:type": this_objects[ob]})
            relations.append({"ocel:eid": row["EID"], "ocel:activity": activity, "ocel:timestamp": timestamp, "ocel:oid": ob, "ocel:type": this_objects[ob], "ordering": ordering})


def read_dataframe_changes_ekko():
    dataframe = pd.read_csv("dataframe_chngsekko.csv", dtype=str)
    dataframe.columns = [x.split(".")[-1] for x in dataframe.columns]
    dataframe["TIMESTAMP"] = dataframe["UDATE"] + " " + dataframe["UTIME"]
    dataframe["TIMESTAMP"] = pd.to_datetime(dataframe["TIMESTAMP"], errors="coerce")
    dataframe = dataframe.dropna(subset=["TIMESTAMP"])
    dataframe["EID"] = "CDHDR_EKKO_" + dataframe.index.astype(str)
    dataframe["EBELN"] = "EBELN_" + dataframe["MANDT"] + "_" + dataframe["EBELN"]
    ordering = 2.5

    for index, row in dataframe.iterrows():
        activity = "Change Purchase Order (" + row["TCODE"] + ")"
        timestamp = row["TIMESTAMP"]
        eid = row["EID"]

        this_objects = {row["EBELN"]: "EBELN"}

        events.append({"ocel:eid": eid, "ocel:activity": activity, "ocel:timestamp": timestamp, "ocel:omap": list(this_objects), "ordering": ordering})

        for ob in this_objects:
            if ob not in added_objects:
                added_objects.add(ob)
                objects.append({"ocel:oid": ob, "ocel:type": this_objects[ob]})

            relations.append({"ocel:eid": row["EID"], "ocel:activity": activity, "ocel:timestamp": timestamp, "ocel:oid": ob, "ocel:type": this_objects[ob], "ordering": ordering})


def read_dataframe_goods_receipts():
    dataframe = pd.read_csv("dataframe_ekbegoods.csv", dtype=str)
    dataframe.columns = [x.split(".")[-1] for x in dataframe.columns]
    dataframe["BUDAT"] = pd.to_datetime(dataframe["BUDAT"], errors="coerce")
    dataframe = dataframe.dropna(subset=["BUDAT"])
    dataframe["EID"] = "EKBE_GR_" + dataframe.index.astype(str)
    dataframe["EBELN"] = "EBELN_" + dataframe["MANDT"] + "_" + dataframe["EBELN"]
    dataframe["EBELNEBELP"] = "EBELNEBELP_" + dataframe["MANDT"] + "_" + dataframe["EBELN"] + dataframe["EBELP"]

    ordering = 2.75

    for index, row in dataframe.iterrows():
        activity = "Goods Receipt"
        timestamp = row["BUDAT"]
        eid = row["EID"]

        this_objects = {row["EBELN"]: "EBELN", row["EBELNEBELP"]: "EBELNEBELP"}

        events.append({"ocel:eid": eid, "ocel:activity": activity, "ocel:timestamp": timestamp, "ocel:omap": list(this_objects), "ordering": ordering})

        for ob in this_objects:
            if ob not in added_objects:
                added_objects.add(ob)
                objects.append({"ocel:oid": ob, "ocel:type": this_objects[ob]})

            relations.append({"ocel:eid": row["EID"], "ocel:activity": activity, "ocel:timestamp": timestamp, "ocel:oid": ob, "ocel:type": this_objects[ob], "ordering": ordering})


def read_dataframe_invoice_receipts():
    dataframe = pd.read_csv("dataframe_ekbeinvreceipts.csv", dtype=str)
    dataframe.columns = [x.split(".")[-1] for x in dataframe.columns]
    dataframe["BUDAT"] = pd.to_datetime(dataframe["BUDAT"], errors="coerce")
    dataframe = dataframe.dropna(subset=["BUDAT"])
    dataframe["EID"] = "EKBE_GR_" + dataframe.index.astype(str)
    dataframe["BELNRGJAHR"] = "EBELN_" + dataframe["MANDT"] + "_" + dataframe["BELNR"] + dataframe["GJAHR"]
    dataframe["BELNRBUZEIGJAHR"] = "EBELN_" + dataframe["MANDT"] + "_" + dataframe["BELNR"] + dataframe["BUZEI"] + dataframe["GJAHR"]

    ordering = 2.95

    for index, row in dataframe.iterrows():
        activity = "Invoice Receipt"
        timestamp = row["BUDAT"]
        eid = row["EID"]

        this_objects = {row["BELNRGJAHR"]: "BELNRGJAHR", row["BELNRBUZEIGJAHR"]: "BELNRBUZEIGJAHR"}

        events.append({"ocel:eid": eid, "ocel:activity": activity, "ocel:timestamp": timestamp, "ocel:omap": list(this_objects), "ordering": ordering})

        for ob in this_objects:
            if ob not in added_objects:
                added_objects.add(ob)
                objects.append({"ocel:oid": ob, "ocel:type": this_objects[ob]})

            relations.append({"ocel:eid": row["EID"], "ocel:activity": activity, "ocel:timestamp": timestamp, "ocel:oid": ob, "ocel:type": this_objects[ob], "ordering": ordering})


def read_dataframe_changes_rbkp():
    dataframe = pd.read_csv("dataframe_chngsrbkp.csv", dtype=str)
    dataframe.columns = [x.split(".")[-1] for x in dataframe.columns]
    dataframe["TIMESTAMP"] = dataframe["UDATE"] + " " + dataframe["UTIME"]
    dataframe["TIMESTAMP"] = pd.to_datetime(dataframe["TIMESTAMP"], errors="coerce")
    dataframe = dataframe.dropna(subset=["TIMESTAMP"])
    dataframe["EID"] = "CDHDR_RBKP_" + dataframe.index.astype(str)
    dataframe["BELNRGJAHR"] = "BELNRGJAHR_" + dataframe["MANDT"] + "_" + dataframe["BELNRGJAHR"]

    ordering = 3.5

    for index, row in dataframe.iterrows():
        activity = "Change Invoice (" + row["TCODE"] + ")"
        timestamp = row["TIMESTAMP"]
        eid = row["EID"]

        this_objects = {row["BELNRGJAHR"]: "BELNRGJAHR"}

        events.append({"ocel:eid": eid, "ocel:activity": activity, "ocel:timestamp": timestamp, "ocel:omap": list(this_objects), "ordering": ordering})

        for ob in this_objects:
            if ob not in added_objects:
                added_objects.add(ob)
                objects.append({"ocel:oid": ob, "ocel:type": this_objects[ob]})

            relations.append({"ocel:eid": row["EID"], "ocel:activity": activity, "ocel:timestamp": timestamp, "ocel:oid": ob, "ocel:type": this_objects[ob], "ordering": ordering})


if __name__ == "__main__":
    read_dataframe_pr()
    read_dataframe_po()
    read_dataframe_invp()
    read_dataframe_changes_ekko()
    read_dataframe_changes_rbkp()
    read_dataframe_goods_receipts()
    read_dataframe_invoice_receipts()

    events = pd.DataFrame(events)
    objects = pd.DataFrame(objects)
    relations = pd.DataFrame(relations)
    if len(events) > 0:
        events = events.sort_values(["ocel:timestamp", "ordering"])
    if len(relations) > 0:
        relations = relations.sort_values(["ocel:timestamp", "ordering"])
    print(events)
    print(relations)

    if len(events) == 0:
        events = pd.DataFrame({"ocel:eid": [], "ocel:activity": [], "ocel:timestamp": []})
        objects = pd.DataFrame({"ocel:oid": [], "ocel:type": []})
        relations = pd.DataFrame({"ocel:eid": [], "ocel:activity": [], "ocel:timestamp": [], "ocel:oid": [], "ocel:type": []})

    ocel = OCEL()
    ocel.events = events
    ocel.objects = objects
    ocel.relations = relations

    pm4py.write_ocel(ocel, "final_result.jsonocel")
