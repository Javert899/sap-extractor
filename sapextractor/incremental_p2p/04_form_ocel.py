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
    dataframe["EID"] = "EKKOROW_" + dataframe["EKPO_ROW_NUMBER"]
    dataframe["EBELN"] = "EBELN_" + dataframe["EBELN"]
    dataframe["EBELNEBELP"] = "EBELNEBELP_" + dataframe["EBELNEBELP"]
    dictio_ebelns = dataframe.groupby("EID")["EBELN"].apply(set).to_dict()
    dictio_ebelnebelps = dataframe.groupby("EID")["EBELNEBELP"].apply(set).to_dict()
    ordering = 2

    dataframe = dataframe.groupby("EID").first().reset_index()
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
    dataframe.columns = [x.split(".")[1] for x in dataframe.columns]
    dataframe["BADAT"] = pd.to_datetime(dataframe["BADAT"], format=parameters["date_column_format"], errors="coerce")
    dataframe = dataframe.dropna(subset=["BADAT"])
    dataframe["EID"] = "EBANROW_" + dataframe["EBAN_ROW_NUMBER"]
    dataframe["EBELN"] = "EBELN_" + dataframe["EBELN"]
    dataframe["EBELNEBELP"] = "EBELNEBELP_" + dataframe["EBELNEBELP"]
    dataframe["BANFN"] = "BANFN_" + dataframe["BANFN"]
    dataframe["BANFNBNFPO"] = "BANFNBNFPO_" + dataframe["BANFNBNFPO"]
    dictio_ebelns = dataframe.dropna(subset=["EBELN"]).groupby("EID")["EBELN"].apply(set).to_dict()
    dictio_ebelnebelps = dataframe.dropna(subset=["EBELNEBELP"]).groupby("EID")["EBELNEBELP"].apply(set).to_dict()
    dictio_banfns = dataframe.groupby("EID")["BANFN"].apply(set).to_dict()
    dictio_banfnbnfpos = dataframe.groupby("EID")["BANFNBNFPO"].apply(set).to_dict()
    ordering = 1

    dataframe = dataframe.groupby("EID").first().reset_index()
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


if __name__ == "__main__":
    read_dataframe_pr()
    read_dataframe_po()

    events = pd.DataFrame(events)
    objects = pd.DataFrame(objects)
    relations = pd.DataFrame(relations)
    events = events.sort_values(["ocel:timestamp", "ordering"])
    relations = relations.sort_values(["ocel:timestamp", "ordering"])
    print(events)
    print(relations)

    ocel = OCEL()
    ocel.events = events
    ocel.objects = objects
    ocel.relations = relations

    pm4py.write_ocel(ocel, "final_result.jsonocel")
