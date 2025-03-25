from pyspark.sql import functions as F
from pyspark.sql import DataFrame, Window
from scipy.interpolate import griddata

import numpy as np

limits = {}

### PD VESSEL LIMITS ###
V5500TEU_limits = {
    '9290799': 1.2  # Tessa
}
V5600TEU_limits = {
    '9535199': 7  # HSHI
}
V6500TEU_limits = {
    '9306287': 30.5,  # Dana
    '9306184': 29,  # Sana
    '9306160': 28  # Wafa
}
V6600TEU_limits = {
    '9395525': 20.3,  # Busan / Actuaria
    '9400071': 20.3  # Wasl
}

### ST VESSEL LIMITS ###
D37_limits = {
    '9102095': 13.5,  # Stolt Creativity
    '9178202': 35.3,  # Stolt Effort
    '9124469': 29.1,  # Stolt Achievement
    '9178197': 19.9,  # Stolt Concept
}
N43_limits = {
    '9414060': 9.5,  # Stolt Norland
    '9414084': 7.3  # Stolt Breland
}
C38_limits = {
    '9680102': 13.3  # Stolt Tenacity
}

### CHR VESSEL LIMITS ###
CHR_limits = {
    "9428449": 2.1,
    "9428451": 2.2,
    "9478286": 3.5,
    "9478298": 2.1,
    "9478303": 1.7,
    "9478315": 1.7
}

accepted_imos = list(V5500TEU_limits) + list(V5600TEU_limits) + list(V6500TEU_limits) + list(V6600TEU_limits) + \
                list(D37_limits) + list(N43_limits) + list(C38_limits) + list(CHR_limits)
limits.update(V5500TEU_limits)
limits.update(V5600TEU_limits)
limits.update(V6500TEU_limits)
limits.update(V6600TEU_limits)
limits.update(D37_limits)
limits.update(N43_limits)
limits.update(C38_limits)
limits.update(CHR_limits)

trim_tables = {}

def get_trim_limit(shipID):
    if str(shipID) in accepted_imos:
        return limits[str(shipID)]
    else:
        return None

def get_trim_percentage(shipID, draft, speed, trim, s3_controller):
    file = None
    if str(shipID) in list(V5500TEU_limits):
        file = "5500TEU"
    if str(shipID) in list(V5600TEU_limits):
        file = "5600TEU"
    if str(shipID) in list(V6500TEU_limits):
        file = "6500TEU"
    if str(shipID) in list(V6600TEU_limits):
        file = "6600TEU"
    if str(shipID) in list(D37_limits):
        file = "D37"
    if str(shipID) in list(N43_limits):
        file = "N43"
    if str(shipID) in list(C38_limits):
        file = "C38"
    if str(shipID) in list(CHR_limits):
        file = "CHR"

    if file is not None:
        if file not in trim_tables:
            df = s3_controller.download_trim_csv_file('deus-trim-tables', f"{file}_GRID.csv")
            if 'fuel%' in df.columns:
                df = df.withColumnRenamed('fuel%', 'power')
            trim_tables[file] = df
        else:
            df = trim_tables[file]
        percs = griddata((df['draft'], df['speed'], df['trim']), df['power'], (draft, speed, trim))
        return percs

    return None

def get_ideal_trim(shipID, draft, speed, s3_controller):
    trim_domain = np.linspace(-2, 4, 50)
    
    totalProduct = (draft
                    .crossJoin(speed)
                    .crossJoin(trim_domain.toDF("trim")))
    
    totalProduct = totalProduct.drop_duplicates()
    totalProduct = totalProduct.withColumn('ideal_trim_perc', get_trim_percentage(shipID, 
                                                                                 totalProduct['draft'], 
                                                                                 totalProduct['speed'], 
                                                                                 totalProduct['trim'], 
                                                                                 s3_controller))
    
    windowSpec = Window.partitionBy("draft", "speed")
    totalProduct = totalProduct.withColumn("min_ideal", F.min("ideal_trim_perc").over(windowSpec))
    
    totalProduct = totalProduct.filter(F.col("ideal_trim_perc") == F.col("min_ideal"))
    
    ideals = []
    for row in draft.collect():
        rowDraft = row['draft']
        rowSpeed = row['speed']
        ideal = totalProduct.filter((F.col('draft') == rowDraft) & (F.col('speed') == rowSpeed)).select('trim').collect()
        if len(ideal) == 0:
            ideals.append(None)
        else:
            ideals.append(ideal[0][0])
    
    return ideals