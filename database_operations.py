#======== Download ========
import pandas as pd
from dbtools.engines import validationDB_engine
df = pd.read_sql("select * from auto_moments order by random() limit 10000", validationDB_engine)

#======== Process ========
equivalence = {
    "nsdm_info":"navigation",
    "nsdm_trajectory_ros":"trajectory",
    "avcm_info":"control",
    "apfs_obstacles":"perception",
    "evaps_imu":"imu",
    "AC3S/ASCS_ADCC_STATUS":"navigation",
    "avls_loc_output":"localisation",
}

df.topic.replace(equivalence, inplace=True)
df.bagid = df.bagid.apply(int)
df.messageid = df.messageid.apply(int)
df.drop(columns="point", inplace=True)


#======== Save ========
import sqlalchemy
save = sqlalchemy.create_engine('sqlite:///./auto_moments.sqlite')
save.execute("DROP TABLE IF EXISTS auto_moments;")
df.to_sql("auto_moments", save, dtype={'messagedata': sqlalchemy.types.JSON}, if_exists='append', index=False)

# save.execute("alter table auto_moments add point geometry")
# save.execute("update auto_moments set point=ST_Point(x,y)")