import pandas as pd
import csv
import configparser
import dateutil
import seaborn as sns
import matplotlib.pyplot as plt
from pandasql import sqldf
from pandas import ExcelWriter
import numpy as np

def ecdf(data):
    """Compute ECDF for 1-d array to check if the data is normalized"""

    # Number of data points: n
    n = len(data)

    # x-data for the ECDF: x
    x = np.sort(data)

    # y-data for the ECDF: y
    y = np.arange(1, n+1) / n

    return x, y




parser = configparser.ConfigParser()
parser.read('C:/Users/dfgh/PycharmProjects/driver/FileConfig_driver.cfg')
directory_Path = parser.get('file_config','directory_Path')
ImportFilePath = parser.get('file_config','ImportFilePath')
ExportFilePath = parser.get('file_config','ExportFilePath')
OverSpeedExportFilePath = parser.get('file_config','OverSpeedExportFilePath')
WorstDriverExportFilePath = parser.get('file_config','WorstDriverExportFilePath')
BestDriverExportFilePath = parser.get('file_config','BestDriverExportFilePath')
MaxStateExportFilePath = parser.get('file_config','MaxStateExportFilePath')
NewDfExportFilePath = parser.get('file_config','NewDfExportFilePath')

#Pushing csv to data frame
df = pd.read_csv(ImportFilePath,error_bad_lines=False)

#checking if data is noramlized
x, y = ecdf(df["speed_mph"])
plt.figure(figsize=(8,7))
sns.set()
plt.plot(x, y, marker=".", linestyle="none")
plt.xlabel("Speed (MPH)")
plt.ylabel("Cumulative Distribution Function")
samples = np.random.normal(np.mean(df["speed_mph"]), np.std(df["speed_mph"]), size=10000)
x_theor, y_theor = ecdf(samples)
plt.plot(x_theor, y_theor)
plt.legend(('Normal Distribution', 'Speed Data'), loc='lower right')
plt.show()


#Find overspeeding drivers
df_OverSpeed=df.query('speed_mph > speedlimit_mph')
print(df_OverSpeed)
df_OverSpeed.to_csv(OverSpeedExportFilePath)

#Number of traffic violations per month: select month, count(driver_id)
df_OverSpeed.timestamp = pd.to_datetime(df_OverSpeed.timestamp)
dg_month = df_OverSpeed.groupby(pd.Grouper(key='timestamp', freq='1M'))['driver_id'].count() # groupby each 1 month
dg_month.index = dg_month.index.strftime('%B')
print(dg_month)



sns.set(style="ticks", palette="pastel")
sns.boxplot(x=df["state"],y=df['speed_mph'])
plt.title('State v/s speed in miles per hour ')
plt.show()

#Distinguish speed limits in Highway
sns.boxplot(x=df["isHighway"],y=df['speed_mph'])
plt.title('If the route is a Highway v/s speed in miles per hour ')
plt.show()

df['violation_speed'] = df['speed_mph'] -df['speedlimit_mph']

#Worst driver
Worst_Driver=sqldf("""select driver_id,count(),avg(violation_speed) from df where violation_speed > 0 group by driver_id order by count() desc""")
Worst_Driver.to_csv(WorstDriverExportFilePath)
print("Worst driver")
print(Worst_Driver)


#Worst state
Max_State = sqldf("""select state,count(Distinct driver_id),avg(violation_speed) from df where violation_speed > 0 group by state order by count(distinct driver_id) desc""")
Max_State.to_csv(MaxStateExportFilePath)
print("Max violation state ")
print(Max_State)

#Best driver
Best_Driver = sqldf("""select driver_id,count(*),avg(violation_speed) from df where violation_speed < 0 group by driver_id order by avg(violation_speed) desc""")
Best_Driver.to_csv(BestDriverExportFilePath)
print("Best driver")
print(Best_Driver)




#Making a new dataframe which will contain new coloumns : violation ( difference between speed limit and diver speed)
#                                                         ,newtimestamp, weekday,month,day,hour)

df['newtimestamp'] = pd.to_datetime(df['timestamp'])  #2015-06-01 00:04:00
df['weekday'] = df['newtimestamp'].dt.day_name() #name of day
new_df = sqldf("""select *,substr(newtimestamp,6,2) month ,substr(newtimestamp,9,2) day , substr(newtimestamp,12,2) hour  from df """) # month,day,hour
new_df.to_csv(NewDfExportFilePath)



