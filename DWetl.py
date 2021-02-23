from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator
import pandas as pd
import glob
import copy
import re
from datetime import datetime, timedelta
from airflow import models
from airflow.operators.python_operator import PythonOperator
import datetime as dt
default_args = {
    'owner': 'amine',
    'depends_on_past': False,
    'start_date': dt.datetime(2020, 12, 31),
    'email': ['drive2colab@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
}
dfss = []
def fuel_access():
    global dfss
    path =r'/home/amine/Dw Project/DW/access-to-clean-fuels-and-technologies-for-cooking.csv'
    dfss.append(pd.read_csv(path))
def elecetricity_access():
    global dfss
    path =r'/home/amine/Dw Project/DW/access-to-electricity.csv'
    dfss.append(pd.read_csv(path))
def co_emission():
    global dfss
    path =r'/home/amine/Dw Project/DW/annual-co-emissions-by-region.csv'
    dfss.append(pd.read_csv(path))
def biofuel_production():
    global dfss
    path =r'/home/amine/Dw Project/DW/biofuel-production.csv'
    dfss.append(pd.read_csv(path))
def co2():
    global dfss
    path =r'/home/amine/Dw Project/DW/CO2-by-source.csv'
    dfss.append(pd.read_csv(path))
def ozone_consumption():
    global dfss
    path =r'/home/amine/Dw Project/DW/consumption-of-ozone-depleting-substances.csv'
    dfss.append(pd.read_csv(path))
def country():
    global dfss
    path =r'/home/amine/Dw Project/DW/country.csv'
    dfss.append(pd.read_csv(path))
def deaths():
    global dfss
    path =r'/home/amine/Dw Project/DW/final.csv'
    dfss.append(pd.read_csv(path))
def energy_substitution():
    global dfss
    path =r'/home/amine/Dw Project/DW/global-energy-substitution.csv'
    dfss.append(pd.read_csv(path))
def renewable_energy():
    global dfss
    path =r'/home/amine/Dw Project/DW/modern-renewable-energy-consumptioncsv'
    dfss.append(pd.read_csv(path))
def agreements():
    global dfss
    path =r'/home/amine/Dw Project/DW/number-of-parties-env-agreements.csv'
    dfss.append(pd.read_csv(path))
def socio():
    global dfss
    path =r'/home/amine/Dw Project/DW/socio.csv'
    dfss.append(pd.read_csv(path))
def nuclear():
    global dfss
    path =r'/home/amine/Dw Project/DW/sub-energy-fossil-renewables-nuclearcsv'
    dfss.append(pd.read_csv(path))
def Pollution():
    global dfss
    path =r'/home/amine/Dw Project/DW/valuePollution.csv'
    dfss.append(pd.read_csv(path))

def importer():
    global dfs
    path =r'/home/amine/Dw Project/DW'
    filenames = glob.glob(path + "/*.csv")
    dfs = []
    for filename in filenames:
        dfs.append(pd.read_csv(filename))
importer()


def idr(data):
    mylist = list(data.columns)
    r = re.compile("ID.*")
    newlist = list(filter(r.match, mylist))
    if len(newlist)!=0:
        return newlist[0]
    else:
        pass
def operate():
    global res
    ids=[]
    for i in dfs:
        ids.append(str(idr(i)))
    res = dict(zip(ids, dfs))

operate()

def clearning():
    global datasets
    global idtable
    temps=0
    framesofid=pd.DataFrame()
    data=[]
    for k,v in res.items():
        v.drop_duplicates(inplace=True)
        v.dropna(subset=[str(k)],inplace=True)
        v.sort_values(by=[k], inplace=True)
        if 'Year' in list(v.columns):
            v.drop(v[v.Year < 1980].index, inplace=True)
            v.drop(v[v.Year > 2016 ].index, inplace=True)
        temps+=1
        framesofid[str(k)]=v[str(k)].dropna()
    data.append(framesofid)
    data.append(list(res.values()))
    datasets=data[1]
    idtable=data[0]

clearning()


def create_date_table2(start='1980', end='2016',freq='A'):
    global yeartable
    df = pd.DataFrame({"Date": pd.date_range(start, end)})
    df["Year"] = df.Date.dt.year
    df = df.drop_duplicates(subset=['Year'])
    df = df.drop(["Date"], axis=1)
    df.insert(0, 'Year_ID', range(1, 1 + len(df)))
    yeartable=df

create_date_table2()


def SumEY(data):
    """datasets[10]"""
    create_date_table2()
    temps = data.groupby(['Year']).sum()
    temps['sum']=temps[["Consumption Geo Biomass Other - TWh", "Consumption Hydro Generation - TWh", "Consumption Solar Generation - TWh","Consumption Wind Generation -TWh"]].sum(axis=1)
    temps=pd.merge(temps,yeartable,on=['Year'])
    temps=temps[['sum','Year']]
    temps.insert(0, 'IDEY', range(1, 1 + len(temps)))
    temps=temps.rename(columns={"Year": "Year_ID"})
    return temps




def SumEC(data):
    """datasets[10]"""

    temps = data.groupby(['Code']).sum()
    temps['IDCode'] = temps.index
    temps['sum']=temps[["Consumption Geo Biomass Other - TWh", "Consumption Hydro Generation - TWh", "Consumption Solar Generation - TWh","Consumption Wind Generation -TWh"]].sum(axis=1)
    #temps=pd.merge(temps,datasets[5],on=['IDCode'])
    temps=temps[['sum','IDCode']]
    temps=temps.rename(columns={"sum": "Consumption ER"})
    temps.insert(0, 'IDEC', range(1, 1 + len(temps)))
    return temps


def DifC(data):
    """datasets[10]"""
    temps = data.groupby(['Code','Year']).sum()
    temps['sum']=temps[["Consumption Geo Biomass Other - TWh", "Consumption Hydro Generation - TWh", "Consumption Solar Generation - TWh","Consumption Wind Generation -TWh"]].sum(axis=1)
    temps=temps['sum']
    tt=temps.reset_index()
    ttmin=tt[(tt['Year'] == min(tt['Year']))]
    ttmax=tt[(tt['Year'] == max(tt['Year']))]
    ttmin=ttmin.rename(columns={"Year": "Ymin", "sum": "Smin"})
    ttmax=ttmax.rename(columns={"Year": "Ymax", "sum": "Smax"})
    tfinal=pd.merge(ttmax, ttmin, on=['Code'])
    tfinal['Relative Change Consumption %']=((tfinal['Smax']-tfinal['Smin'])/tfinal['Smin'])*100
    tfinal=tfinal[['Code','Relative Change Consumption %']]
    tfinal=tfinal.rename(columns={"Code": "IDCode"})
    tfinal.insert(0, 'IDCONEDIF', range(1, 1 + len(tfinal)))
    return tfinal


def DifFuel(data):
    """datasets[0]"""
    temps = data.groupby(['Code','Year']).sum()
    temps['sum']=temps[["Access to clean fuels cooking %"]].sum(axis=1)
    temps=temps['sum']
    tt=temps.reset_index()
    ttmin=tt[(tt['Year'] == min(tt['Year']))]
    ttmax=tt[(tt['Year'] == max(tt['Year']))]
    ttmin=ttmin.rename(columns={"Year": "Ymin", "sum": "Smin"})
    ttmax=ttmax.rename(columns={"Year": "Ymax", "sum": "Smax fuel"})
    tfinal=pd.merge(ttmax, ttmin, on=['Code'])
    tfinal['Relative Change Fuels cooking %']=((tfinal['Smax fuel']-tfinal['Smin'])/tfinal['Smin'])*100
    tfinal=tfinal[['Code','Relative Change Fuels cooking %','Smax fuel']]
    tfinal=tfinal.rename(columns={"Code": "IDCode","Smax fuel":"Cooking With Fuel %"})
    tfinal.insert(0, 'IDFUELDIF', range(1, 1 + len(tfinal)))
    return tfinal


def PAFuel(data):
    """datasets[0]"""

    temps = data.groupby(['Code','Year']).sum()
    temps['sum']=temps[["Access to clean fuels cooking %"]].sum(axis=1)
    temps=temps['sum']
    tt=temps.reset_index()
    tt = tt.groupby(['Year']).mean()
    tt=tt.reset_index()
    tt=tt.rename(columns={"Year": "Year_ID","sum":"Global Fuels Access %"})
    tt.insert(0, 'IDPRODFUEL', range(1, 1 + len(tt)))
    return tt



def PAEL(data):

    temps = data.groupby(['Code','Year']).sum()
    temps['sum']=temps[["Access to electricity (% of population)"]].sum(axis=1)
    temps=temps['sum']
    tt=temps.reset_index()
    tt = tt.groupby(['Year']).mean()
    tt=tt.reset_index()
    tt=tt.rename(columns={"Year": "Year_ID","sum":"Global electricity Access %"})
    tt.insert(0, 'IDPRODELECT', range(1, 1 + len(tt)))
    return tt



def DifEL(data):
    """datasets[1]"""
    temps = data.groupby(['Code','Year']).sum()
    temps['sum']=temps[["Access to electricity (% of population)"]].sum(axis=1)
    temps=temps['sum']
    tt=temps.reset_index()
    ttmin=tt[(tt['Year'] == min(tt['Year']))]
    ttmax=tt[(tt['Year'] == max(tt['Year']))]
    ttmin=ttmin.rename(columns={"Year": "Ymin", "sum": "Smin"})
    ttmax=ttmax.rename(columns={"Year": "Ymax", "sum": "Smax fuel"})
    tfinal=pd.merge(ttmax, ttmin, on=['Code'])
    tfinal['Relative Change Fuels cooking %']=((tfinal['Smax fuel']-tfinal['Smin'])/tfinal['Smin'])*100
    tfinal=tfinal[['Code','Relative Change Fuels cooking %','Smax fuel']]
    tfinal=tfinal.rename(columns={"Code": "IDCode","Smax fuel":"Electricity Access %"})
    tfinal.insert(0, 'IDDIFELECT', range(1, 1 + len(tfinal)))
    return tfinal



def SumCo2(data):
    """datasets[2]"""
    temps = data.groupby(['Code']).mean()
    temps['IDCode'] = temps.index
    temps=temps.rename(columns={"Annual CO2 emissions": "CO2 emissions"})
    temps=temps[['CO2 emissions','IDCode']]
    temps['CO2 emissions %'] = (temps['CO2 emissions'] / temps['CO2 emissions']. sum ()) * 100
    temps.insert(0, 'IDCO2EMI', range(1, 1 + len(temps)))
    return temps



def PAP(data):
    """datasets[15]"""
    temps = data.groupby(['Code','Year']).sum()
    temps['sum']=temps[["ValPol"]].sum(axis=1)
    temps=temps['sum']
    tt=temps.reset_index()
    tt = tt.groupby(['Year']).mean()
    tt=tt.reset_index()
    tt=tt.rename(columns={"Year": "Year_ID","sum":"Prod of Pollution"})
    tt.insert(0, 'IDPOLVALUE', range(1, 1 + len(tt)))
    return tt


def MeanP(data):
    """datasets[16]"""
    temps = data.groupby(['Code']).mean()
    temps['IDCode'] = temps.index
    temps=temps[['ValPol','IDCode']]
    temps['V of Pollution Country %'] = (temps['ValPol'] / temps['ValPol']. sum ()) * 100
    temps=temps[['IDCode','V of Pollution Country %']]
    temps.insert(0, 'IDMENPV', range(1, 1 + len(temps)))
    return temps


def MeanREC(data):
    """datasets[10]"""
    temps = data.groupby(['Code']).mean()
    temps['IDCode'] = temps.index
    temps['Consumption Geo Biomass %'] = (temps['Consumption Geo Biomass Other - TWh'] / temps['Consumption Geo Biomass Other - TWh']. sum ()) * 100
    temps['Consumption Hydro Generation %'] = (temps['Consumption Hydro Generation - TWh'] / temps['Consumption Hydro Generation - TWh']. sum ()) * 100
    temps['Consumption Solar Generation %'] = (temps['Consumption Solar Generation - TWh'] / temps['Consumption Solar Generation - TWh']. sum ()) * 100
    temps['Consumption Wind Generation %'] = (temps['Consumption Wind Generation -TWh'] / temps['Consumption Wind Generation -TWh']. sum ()) * 100
    temps=temps[['Consumption Wind Generation %','Consumption Solar Generation %','Consumption Hydro Generation %','Consumption Geo Biomass %','IDCode']]
    temps.insert(0, 'IDMEANREC', range(1, 1 + len(temps)))
    return temps


def UENC(data):
    """datasets[15]"""
    temps = data.groupby(['Code']).mean()
    temps['IDCode'] = temps.index
    temps.drop(['Year'], axis=1,inplace=True)
    temps.insert(0, 'IDTYPENC', range(1, 1 + len(temps)))
    return temps



def UENY(data):
    """datasets[15]"""
    temps = data.groupby(['Year']).mean()
    temps['Year_ID'] = temps.index
    temps.insert(0, 'IDTYPENY', range(1, 1 + len(temps)))
    return temps


def MeanREY(data):
    """datasets[10]"""
    temps = data.groupby(['Year']).mean()
    temps['Year_ID'] = temps.index
    temps['sum']=temps[["Consumption Geo Biomass Other - TWh", "Consumption Hydro Generation - TWh", "Consumption Solar Generation - TWh","Consumption Wind Generation -TWh"]].sum(axis=1)
    temps['Geo Biomass Y %'] = (temps['Consumption Geo Biomass Other - TWh'] / temps['sum']) * 100
    temps['Hydro Generation Y %'] = (temps['Consumption Hydro Generation - TWh'] / temps['sum']) * 100
    temps['Solar Generation Y %'] = (temps['Consumption Solar Generation - TWh'] / temps['sum']) * 100
    temps['Wind Generation Y%'] = (temps['Consumption Wind Generation -TWh'] / temps['sum']) * 100
    temps=temps[['Geo Biomass Y %','Hydro Generation Y %','Solar Generation Y %','Wind Generation Y%','Year_ID']]
    temps.insert(0, 'IDMEANREY', range(1, 1 + len(temps)))
    return temps



def red(x):
    x=x.split('-')[0]
    return x
def temperature():
    global temperatures
    temperatures=pd.read_csv('/home/amine/Dw Project/tempreature.csv')
    temperatures['Year'] = temperatures['Date'].apply(red)
    temperatures.drop(['Date'], axis=1,inplace=True)
    temperatures.drop(['Country'], axis=1,inplace=True)
    temperatures=temperatures.groupby(['Year']).mean()
    temperatures.insert(0, 'IDTEMP', range(1, 1 + len(temperatures)))
    temperatures['Year_ID'] = temperatures.index
    temperatures['Year_ID']= temperatures['Year_ID'].apply(lambda x:int(x))
    temperatures.head(2)
datasets[14]=datasets[14].dropna()
datasets[14].insert(0, 'IDSOCIO', range(1, 1 + len(datasets[14])))


temperature()
def MeanDY(data):
    """datasets[7]"""
    temps = data.groupby(['Year']).mean()
    temps['Year_ID'] = temps.index
    temps.insert(0, 'IDMEANREYY', range(1, 1 + len(temps)))
    return temps


def MeanDC(data):
    """datasets[7]"""
    temps = data.groupby(['Code']).mean()
    temps['IDCode'] = temps.index
    temps.insert(0, 'IDMEANREYY', range(1, 1 + len(temps)))
    temps.drop(['Year'], axis=1,inplace=True)
    return temps



def MeanOZ(data):
    """datasets[5]"""
    temps = data.groupby(['Year']).mean()
    temps['Year_ID'] = temps.index
    temps.insert(0, 'IDOZONE', range(1, 1 + len(temps)))
    return temps
datasets[11]['Year_ID']=datasets[11]['IDYear_AGREEMENTS']
resultC = [MeanDC(datasets[7]),UENC(datasets[15]),MeanREC(datasets[10]),MeanP(datasets[15]),SumCo2(datasets[2]),DifEL(datasets[1]),SumEC(datasets[10]),DifC(datasets[10])]
resultY = [datasets[11],MeanDY(datasets[7]),temperatures,SumEY(datasets[10]),PAEL(datasets[1]),PAFuel(datasets[0]),PAEL(datasets[1]),MeanREY(datasets[10]),UENY(datasets[15]),PAP(datasets[15]),MeanOZ(datasets[5])]

dfc = resultC[0]
for df_ in resultC[1:]:
    dfc = dfc.merge(df_, on='IDCode',how='outer')
dfy = resultY[0]
for df_ in resultY[1:]:
    dfy = dfy.merge(df_, on='Year_ID',how='outer')

def factable():
    global final
    final=dfy.merge(dfc,how='outer')

def val():
    print('fin dw')


def loading():
    final.to_csv('/home/amine/Dw Project/Data/final/facttable.csv')
    df=final
    import sqlite3
    conn = sqlite3.connect('session.db')
    print(conn)

    try:
        conn.execute('DROP TABLE IF EXISTS `Fact` ')
    except Exception as e:
        raise(e)
    finally:
        print('Table dropped')

    try:
        conn.execute('''
         CREATE TABLE Fact ('ID',
         'IDMEANREYY' FLOAT,
         'Deaths - Exposure to forces of nature %' FLOAT,
         'Deaths - Under 5' FLOAT,
         'Deaths -  Age: 5-14 years' FLOAT,
         'Deaths - Age: 15-49 years' FLOAT,
         'Deaths - Age: 50-69 years' FLOAT,
         'Deaths - Age: 70+ years' FLOAT ,
         'Deaths - No access to handwashing facility' FLOAT,
         'Deaths - Smoking' FLOAT,
         'Deaths - Secondhand smoke' FLOAT,
         'Deaths - Unsafe water source' FLOAT,
         'Deaths - Household air pollution from solid fuels' FLOAT,
         'Deaths - Air pollution' FLOAT,
         'Deaths Outdoor air pollution' FLOAT,
         'Year_ID' INT,
         'IDTEMP' INT,
         'AverageTemperature' FLOAT,
         'AverageTemperatureUncertainty' FLOAT,
         'IDEY' INT,
         'sum' FLOAT,
         'IDPRODELECT_x' INT,
         'Global electricity Access %_x' FLOAT,
         'IDPRODFUEL' INT,
         'Global Fuels Access %' FLOAT,
         'IDPRODELECT_y' INT,
         'Global electricity Access %_y' FLOAT,
         'IDMEANREY' INT,
         'Geo Biomass Y %' FLOAT,
         'Hydro Generation Y %' FLOAT,
         'Solar Generation Y %' FLOAT,
         'Wind Generation Y%' FLOAT,
         'IDTYPENY' INT,
         'ValPol' FLOAT,
         'IDPOLVALUE' INT,
         'Prod of Pollution' INT,
         'IDCode' INT,
         'IDTYPENC' INT,
         'IDMEANREC' INT,
         'Consumption Wind Generation %' FLOAT,
         'Consumption Solar Generation %' FLOAT,
         'Consumption Hydro Generation %' FLOAT,
         'Consumption Geo Biomass %' FLOAT,
         'IDMENPV' INT,
         'V of Pollution Country %' FLOAT,
         'IDCO2EMI' INT,
         'CO2 emissions' FLOAT,
         'CO2 emissions %' FLOAT,
         'IDDIFELECT' INT,
         'Relative Change Fuels cooking %' FLOAT,
         'Electricity Access %' FLOAT,
         'IDEC' INT,
         'Consumption ER' FLOAT,
         'IDCONEDIF' INT,
         'Relative Change Consumption %' FLOAT);''')
        print ("Table created successfully");
    except Exception as e:
        print(str(e))
        print('Table Creation Failed!!!!!')
    finally:
        conn.close()
    val_list = df.values.tolist()
    conn = sqlite3.connect('DW.db')
    cur = conn.cursor()
    try:

        cur.executemany("INSERT INTO Fact('ID','IDMEANREYY','Deaths - Exposure to forces of nature %','Deaths - Under 5','Deaths -  Age: 5-14 years','Deaths - Age: 15-49 years','Deaths - Age: 50-69 years','Deaths - Age: 70+ years','Deaths - No access to handwashing facility','Deaths - Smoking','Deaths - Secondhand smoke','Deaths - Unsafe water source','Deaths - Household air pollution from solid fuels','Deaths - Air pollution','Deaths Outdoor air pollution','Year_ID','IDTEMP','AverageTemperature','AverageTemperatureUncertainty','IDEY','sum','IDPRODELECT_x','Global electricity Access %_x','IDPRODFUEL','Global Fuels Access %','IDPRODELECT_y','Global electricity Access %_y','IDMEANREY','Geo Biomass Y %','Hydro Generation Y %','Solar Generation Y %','Wind Generation Y%','IDTYPENY','ValPol','IDPOLVALUE','Prod of Pollution','IDCode','IDTYPENC','IDMEANREC','Consumption Wind Generation %','Consumption Solar Generation %','Consumption Hydro Generation %','Consumption Geo Biomass %','IDMENPV','V of Pollution Country %','IDCO2EMI','CO2 emissions','CO2 emissions %','IDDIFELECT','Relative Change Fuels cooking %','Electricity Access %','IDEC','Consumption ER','IDCONEDIF','Relative Change Consumption %') VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", val_list)
        conn.commit()
        print('Data Inserted Successfully')
    except Exception as e:
        print(str(e))
        print('Data Insertion Failed')
    finally:

        conn.close()



from airflow.utils.dates import days_ago
with models.DAG(
    dag_id="ETL_DW",
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['example', 'example2', 'example3'],
    ) as dag:
    l1 = PythonOperator(
        task_id='IMPORT_fuel_access',
        provide_context=True,
        python_callable=fuel_access)
    l2 = PythonOperator(
        task_id='IMPORT_elecetricity_access',
        provide_context=True,
        python_callable=elecetricity_access)
    l3 = PythonOperator(
        task_id='IMPORT_biofuel_production',
        provide_context=True,
        python_callable=biofuel_production)
    l4 = PythonOperator(
        task_id='IMPORT_co2',
        provide_context=True,
        python_callable=co2)
    l6 = PythonOperator(
        task_id='IMPORT_ozone_consumption',
        provide_context=True,
        python_callable=ozone_consumption)
    l7 = PythonOperator(
        task_id='IMPORT_country',
        provide_context=True,
        python_callable=country)
    l8 = PythonOperator(
        task_id='IMPORT_deaths',
        provide_context=True,
        python_callable=deaths)
    l9 = PythonOperator(
        task_id='IMPORT_energy_substitution',
        provide_context=True,
        python_callable=energy_substitution)
    l10 = PythonOperator(
        task_id='IMPORT_renewable_energy',
        provide_context=True,
        python_callable=renewable_energy)
    l11 = PythonOperator(
        task_id='IMPORT_agreements',
        provide_context=True,
        python_callable=agreements)
    l12 = PythonOperator(
        task_id='IMPORT_socio',
        provide_context=True,
        python_callable=socio)
    l13 = PythonOperator(
        task_id='IMPORT_nuclear',
        provide_context=True,
        python_callable=nuclear)
    l14 = PythonOperator(
        task_id='IMPORT_Pollution',
        provide_context=True,
        python_callable=Pollution)
    l15 = PythonOperator(
        task_id='IMPORT_temperature',
        provide_context=True,
        python_callable=temperature)

    t1 = PythonOperator(
        task_id='IMPORT_DATA',
        provide_context=True,
        python_callable=importer)
    t2 = PythonOperator(
        task_id='OPERATE_DATA',
        provide_context=True,
        python_callable=operate)
    t3 = PythonOperator(
        task_id='ClEANING_DATA',
        provide_context=True,
        python_callable=clearning)
    t4 = PythonOperator(
        task_id='MANAGIND_DATA',
        provide_context=True,
        python_callable=factable)

    t5 = PythonOperator(
        task_id='VALIDATE_DATA',
        provide_context=True,
        python_callable=val)

    t6 = PythonOperator(
        task_id='LOAD_DATA',
        provide_context=True,
        python_callable=loading)

    [l1, l2, l3, l4, l6, l7, l8, l9, l10, l11, l12, l13, l14, l15] >> t1 >> t2 >> t3 >> t4 >> t5 >> t6
