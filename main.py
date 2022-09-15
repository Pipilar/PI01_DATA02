from fastapi import FastAPI
import json
import pandas as pd

superapi = FastAPI(title = 'La SuperAPI de Pili',
                   description = 'Esta es la API desarrollada por una alumna de Data Science de Henry, que tiene la finalidad de mostrar ciertas consultas que se realizaron sobre una base de datos relacionada a carreras automovilísticas. Los primeros métodos get devuelven los archivos JSON que fueron consultados para realizar las consultas, que corresponden a los últimos 4 get del listado. Aquellas consultas responden, cuál es el año en que se jugaron más carreras, cuál es el piloto con más primeros puestos ganados, cuál es el circuito donde más se corrieron carreras y por último, cuál es el piloto con mayor cantidad de puntos, utilizando únicamente autos de marcas británicas o norteamericanas',
                   version = '1.0')

# para conectar la api, en este caso la superapi, importamos fastapi desde main.py, y este archivo lo conectamos
# a través de la consola git bash, levantando el servidor usando el comando 'uvicorn main:superapi --reload'
# (una vez que ingresamos al directorio con el comando cd '<ruta de ubicación del archivo main.py>')
# con esta configuración, el presente archivo queda conectado al servidor de uvicorn, que gracias al '--reload'
# todos los cambios que se efectúen en el presente archivo, se verán reflejados en localhost

# previamente instalamos fastapi y uvicorn desde la terminal de visual (con el comando pip install)

# usamos el método get de HTTP que nos permite leer lo que la función devuelve, nos permite 
# obtener un recurso por parte del servidor

@superapi.get('/')

async def bienvenida():
    return 'Hola mortal, bienvenido a mi SuperAPI. Para obtener más información, ingresá con /docs a la documentación'

# conectamos api con dataset de circuits

@superapi.get('/circuits')

async def circuits():
    with open('./Datasets/circuits.json', encoding = 'UTF-8') as c:
        circuits = json.load(c)
    return circuits

# conectamos api con dataset de constructors
# a este hubo que modificarlo porque le faltaban las comas al json

@superapi.get('/constructors')

async def constructors():
    with open('./Datasets/constructors.json', encoding = 'UTF-8') as co:
        constructors = json.load(co)
    return constructors

# conectamos api con dataset de drivers
# a este hubo que modificarlo porque le faltaban las comas al json

@superapi.get('/drivers')

async def drivers():
    with open('./Datasets/drivers.json', encoding = 'UTF-8') as d:
        drivers = json.load(d)
    return drivers

# conectamos api con dataset de pit_stops

@superapi.get('/pit_stops')

async def pit_stops():
    with open('./Datasets/pit_stops.json', encoding = 'UTF-8') as p:
        pit_stops = json.load(p)
    return pit_stops

# conectamos api con dataset de races

@superapi.get('/races')

async def races():
    with open('./Datasets/races.json', encoding = 'UTF-8') as r:
        races = json.load(r)
    return races

# conectamos api con dataset de races

@superapi.get('/results')

async def results():
    with open('./Datasets/results.json', encoding = 'UTF-8') as res:
        results = json.load(res)
    return results

# conectamos api con dataset de qualifying_split_1

@superapi.get('/qualifying/1')

async def qualifying1():
    with open('./Datasets/Qualifying/qualifying_split_1.json', encoding = 'UTF-8') as q1:
        qualifying_split_1 = json.load(q1)
    return qualifying_split_1

# conectamos api con dataset de qualifying_split_2

@superapi.get('/qualifying/2')

async def qualifying2():
    with open('./Datasets/Qualifying/qualifying_split_2.json', encoding = 'UTF-8') as q2:
        qualifying_split_2 = json.load(q2)
    return qualifying_split_2

# CONSULTA 1: AÑO CON MÁS CARRERAS CORRIDAS

# usamos un decorador para la función asíncrona, usamos el método get 
@superapi.get('/año_con_mas_carreras')

async def año_con_mas_carreras():
    
    # creamos un dataframe del archivo json ya que es más ameno para trabajarlo y hacerle consultas usando pandas
    df_races = pd.read_json(r'C:\Users\aalbe\OneDrive\Escritorio\PI01_DATA02\Datasets/races.json')
    
    # de ese dataframe, creamos otro más pequeño, es decir, tomamos la parte que nos interesa para esta consulta:
    contador_carreras = pd.DataFrame(df_races['year'].value_counts())
    
    # al visualizar el código de arriba, al dataframe es necesario hacerle un cambio de índice y un renombramiento de 
    # las columnas (para más esclarecimiento, ver notebook 'consulta1.ipynb)
    contador_carreras.reset_index(inplace=True)
    contador_carreras.rename({'year': 'repeticiones'}, axis=1, inplace=True)
    contador_carreras.rename({'index': 'year'}, axis=1, inplace=True)
    
    # una vez que tenemos el dataframe listo, creamos máscara que muestra mayores repeticiones
    mayor_repeticion = contador_carreras['repeticiones'] == contador_carreras['repeticiones'].max()
    
    return 'El año con mayor cantidad de carreras es el año', int(contador_carreras[mayor_repeticion]['year'].to_string(index = False)), 'con', int(contador_carreras['repeticiones'].max()), 'repeticiones'
    
# CONSULTA 2: PILOTO CON MAYOR CANTIDAD DE PRIMEROS PUESTOS

@superapi.get('/piloto_primeros_puestos')

async def piloto_primeros_puestos():
    
    # levantamos los archivos json necesarios, y los convertimos a dataframe para poder consultar con pandas
    df_results = pd.read_json(r'C:\Users\aalbe\OneDrive\Escritorio\PI01_DATA02\Datasets/results.json')
    df_drivers = pd.read_json(r'C:\Users\aalbe\OneDrive\Escritorio\PI01_DATA02\Datasets/drivers.json')
    
    # realizamos una primer máscara, filtrando únicamente las filas correspondientes a primeros puestos:
    primeros = df_results['position'] == 1
    
    # creamos otro dataframe a través de dicha máscara y se obtiene qué driverId tiene mayor cantidad de primeros puestos
    primeros_puestos = df_results[primeros]['driverId'].value_counts()
    df_primeros_puestos = pd.DataFrame(primeros_puestos)
    
    # le hacemos algunos arreglos
    df_primeros_puestos.reset_index(inplace=True)
    df_primeros_puestos.rename({'driverId': 'repeticiones'}, axis=1, inplace=True)
    df_primeros_puestos.rename({'index': 'driverId'}, axis=1, inplace=True)
    
    # y lo unimos a través del comando .merge al dataframe de drivers, para obtener el nombre del corredor
    drivers_y_primeros_puestos = pd.merge(df_primeros_puestos, df_drivers, on = 'driverId')
    
    # creamos una máscara para obtener la fila del dataframe que tiene la información
    # de aquel corredor que salió más veces primero:
    max_repeticiones = drivers_y_primeros_puestos['repeticiones'] == drivers_y_primeros_puestos['repeticiones'].max()    
    
    return 'El piloto con mayor cantidad de primeros puestos es', str(drivers_y_primeros_puestos[max_repeticiones]['name'].to_string(index = False)) 

# CONSULTA 3: CIRCUITO MÁS CORRIDO

@superapi.get('/circuito_mas_corrido')

async def circuito_mas_corrido():
    
    # levantamos los datasets necesarios para esta consulto con pandas
    df_circuits = pd.read_json(r'C:\Users\aalbe\OneDrive\Escritorio\PI01_DATA02\Datasets/circuits.json')
    df_races = pd.read_json(r'C:\Users\aalbe\OneDrive\Escritorio\PI01_DATA02\Datasets/races.json')
    
    # creamos un df más pequeño con la información necesaria y suficiente para llevar a cabo esta consulta
    circuitos_mas_corridos = df_races['circuitId'].value_counts()
    df_circuitos_mas_corridos = pd.DataFrame(circuitos_mas_corridos)
    
    # le hacemos algunos arreglos pertinentes
    df_circuitos_mas_corridos.reset_index(inplace=True)
    df_circuitos_mas_corridos.rename({'circuitId': 'repeticiones'}, axis=1, inplace=True)
    df_circuitos_mas_corridos.rename({'index': 'circuitId'}, axis=1, inplace=True)
    
    # unimos con el método .merge de pandas, el pequeño dataframe que creamos anteriormente con el dataframe
    # que contiene la información sobre los circuitos, del cual necesitamos el nombre del circuito:
    circuits_y_mas_corridos = pd.merge(df_circuitos_mas_corridos, df_circuits, on = 'circuitId')
    
    # filtramos este dataframe mergeado con la siguiente máscara, que filtra la fila en la que las repeticiones 
    # son máximas:
    mascara_mas_corrido = circuits_y_mas_corridos['repeticiones'] == circuits_y_mas_corridos['repeticiones'].max() 
    
    return 'El circuito más corrido es', str(circuits_y_mas_corridos[mascara_mas_corrido]['name'].to_string(index=False))    
        
# CONSULTA 4: PILOTO CON MÁS PUNTOS, Y CONSTRUCTOR BRITISH O AMERICAN

@superapi.get('/piloto_con_mas_puntos')

async def piloto_con_mas_puntos():
    
    # leemos y pasamos a dataframe los archivos necesarios para esta consulta:
    df_results = pd.read_json(r'C:\Users\aalbe\OneDrive\Escritorio\PI01_DATA02\Datasets/results.json')
    df_drivers = pd.read_json(r'C:\Users\aalbe\OneDrive\Escritorio\PI01_DATA02\Datasets/drivers.json')
    df_constructors = pd.read_json(r'C:\Users\aalbe\OneDrive\Escritorio\PI01_DATA02\Datasets/constructors.json')
    
    # unimos a través de .merge de pandas, los df results y constructors, usando la clave 'constructorId', con el fin de 
    # filtrar primero al constructor, que sea british o american:
    results_y_constructors = pd.merge(df_results, df_constructors, on = 'constructorId')
    
    # máscara que filtra el df mergeado para obtener sólo constructors british o american
    british_o_american = results_y_constructors['nationality'].isin(['British','American'])
    
    # filtramos el df mergeado anteriormente
    df_results_y_constructors = pd.DataFrame(results_y_constructors[british_o_american].groupby('driverId')['points'].sum().sort_values(ascending = False))
    
    # le hacemos un pequeño arreglo
    df_results_y_constructors.reset_index(inplace = True)
    
    # volvemos a realizar otro merge para unir el anterior a drivers, a través de driverId
    drivers_y_df_results_y_constructors = pd.merge(df_results_y_constructors, df_drivers, on = 'driverId')
    
    # creamos una segunda máscara que va a filtrar el df de arriba buscando el driver con mayor cantidad de puntos
    piloto_mas_puntos = drivers_y_df_results_y_constructors['points'] == drivers_y_df_results_y_constructors['points'].max()
    
    return 'El piloto con más puntos, siendo el constructor Británico o Norteamericano, es', str(drivers_y_df_results_y_constructors['name'][piloto_mas_puntos].to_string(index = False)), 'con una cantidad total de puntos de', int(df_results_y_constructors['points'].max())
