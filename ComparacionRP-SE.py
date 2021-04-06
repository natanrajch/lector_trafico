import pandas as pd
import numpy as np
import glob
import json
import datetime

#REQUIERE LOS DATOS DE SE EN UN FORMATO SIMPLIFICADO
base_movimientos = pd.read_csv("base_movimientos.csv", sep=',')
servicio_efectuado = pd.read_csv('ServicioEfectuado290920.csv', sep=';',encoding='utf-8-sig')

#FUNCIONES VS
def tipo_cabecera(row):
    if pd.isnull(row['Lugar']):
        val=''
    elif 'Pre' in row['Lugar']:
        val = ""
    elif 'cochera' in row['Lugar']:
        val=""
    elif 'Retiro' in row['Lugar']:
        val="D"
    elif 'Virreyes' in row['Lugar']:
        val="A"
    else:
        val=""
    return val
def get_sec(time_str):
    """Get Seconds from time."""
    h, m, s = time_str.split(':')
    return int(h) * 3600 + int(m) * 60 + int(s)
from bisect import bisect_left

def take_closest(myList, myNumber):
    """
    Assumes myList is sorted. Returns closest value to myNumber.

    If two numbers are equally close, return the smallest number.
    """
    pos = bisect_left(myList, myNumber)
    if pos == 0:
        return myList[0]
    if pos == len(myList):
        return myList[-1]
    before = myList[pos - 1]
    after = myList[pos]
    if after - myNumber < myNumber - before:
       return after
    else:
       return before

#TRANSFORMACIONES DE DATAFRAMES PARA AGREGARLE COLUMNAS DE ANALISIS
base_movimientos['tipo_cabecera'] = base_movimientos.apply(tipo_cabecera, axis=1)
base_movimientos.sort_values(by=['IDTren','hora_entrada'], inplace = True)
proxima_salida = base_movimientos.groupby(['IDTren','hora_entrada']).size().reset_index()
#ITERACION TRAE PROXIMO HORARIO DE PISADA DE CV TRAS ESTAR EN UN CV
for row in proxima_salida.itertuples():
    #print(row)
    if row.Index != (len(proxima_salida)-1):
        if proxima_salida.IDTren[row.Index+1] == row.IDTren:
            proxima_salida.loc[row.Index,'proxima_salida'] = proxima_salida.hora_entrada[row.Index+1]
        else:
            proxima_salida.loc[row.Index,'proxima_salida'] = ""
    else:
        proxima_salida.loc[row.Index,'proxima_salida'] = ""


salidas_cabeceras = base_movimientos.loc[(base_movimientos['tipo_cabecera'] == "A") 
                                            | (base_movimientos['tipo_cabecera'] == "D")].reset_index(drop=True)
salidas_cabeceras['tren_comercial'] = ""

#ITERACION DEFINE CUALES SON TRENES COMERCIALES: SI SALIO DE A Y LLEGO A D O VICEVERSA UNICAMENTE
for row in salidas_cabeceras.itertuples():
    if row.Index != (len(salidas_cabeceras)-1):
        if salidas_cabeceras.IDTren[row.Index+1] != row.IDTren:
            salidas_cabeceras.at[row.Index,'tren_comercial'] = 0
        elif    ((salidas_cabeceras.tipo_cabecera[row.Index] == "A" 
                 and salidas_cabeceras.tipo_cabecera[row.Index+1] == "D")
                | (salidas_cabeceras.tipo_cabecera[row.Index] == "D"
                 and salidas_cabeceras.tipo_cabecera[row.Index+1] == "A")):
            salidas_cabeceras.at[row.Index,'tren_comercial'] = 1
        else:
            salidas_cabeceras.at[row.Index,'tren_comercial'] = 0
    else:
        salidas_cabeceras.at[row.Index,'tren_comercial'] = 0

trenes_comerciales = salidas_cabeceras.loc[salidas_cabeceras['tren_comercial']==1].reset_index(drop=True)
trenes_comerciales = trenes_comerciales.merge(proxima_salida, how='left', on=['IDTren','hora_entrada'])
trenes_comerciales.drop(['CV','hora_entrada','Lugar','nace_finaliza','tren_comercial',0], axis=1, inplace=True)
#SE TRABAJAN LOS STRING DE HORA EN SEGUNDOS MEDIANTE LA FUNC GET_SEC
trenes_comerciales['proxima_salida'] = [get_sec(trenes_comerciales['proxima_salida'][i]) for i in range(len(trenes_comerciales))]

#SE GENERA UN DATAFRAME DE COMPARACION PARA IR ACOMODANDO
comparacion_rp_se = servicio_efectuado.copy()
comparacion_rp_se['IDTren'] = ''
comparacion_rp_se['proxima_salida'] = ''
comparacion_rp_se['Hora Sale'] = [get_sec(comparacion_rp_se['Hora Sale'][i]) for i in range(len(comparacion_rp_se))]
comparacion_rp_se.sort_values(['Equipo','Hora Sale'],inplace=True)
comparacion_rp_se = comparacion_rp_se.reset_index(drop=True)

nombres_equipo = servicio_efectuado['Equipo'].unique()

#POR CADA TREN DEL LECTOR DE RP, CHEQUEA CONTRA EL SE EL MEJOR MATCH (CONSIDERANDO CABECERA DE SALIDA + HORARIO)
for numero_tren in trenes_comerciales['IDTren'].unique():
    rp_salidas_tren = trenes_comerciales.loc[trenes_comerciales['IDTren']==numero_tren].reset_index(drop=True)
    primera_salida = trenes_comerciales['proxima_salida'].loc[trenes_comerciales['IDTren']==numero_tren].iloc[0]
    tipo_cabecera = trenes_comerciales['tipo_cabecera'].loc[trenes_comerciales['IDTren']==numero_tren].iloc[0]
    horarios_cercanos = []
    diferencias=[]
    equipos_posibles=[]
    for equipo in nombres_equipo:
        se_por_equipo = comparacion_rp_se.loc[(comparacion_rp_se['Equipo']==equipo) 
                                            & (comparacion_rp_se['IDTren']=='')].reset_index(drop=True)
        hora_por_cabecera = list(se_por_equipo.loc[se_por_equipo['Circulacion']==tipo_cabecera]['Hora Sale'])#.reset_index(drop=True)

        if len(hora_por_cabecera) > 0:
            hora_cercana = take_closest(hora_por_cabecera,primera_salida)
            pos_hora_cercana = list(se_por_equipo['Hora Sale']).index(hora_cercana)

            if len(rp_salidas_tren) <= len(se_por_equipo[pos_hora_cercana:]):
                diferencia_con_equipo = sum(abs((rp_salidas_tren['proxima_salida']
                                            -se_por_equipo['Hora Sale'][pos_hora_cercana:].reset_index(drop=True)).dropna()))
                equipos_posibles.append(equipo)
                horarios_cercanos.append(hora_cercana)
                diferencias.append(diferencia_con_equipo)

    equipo_elegido = equipos_posibles[diferencias.index(min(diferencias))]
    horario_elegido = horarios_cercanos[diferencias.index(min(diferencias))]  

    print('a equipo', equipo_elegido, horario_elegido)
    pos_global_a_pegar = np.where((comparacion_rp_se['Equipo']==equipo_elegido) & (comparacion_rp_se['Hora Sale']==horario_elegido))[0]
    pos_global_a_pegar = pos_global_a_pegar.item()
    
    #PEGA EL TREN DEL LECTOR RP EN EL DATAFRAME DE COMPARACION, ASI YA NO SE USAN MAS ESOS ROWS
    for i in range(len(rp_salidas_tren)):
        comparacion_rp_se.loc[pos_global_a_pegar+i,'IDTren'] = numero_tren
        comparacion_rp_se.loc[pos_global_a_pegar+i,'proxima_salida'] = rp_salidas_tren['proxima_salida'][i]


#Convertir segundos en string de hora
comparacion_rp_se['Hora Sale'] = [str(datetime.timedelta(seconds=comparacion_rp_se['Hora Sale'][i].item())) for i in range(len(comparacion_rp_se))]
comparacion_rp_se['proxima_salida'] = ['' if comparacion_rp_se['proxima_salida'][i] == '' else str(datetime.timedelta(seconds=comparacion_rp_se['proxima_salida'][i].item())) for i in range(len(comparacion_rp_se))]
print(comparacion_rp_se)

#exportar a csv
try:
    comparacion_rp_se.to_csv('comparacion_rp_se.csv', index = False,encoding='utf-8-sig')
except:
    print('debe estar abierta la base. Cerrarla antes de darle Enter')
    input('Cerrar base antes de dar Enter')

    


