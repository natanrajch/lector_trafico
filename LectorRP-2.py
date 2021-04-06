import pandas as pd
import numpy as np
import glob
import json
import datetime

 

relaciones = pd.read_csv("Relaciones3.csv", sep=';')
relaciones = relaciones.groupby('ID')['Relacionado con'].apply(list).reset_index(name='Relacionado_con')
nombres = pd.read_csv("cvs-nombres.csv", sep=';', encoding='iso-8859-1')
 

circuitos_de_creacion = pd.read_csv("circuitos_creacion.csv", sep=';')
cvs_de_creacion = pd.read_csv("circuitos_creacion.csv", sep=';').circuito_de_creacion.values
cvs_creacion_virreyes = ['15V02TP','15V03TP']
cvs_limite_virreyes = ['14V03TP']

 

datos_dia =  json.load(open("29092020.json","rb"))
registros = len(datos_dia['reg'])
cvs_r_guardados = []
data = [[-1,[]],[0,[]]]
tren_id = 1
tren_desaparecido = []
cvs_guard_ant = 0
trenes_activos = pd.DataFrame(data, columns=['IDTren','CVS'])
base_movimientos = pd.DataFrame(columns=['IDTren','CV','hora_entrada'])
trenes_activos_anterior = pd.DataFrame(columns=['IDTren','CVS'])

 
#Crea listado de CIRCUITOS QUE CAMBIAN para 1 horario/Reg
cambio_vacio = [[''], ['']]
for registro in range(registros):
    rows = []
    for item in datos_dia['reg'][registro]['Datos']['cambios']:
        row = item[0:2]
        rows.append(row)
 
    for item in datos_dia['reg'][registro]['Datos']['circuitovias']:
        row = item[0:2]
        rows.append(row)

    if rows != cambio_vacio: #Saltea si el registro no incluye ni circuitos ni cambios.
        circuitos_que_cambian = pd.DataFrame.from_dict(rows)
        circuitos_que_cambian.columns = ['Circuito','Estado'] 


        #CIRCUITOS QUE SE PUSIERON ROJOS
        cvs_a_rojo = circuitos_que_cambian.loc[(circuitos_que_cambian['Estado'] == '1') 
                                                |(circuitos_que_cambian['Estado'].str[-2:]=='01')
                                                |(circuitos_que_cambian['Estado'].str[-2:]=='08')]
        cvs_a_rojo = pd.merge(cvs_a_rojo, relaciones, how='left', 
                              left_on=['Circuito'], right_on=['ID'])[['Circuito','Relacionado_con']]
        cvs_a_rojo.rename(columns={'Circuito':'Circuitos', 'Relacionado_con':'Relacionado_con'}, inplace = True)

        #Agrega a los cvs a rojo, los guardados de registros anteriores
        tiempo_max_guardado = datetime.timedelta(minutes=1.5)
        #print(datos_dia['reg'][registro]['Hora'] + '--')
        tiempo_reg_actual = datetime.datetime.strptime(datos_dia['reg'][registro]['Hora'].strip(),'%H:%M:%S')
   
        for cv_guardado in cvs_r_guardados:
            if (tiempo_reg_actual - datetime.datetime.strptime(cv_guardado[2],'%H:%M:%S')) <= tiempo_max_guardado:
                cvs_a_rojo.loc[len(cvs_a_rojo)+1] = [cv_guardado[0],cv_guardado[1]]

        cvs_r_guardados = [cv_guardado for cv_guardado in cvs_r_guardados if (tiempo_reg_actual - datetime.datetime.strptime(cv_guardado[2],'%H:%M:%S')) <= tiempo_max_guardado]

        #Forzar columna "Relacionado_con" a todos list para que funcione la funcion crear df-joins_a_rojo DF-JOINS_A_ROJO
        for cv_a_rojo in cvs_a_rojo.itertuples():
            if not(type(cv_a_rojo.Relacionado_con) is list):
                cvs_a_rojo.Relacionado_con[cv_a_rojo.Index] = [cvs_a_rojo.Relacionado_con[cv_a_rojo.Index]]

        #Crear DF JOINS a ROJO (Cvs contiguos que se ponen rojos)
        joins_a_rojo = pd.DataFrame(columns=['Circuitos','Relacionados_con'])
        joins_a_rojo['Circuitos'] = [[] for _ in cvs_a_rojo.Circuitos]
        joins_a_rojo['Relacionados_con'] = [[] for _ in cvs_a_rojo.Circuitos]

        saltear = []
        escribir_en = 0
        for cv_a_rojo in cvs_a_rojo.itertuples():
            if cv_a_rojo.Index not in saltear:
                joins_cvs = [cv_a_rojo.Circuitos]
                joins_relaciones = []
                cvs_ya_unidos = []
                agregado = 1
                while agregado > 0:
                    agregado = 0
                    for join in joins_cvs:
                        if join not in cvs_ya_unidos:
                            cvs_ya_unidos.append(join)
                            for cv_relacionado in cvs_a_rojo.itertuples():
                                if join in cv_relacionado.Relacionado_con:
                                    if cv_relacionado.Circuitos not in joins_cvs:
                                        joins_cvs.append(cv_relacionado.Circuitos)
                                    joins_relaciones = list(set(joins_relaciones).union(set(cv_relacionado.Relacionado_con)))
                                    saltear.append(cv_relacionado.Index)
                                    agregado += 1

                joins_a_rojo.Circuitos[escribir_en] = joins_cvs
                joins_a_rojo.Relacionados_con[escribir_en] = joins_relaciones
                escribir_en += 1

        #CHEQUEAR JOINS-ROJOS VS TRENES ACTIVOS y MODIFICAR o CREAR Trenes activos Según corresponda

        for join_a_rojo in joins_a_rojo.itertuples():



            activado = 0

            for tren_activo in trenes_activos.itertuples():
                if not(set(join_a_rojo.Relacionados_con).isdisjoint(tren_activo.CVS)): #join TIENE relacion con Tren activo
                    #1 MODIFICAR TREN ACTIVO (AGREGAR NUEVOS CVS ROJOS)
                    trenes_activos.at[tren_activo.Index,'CVS'] = list(set(tren_activo.CVS).union(set(join_a_rojo.Circuitos)))
                    activado = 1

                    # 2 AGREGAR dato a base_movimientos
                    for pisada_cv in join_a_rojo.Circuitos: #Para los rows vacíos el loop los saltea, no considera pisada_cv = ''
                        if not(pisada_cv in tren_activo.CVS):
                            base_movimientos.loc[len(base_movimientos)+1] = [tren_activo.IDTren, pisada_cv, datos_dia['reg'][registro]['Hora']]

            if activado == 0:  #No se activó el Modificar el tren activo
                #CREAR NUEVO TREN ACTIVO

                # 1 AGREGAR Tren a Trenes_Activos

                if registro == 0: #Para primer registro, crear todos
                    if not(not(join_a_rojo.Circuitos)): #Chequea que no esté vacío
                        #max_id = trenes_activos['IDTren'].max()
                        trenes_activos.loc[len(trenes_activos)+1] = [tren_id, join_a_rojo.Circuitos]
                # 2 AGREGAR dato a base_movimientos
                    for pisada_cv in join_a_rojo.Circuitos: #Para los rows vacíos el loop los saltea, no considera nuevo_cv = ''
                        base_movimientos.loc[len(base_movimientos)+1] = [tren_id, pisada_cv, datos_dia['reg'][registro]['Hora']]
                    tren_id += 1               

                else: #Para el resto de los registros, chequea si es posible que se haya creado un tren en el cv
                    if not(not(join_a_rojo.Circuitos)): #Chequea que no esté vacío
                        if not(set(join_a_rojo.Circuitos).isdisjoint(cvs_de_creacion)): #Es lógico crear un tren en este cv
                            if len(tren_desaparecido) > 0: #Chequea caso de trenes desaparecidos
                                if not(set(join_a_rojo.Circuitos).isdisjoint(cvs_creacion_virreyes)):
                                    trenes_activos.loc[len(trenes_activos)+1] = [tren_desaparecido[0][0], join_a_rojo.Circuitos]
                                    for pisada_cv in join_a_rojo.Circuitos: #Para los rows vacíos el loop los saltea, no considera nuevo_cv = ''
                                        base_movimientos.loc[len(base_movimientos)+1] = [tren_desaparecido[0][0], pisada_cv, datos_dia['reg'][registro]['Hora']]
                                    tren_desaparecido = []

                            else:                 
                                trenes_activos.loc[len(trenes_activos)+1] = [tren_id, join_a_rojo.Circuitos] 
                                for pisada_cv in join_a_rojo.Circuitos: #Para los rows vacíos el loop los saltea, no considera nuevo_cv = ''
                                    base_movimientos.loc[len(base_movimientos)+1] = [tren_id, pisada_cv, datos_dia['reg'][registro]['Hora']]
                                tren_id += 1

                        else: #no puede haber aparecido un tren en este(os) cv(s). Se guarda el join_a_rojo para proximos registros
                            cvsguard = [cvs_r_guardados[i][0] for i in range(len(cvs_r_guardados))]
                            for pisada_cv in join_a_rojo.Circuitos:                                                             
                                if pisada_cv not in cvsguard:
                                    cvs_r_guardados.append([pisada_cv,join_a_rojo.Relacionados_con, datos_dia['reg'][registro]['Hora'].strip()])

        #CIRCUITOS QUE SE PUSIERON NO-ROJOS
        cvs_a_gris = circuitos_que_cambian.loc[(circuitos_que_cambian['Estado'] != '1') & (circuitos_que_cambian['Estado'].str[-2:] !='01') & (circuitos_que_cambian['Estado'].str[-2:] !='08')]
        #BORRAR CVS GRISES DE TRENES_ACTIVOS:
        for tren_activo in trenes_activos.itertuples():
            for cv_activo in tren_activo.CVS:
                if cv_activo in cvs_a_gris.Circuito.values:
                    if (len(tren_activo.CVS) == 1) & (cv_activo in cvs_limite_virreyes): #TRENES DESAPARECIDOS por CAMBIOS VIOLETA EN VIRREYES 
                        tren_desaparecido.append([tren_activo.IDTren,cv_activo])
                    trenes_activos.CVS[tren_activo.Index].remove(cv_activo)
        #Chequea casos donde un tren sale de cabecera, pero no se pone gris el cdv de cabecera
        cabeceras = [['22V03TK',['21V02TK','21V01TK']], 
                     ['22V02TK',['21V02TK','21V01TK']], 
                     ['15V03TP',['14V01TP','14V02TP']],
                     ['15V02TP',['14V01TP','14V02TP']]] 
        for tren_activo in trenes_activos.itertuples():
            for cabecera in cabeceras: 
                if cabecera[0] in tren_activo.CVS and len(tren_activo.CVS) > 1:
                    tren_en_2_estaciones = any(cv in tren_activo.CVS for cv in cabecera[1])
                    if tren_en_2_estaciones:
                        trenes_activos.CVS[tren_activo.Index].remove(cabecera[0])
        #CHEQUEO DE COLISIONES
        remover_trenes = []
        lista_cvs_trenes = list(trenes_activos['CVS'])
        cvs_activados = [cv for tren_activo_cvs in lista_cvs_trenes for cv in tren_activo_cvs]
        if len(cvs_activados) != len(set(cvs_activados)): #Existen colisiones
            visto = {}
            cvs_duplicados = [] #Armo lista de duplicados
            for cv in cvs_activados:
                if cv not in visto:
                    visto[cv] = 1
                else:
                    if visto[cv] == 1:
                        cvs_duplicados.append(cv)
                    visto[cv] += 1
            for cv_duplicado in cvs_duplicados:
                if cv_duplicado in cvs_de_creacion:
                    #Borrar el tren_activo que estaba de antes en la cabecera:                                       
                    for tren_activo in trenes_activos.itertuples():
                        tren_anterior = list(trenes_activos_anterior.loc[trenes_activos_anterior['IDTren'] == tren_activo.IDTren,'CVS'])[0]
                        if (cv_duplicado in tren_activo.CVS) & (cv_duplicado in tren_anterior):
                            remover_trenes.append(tren_activo.Index)
                            print('tren a remover es', tren_activo.Index)
        trenes_activos = trenes_activos.drop(remover_trenes) #Quita los trenes duplicados en cabecera
        trenes_activos = trenes_activos.reset_index(drop=True) #Resetea el index, para que el metodo Crear Trenes no traiga problemas
        #Creo una copia de los trenes_activos para comparar en el proximo loop
        trenes_activos_anterior = trenes_activos.copy() 


        


        
    
    print(registro, 'h:',datos_dia['reg'][registro]['Hora'],'cvs guardados:', len(cvs_r_guardados))

    
#SE AGREGAN COLUMNAS DE ANALISIS A LA BASE_MOVIMIENTOS Y SE EXPORTA A CSV
nace = [tren in list(base_movimientos.IDTren[0:ind]) for (ind,tren) in enumerate(base_movimientos.IDTren)]
nace = ["" if i else "Nace" for i in nace]
finaliza = [tren in list(base_movimientos.IDTren[(ind+1):(len(base_movimientos)+1)]) for (ind,tren) in enumerate(base_movimientos.IDTren)]
finaliza = ["" if i else "Finaliza" for i in finaliza]
nace_finaliza = [i+j for (i,j) in zip(nace, finaliza)]

base_movimientos = base_movimientos.merge(nombres, how='left', on='CV')
base_movimientos['nace_finaliza'] = nace_finaliza
try:
    base_movimientos.to_csv('base_movimientos.csv', index = False,encoding='utf-8-sig')
except:
    print('debe estar abierta la base. Cerrarla antes de darle Enter')
    input('Cerrar base antes de dar Enter')

#base_movimientos.to_excel(r'C:\Users\Natan\Documents\QuickstartSheets\base_movimientos.xlsx', index = False)