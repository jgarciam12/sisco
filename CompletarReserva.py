# -*- coding: utf-8 -*-
"""
Created on Wed Jun 28 12:13:38 2023

@author: jcgarciam
"""

import pandas as pd
import glob

path = r'D:\DATOS\Users\jcgarciam\Downloads'

reserva_2017_2019 = pd.read_excel(path + '/Revisión Reserva.xlsx', header = 0, sheet_name = '2017-2019')

#%%
reserva_2017_2019 = reserva_2017_2019[reserva_2017_2019['Cod_Barra'].isnull() == False]

#%%

reserva_2020_2021 = pd.read_excel(path + '/Revisión Reserva.xlsx', header = 0, sheet_name = '2020-2021')

#%%
reserva_2020_2021 = reserva_2020_2021[reserva_2020_2021['Cod_Barra'].isnull() == False]

#%%

import numpy as np
def CambioFormato(df, a = 'a'):
    df[a] = df[a].astype(str).str.strip().str.strip('\x02').str.strip('')
    df[a] = np.where(df[a].str[-2::] == '.0', df[a].str[0:-2], df[a])
    df[a] = np.where(df[a] == 'nan', np.nan, df[a])
    return df[a]

#%%

reserva_2017_2019['Cod_Barra'] = CambioFormato(reserva_2017_2019, a = 'Cod_Barra')
reserva_2020_2021['Cod_Barra'] = CambioFormato(reserva_2020_2021, a = 'Cod_Barra')

reserva_2017_2019['Cod_Barra_Pago_Parcial'] = CambioFormato(reserva_2017_2019, a = 'Cod_Barra_Pago_Parcial')
reserva_2020_2021['Cod_Barra_Pago_Parcial'] = CambioFormato(reserva_2020_2021, a = 'Cod_Barra_Pago_Parcial')

#%%

path_entrada2 = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\19-Soat-Salud-Arl\4-TRANSVERSAL\SISCO\SISCO\General\Salidas\SALUD\CONSOLIDADO_SISCO'


#%%

dic = {}

columnas = ['Cod_Barra','Fecha_OP','Numero_OP','Valor_Pagado_Egresos','Estado_Pago','Rete_ICA','Rete_Fuente','Estado_actual',
            'Numero_Factura_Original','NIT']
print('\n Inicio de carga de archivos SISCO Salud \n')
for Archivo in glob.glob(path_entrada2 + '\*csv'):
    print('Cargando archivo ', Archivo[-20:-4])
    df = pd.read_csv(Archivo, sep = '|', header = 0)#, usecols = columnas)
    df['Origen'] = 'Salud'
    print('Archivo ', Archivo[-20:-4], ' cargado \n')
    dic[Archivo] = df
    
#%%   

sisco = pd.concat(dic).reset_index(drop = True)

#%%

sisco['Folio_BH'] = CambioFormato(sisco, a = 'Folio_BH')
sisco['Cod_Barra'] = CambioFormato(sisco, a = 'Cod_Barra')

#%%

sisco2 = sisco[['Cod_Barra','Fecha_Ingreso_BH','Estado_RR','Fecha_Ultimo_estado','Estado_actual']]

sisco2 = sisco2.rename(columns = {'Fecha_Ingreso_BH':'Ultima Fecha_Ingreso_BH','Estado_RR':'Estado_BH',
                                  'Estado_actual':'Estado Activa','Fecha_Ultimo_estado':'Fecha Estado Activa'})


reserva_2017_2019[reserva_2017_2019['Cod_Barra'].isin(sisco['Cod_Barra']) == True]
reserva_2020_2021[reserva_2020_2021['Cod_Barra'].isin(sisco['Cod_Barra']) == True]

reserva_2017_2019_2 = reserva_2017_2019.merge(sisco2, how = 'left', on = 'Cod_Barra')
reserva_2020_2021_2 = reserva_2020_2021.merge(sisco2, how = 'left', on = 'Cod_Barra')

#%%



writer= pd.ExcelWriter(path + '/Actualizacion Consulta Reserva Salud.xlsx')

reserva_2017_2019_2.to_excel(writer, index = False, sheet_name = '2017-2019')
reserva_2020_2021_2.to_excel(writer, index = False, sheet_name = '2020-2021')


writer.save()
writer.close()
print('Proceso finalizado con exito. Presionar ENTER para salir')