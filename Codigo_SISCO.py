# -*- coding: utf-8 -*-
"""
Created on Tue Jun 28 07:27:39 2022

@author: jcgarciam
"""

import pandas as pd
import numpy as np
import zipfile
from datetime import datetime
import os
import glob
from Extraccion import ExtraccionBases


#%%
Tiempo_Total = datetime.now()

Current_Date = datetime.today().date()
Current_Date = Current_Date.strftime('%Y')+Current_Date.strftime('%m')+ '17'#Current_Date.strftime('%d')

path1 = r'\\dc1pcadfrs1\Reportes_Activa\axa'
path2 = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\19-Soat-Salud-Arl\4-TRANSVERSAL\SISCO\SISCO\General\Entradas\AS400'
path3 = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\19-Soat-Salud-Arl\4-TRANSVERSAL\SISCO\SISCO\General\Entradas\Glosas'
path4 = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\19-Soat-Salud-Arl\4-TRANSVERSAL\SISCO\SISCO\General\Entradas\BH'
path5 = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\19-Soat-Salud-Arl\4-TRANSVERSAL\SISCO\SISCO\General\Entradas\goanywhere'

path_salida1 = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\19-Soat-Salud-Arl\4-TRANSVERSAL\SISCO\SISCO\General\Salidas\SALUD'
path_salida2 = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\19-Soat-Salud-Arl\4-TRANSVERSAL\SISCO\SISCO\General\Salidas\ARL'

path_temporal = r'D:\DATOS\Users\jcgarciam\OneDrive - AXA Colpatria Seguros\Documentos\Informes\SISCO'
#%%

Maestro, Pagos_Parciales, Pagos, Asistenciales, Glosas, RR_Cuentas_medicas, RPT_RADICADOS_ARL = ExtraccionBases(path1, path2, path3, path4, path5, Current_Date)


#%%
RPT = RPT_RADICADOS_ARL['CODIGO_BARRAS'].drop_duplicates().str.strip()
Pagos_Parciales['Cod_Barra'] = Pagos_Parciales['Cod_Barra'].str.strip()
Pagos_Parciales['RPT'] = Pagos_Parciales['Cod_Barra'].isin(RPT)


#%%
Maestro['Cod_Barra'] = Maestro['Cod_Barra'].astype(str)
Pagos_Parciales['Cod_Barra'] = Pagos_Parciales['Cod_Barra'].astype(str)

Maestro['Cod_Barra'] = Maestro['Cod_Barra'].str.strip('\x02')
Maestro['Cod_Barra'] = Maestro['Cod_Barra'].str.strip()
Pagos_Parciales['Cod_Barra'] = Pagos_Parciales['Cod_Barra'].str.strip('\x02')
Pagos_Parciales['Cod_Barra'] = Pagos_Parciales['Cod_Barra'].str.strip()

Pagos_Parciales['Origen'] = 'Pagos Parciales'

Maestro_Sin_PP = Maestro[~Maestro['Cod_Barra'].isin(Pagos_Parciales['Cod_Barra'].unique())]
Maestro_Sin_PP['Origen'] = 'Maestro'
#%%
Pagos_Parciales = Pagos_Parciales.rename(columns = {'Fac_Nroint':'Fac_Norint','Estado_Pago':'Estado_actual'})
Maestro_Sin_PP = Maestro_Sin_PP.rename(columns = {'Numero_Factura':'Numero_Factura_Original','Valor_Neto':'Valor_Pagado',
                                                  'Contrato_Capita':'Numero_Contrato'})
#Maestro_Sin_PP['Cod_Barra_Pago_Parcial'] = Maestro_Sin_PP['Cod_Barra'].copy()
Maestro_Consolidado = pd.concat([Pagos_Parciales,Maestro_Sin_PP]).reset_index(drop = True)

#%%
print('Estos son los registros únicos del campo Regimen ',Maestro_Consolidado['Regimen'].unique())

Maestro_ARL = Maestro_Consolidado[Maestro_Consolidado['Regimen'].str.upper().str.contains('ARL')]
Maestro_Salud = Maestro_Consolidado[~Maestro_Consolidado['Regimen'].str.upper().str.contains('ARL')]

#%%

RR_Cuentas_medicas = RR_Cuentas_medicas.drop_duplicates('FOLIO', keep = 'last')

Salud_No_registra_RR = Maestro_Salud[~Maestro_Salud['Cod_Barra'].isin(RR_Cuentas_medicas['FOLIO'])]
Salud_No_registra_RR = Salud_No_registra_RR[~Salud_No_registra_RR['Cod_Barra_Pago_Parcial'].isin(RR_Cuentas_medicas['FOLIO'])]
#Salud_No_registra_RR.to_csv(path_salida1 + '\Salud_No_registra_RR.csv', index = False, sep = '|')

#%%
RR_Cuentas_medicas1 = RR_Cuentas_medicas[['FECHA INGRESO', 'ESTADO','FOLIO']]
Maestro_Salud = Maestro_Salud.merge(RR_Cuentas_medicas1, how = 'left', left_on = 'Cod_Barra', right_on = 'FOLIO')
#%%
Maestro_Salud = Maestro_Salud.merge(RR_Cuentas_medicas1, how = 'left', left_on = 'Cod_Barra_Pago_Parcial', right_on = 'FOLIO')
#%%

Maestro_Salud['FECHA INGRESO_y'] = Maestro_Salud['FECHA INGRESO_y'].fillna(0)
Maestro_Salud['ESTADO_y'] = Maestro_Salud['ESTADO_y'].fillna(0)
Maestro_Salud['FOLIO_y'] = Maestro_Salud['FOLIO_y'].fillna(0)
Maestro_Salud['FOLIO_x'] = Maestro_Salud['FOLIO_x'].fillna(0)

#%%

Maestro_Salud['FECHA INGRESO_BH'] = np.where(Maestro_Salud['FECHA INGRESO_y'] != 0,Maestro_Salud['FECHA INGRESO_y'],Maestro_Salud['FECHA INGRESO_x'])
Maestro_Salud['Estado_RR'] = np.where(Maestro_Salud['ESTADO_y'] != 0,Maestro_Salud['ESTADO_y'],Maestro_Salud['ESTADO_x'])
Maestro_Salud['Marcacion_RR'] = np.where((Maestro_Salud['FOLIO_y'] != 0) | (Maestro_Salud['FOLIO_x'] != 0), 'Si', 'No')
Maestro_Salud['Fecha_OP_y'] = Maestro_Salud['Fecha_OP_y'].fillna('sin fecha')
Maestro_Salud['Folio'] = np.where(Maestro_Salud['FOLIO_y'] != 0,Maestro_Salud['FOLIO_y'],Maestro_Salud['FOLIO_x'])

#%%
Pagos = Pagos[['NUREFE','FEESOP','NUORPA','VANEPG','VAORPA','IVA','RTE_FUE','RTE_ICA', 
                           'RTE_IVA','ESTOPG','CODFPG','ESTCAU','CODCOM','CODFUE']]
Pagos['FEESOP'] = Pagos['FEESOP'].replace(50210915,20210915)
Pagos['FEESOP'] =pd.to_datetime(Pagos['FEESOP'], format = '%Y%m%d')
Pagos['FEESOP'] = Pagos['FEESOP'].dt.strftime('%d/%m/%Y')

Pagos = Pagos.rename(columns = {'FEESOP':'Fecha_OP','NUORPA':'Numero_OP','VAORPA':'Valor_OP',
                                            'VANEPG':'Valor_Pagado_Egresos','ESTOPG':'Estado_Pago',
                                            'CODFPG':'Codigo_Forma_Pago',
                                            'ESTCAU':'Estado_Egresos'})

Pagos['NUREFE'] = Pagos['NUREFE'].astype(str)


#%%
Pagos_Salud = Pagos[Pagos['CODCOM'].isin([13,17])]

Pagos_Salud = Pagos_Salud.drop_duplicates('NUREFE')

#%%

Salud_Sin_Ingresos_Pagos = Maestro_Salud[~Maestro_Salud['Cod_Barra'].isin(Pagos_Salud['NUREFE'])]
Salud_Sin_Ingresos_Pagos = Salud_Sin_Ingresos_Pagos[~Salud_Sin_Ingresos_Pagos['Cod_Barra_Pago_Parcial'].isin(Pagos_Salud['NUREFE'])]
#%%

Salud_Sin_Ingresos_Pagos.to_csv(path_salida1 + '\Salud_Sin_Ingresos_Pagos.csv', index = False, sep = '|')

#%%

Maestro_Salud = Maestro_Salud.merge(Pagos_Salud, how = 'left', left_on = 'Cod_Barra', right_on = 'NUREFE')
#%%

Maestro_Salud = Maestro_Salud.merge(Pagos_Salud, how = 'left', left_on = 'Cod_Barra_Pago_Parcial', right_on = 'NUREFE')
#%%
Maestro_Salud['NUREFE_y'] = Maestro_Salud['NUREFE_y'].fillna(0)
Maestro_Salud['NUREFE_x'] = Maestro_Salud['NUREFE_x'].fillna(0)
Maestro_Salud['Marcacion_Egresos_Imp'] = np.where((Maestro_Salud['NUREFE_y'] != 0) | (Maestro_Salud['NUREFE_x'] != 0), 'Si', 'No')

Maestro_Salud['Fecha_OP_y'] = Maestro_Salud['Fecha_OP_y'].fillna('sin fecha')
Maestro_Salud['Pagos_Salud.Fecha_OP'] = np.where(Maestro_Salud['Fecha_OP_y'] != 'sin fecha',Maestro_Salud['Fecha_OP_y'],Maestro_Salud['Fecha_OP_x'])

Maestro_Salud['Numero_OP_y'] = Maestro_Salud['Numero_OP_y'].fillna('sin numero_op')
Maestro_Salud['Pagos_Salud.Numero_OP'] = np.where(Maestro_Salud['Numero_OP_y'] != 'sin numero_op',Maestro_Salud['Numero_OP_y'],Maestro_Salud['Numero_OP_x'])

Maestro_Salud['Valor_OP_y'] = Maestro_Salud['Valor_OP_y'].fillna('sin valor_op')
Maestro_Salud['Pagos_Salud.Valor_OP'] = np.where(Maestro_Salud['Valor_OP_y'] != 'sin valor_op',Maestro_Salud['Valor_OP_y'],Maestro_Salud['Valor_OP_x'])

Maestro_Salud['Valor_Pagado_Egresos_y'] = Maestro_Salud['Valor_Pagado_Egresos_y'].fillna('sin Valor_Pagado_Egresos_op')
Maestro_Salud['Pagos_Salud.Valor_Pagado_Egresos'] = np.where(Maestro_Salud['Valor_Pagado_Egresos_y'] != 'sin Valor_Pagado_Egresos_op',Maestro_Salud['Valor_Pagado_Egresos_y'],Maestro_Salud['Valor_Pagado_Egresos_x'])

Maestro_Salud['Estado_Pago_y'] = Maestro_Salud['Estado_Pago_y'].fillna('sin Estado_Pago')
Maestro_Salud['Pagos_Salud.Estado_Pago'] = np.where(Maestro_Salud['Estado_Pago_y'] != 'sin Estado_Pago',Maestro_Salud['Estado_Pago_y'],Maestro_Salud['Estado_Pago_x'])

Maestro_Salud['Codigo_Forma_Pago_y'] = Maestro_Salud['Codigo_Forma_Pago_y'].fillna('sin Codigo_Forma_Pago')
Maestro_Salud['Pagos_Salud.Codigo_Forma_Pago'] = np.where(Maestro_Salud['Codigo_Forma_Pago_y'] != 'sin Codigo_Forma_Pago',Maestro_Salud['Codigo_Forma_Pago_y'],Maestro_Salud['Codigo_Forma_Pago_x'])

Maestro_Salud['Estado_Egresos_y'] = Maestro_Salud['Estado_Egresos_y'].fillna('sin Estado_Egresos')
Maestro_Salud['Pagos_Salud.Estado_Egresos'] = np.where(Maestro_Salud['Estado_Egresos_y'] != 'sin Estado_Egresos',Maestro_Salud['Estado_Egresos_y'],Maestro_Salud['Estado_Egresos_x'])

Maestro_Salud['IVA_y'] = Maestro_Salud['IVA_y'].fillna('sin IVA')
Maestro_Salud['Pagos_Salud.IVA'] = np.where(Maestro_Salud['IVA_y'] != 'sin IVA',Maestro_Salud['IVA_y'],Maestro_Salud['IVA_x'])

Maestro_Salud['RTE_FUE_y'] = Maestro_Salud['RTE_FUE_y'].fillna('sin RTE_FUE')
Maestro_Salud['Pagos_Salud.Rete_Fuente'] = np.where(Maestro_Salud['RTE_FUE_y'] != 'sin RTE_FUE',Maestro_Salud['RTE_FUE_y'],Maestro_Salud['RTE_FUE_x'])

Maestro_Salud['RTE_ICA_y'] = Maestro_Salud['RTE_ICA_y'].fillna('sin RTE_ICA')
Maestro_Salud['Pagos_Salud.Rete_ICA'] = np.where(Maestro_Salud['RTE_ICA_y'] != 'sin RTE_ICA',Maestro_Salud['RTE_ICA_y'],Maestro_Salud['RTE_ICA_x'])

Maestro_Salud['RTE_IVA_y'] = Maestro_Salud['RTE_IVA_y'].fillna('sin RTE_IVA')
Maestro_Salud['Pagos_Salud.Rete_IVA'] = np.where(Maestro_Salud['RTE_IVA_y'] != 'sin RTE_IVA',Maestro_Salud['RTE_IVA_y'],Maestro_Salud['RTE_IVA_x'])
#%%
Maestro_Salud['Fac_Norint'] = Maestro_Salud['Fac_Norint'].fillna(0)
Maestro_Salud['Fac_Norint'] = Maestro_Salud['Fac_Norint'].astype(int)
Glosas.Numero_Interno = Glosas.Numero_Interno.astype(int)
Glosas.drop_duplicates('Numero_Interno', inplace = True)

Glosas

Maestro_Salud = Maestro_Salud.merge(Glosas, how = 'left', left_on = 'Fac_Norint', right_on = 'Numero_Interno')
#%%
Maestro_Salud['Marcacion_F_Glosas'] = np.where(Maestro_Salud['Numero_Interno'].isnull() == False, 'Si', 'No')
#%%

Maestro_Salud.Fecha_Auditoria = pd.to_datetime(Maestro_Salud.Fecha_Auditoria, format = '%Y-%m-%d')
Maestro_Salud.Fecha_Auditoria = Maestro_Salud.Fecha_Auditoria.dt.strftime('%d/%m/%Y')
#%%

Maestro_Salud.Fecha_Envio = pd.to_datetime(Maestro_Salud.Fecha_Envio, format = '%Y-%m-%d')
Maestro_Salud.Fecha_Envio = Maestro_Salud.Fecha_Envio.dt.strftime('%d/%m/%Y')
#%%
lista = ['Numero_Factura_Original','NIT','Razon_Social','Fecha_Radicacion','Valor_Bruto','Valor_Neto','Valor_Glosa',
         'Pagos_Salud.Numero_OP','Pagos_Salud.Fecha_OP','Pagos_Salud.Valor_Pagado_Egresos','Pagos_Salud.Valor_OP','Pagos_Salud.IVA','Pagos_Salud.Rete_Fuente','Pagos_Salud.Rete_ICA','Pagos_Salud.Rete_IVA','Pagos_Salud.Estado_Pago',
         'FECHA INGRESO_BH','Regimen','Numero_Factura_Virtual','Folio','Estado_RR','Auditor_conciliación','Cod_Barra',
         'Estado_actual','Fecha_Ultimo_estado','Fac_Norint','Valor_Descuento','Pagos_Salud.Codigo_Forma_Pago','Fecha_Auditoria',
         'Fecha_Envio','Nro_Guia','Codigo_Glosa','Observacion','Origen','Cod_Barra_Pago_Parcial','Centro_Costo_Medico',
         'Tipo_Cuenta_Medica','Tipo_Auditoría','Numero_Interno','Departamento','Municipio','Zona','Usuario_Inicia_Radicacion',
         'Usuario_Finaliza_Radicacion','Valor_Copago','Valor_Cuota_Moderadora','Valor_Descuento','Valor_Iva',
         'Marcacion_F_Glosas','Marcacion_RR','Marcacion_Egresos_Imp']

Maestro_Salud = Maestro_Salud[lista]
Maestro_Salud['Fecha_Radicacion'] = pd.to_datetime(Maestro_Salud['Fecha_Radicacion'], format = '%d/%m/%Y')
Maestro_Salud['Año_Radicacion'] = Maestro_Salud['Fecha_Radicacion'].dt.year
Maestro_Salud['Fecha_Radicacion'] = Maestro_Salud['Fecha_Radicacion'].dt.strftime('%d/%m/%Y')
#%%
#dir_db = path_salida1 + '\SISCO_Salud.xlsx'
#writer= pd.ExcelWriter(dir_db)

for i in range(min(Maestro_Salud['Año_Radicacion']), max(Maestro_Salud['Año_Radicacion']) + 1):  
    print(i)
    a = Maestro_Salud[Maestro_Salud['Año_Radicacion'] == i]
    a.to_csv(path_salida1 + '\SISCO_Salud ' + str(i) +'.csv', index = False, sep = '|')

#writer.save()
#writer.close()

#Maestro_Salud_Prueba.to_excel(path_temporal + '\Maestro_Salud_Prueba.xlsx', index = False)

#%%

Maestro_ARL_Sin_RPT = Maestro_ARL[~Maestro_ARL['Cod_Barra_Pago_Parcial'].isin(RPT_RADICADOS_ARL['CODIGO_BARRAS'])]
Maestro_ARL_Sin_RPT = Maestro_ARL_Sin_RPT[~Maestro_ARL_Sin_RPT['Cod_Barra'].isin(RPT_RADICADOS_ARL['CODIGO_BARRAS'])]

#Maestro_ARL_Sin_RPT.to_csv(path_salida2 + '\Arl_No_registra_RPT.csv', index = False, sep = '|')

#%%
RPT_RADICADOS_ARL2 = RPT_RADICADOS_ARL[['CODIGO_BARRAS','RADICADO']]
RPT_RADICADOS_ARL2 = RPT_RADICADOS_ARL2.drop_duplicates('RADICADO', keep = 'last')
Maestro_ARL2 = Maestro_ARL.merge(RPT_RADICADOS_ARL2, how = 'left', left_on = 'Cod_Barra_Pago_Parcial', right_on = 'CODIGO_BARRAS')
#%%
Maestro_ARL2 = Maestro_ARL2.merge(RPT_RADICADOS_ARL2, how = 'left', left_on = 'Cod_Barra', right_on = 'CODIGO_BARRAS')
#%%
Maestro_ARL2['RADICADO_y'] = Maestro_ARL2['RADICADO_y'].fillna('Sin Radicado')
Maestro_ARL2['Radicado'] = np.where(Maestro_ARL2['RADICADO_y'] != 'Sin Radicado', Maestro_ARL2['RADICADO_y'], Maestro_ARL2['RADICADO_x'])

Maestro_ARL2['CODIGO_BARRAS_y'] = Maestro_ARL2['CODIGO_BARRAS_y'].fillna('Sin Codigo')
Maestro_ARL2['CODIGO_BARRAS_x'] = Maestro_ARL2['CODIGO_BARRAS_x'].fillna('Sin Codigo')

Maestro_ARL2['Marcacion_RPT'] = np.where((Maestro_ARL2['CODIGO_BARRAS_y'] != 'Sin Codigo') | (Maestro_ARL2['CODIGO_BARRAS_x'] != 'Sin Codigo'), 'Si', 'No')

#%%
Asistenciales2 = Asistenciales.drop_duplicates('RADICADO', keep = 'last')
Maestro_ARL3 = Maestro_ARL2.merge(Asistenciales2, how = 'left', left_on = 'Radicado', right_on = 'RADICADO')

Maestro_ARL3['RADICADO'] = Maestro_ARL3['RADICADO'].fillna('Sin Radicado')
Maestro_ARL3['Marcacion_Asistenciales'] = np.where(Maestro_ARL3['RADICADO'] != 'Sin Radicado', 'Si', 'No')

Maestro_ARL_Sin_Asistenciales = Maestro_ARL3[Maestro_ARL3['Marcacion_Asistenciales'] == 'No']
#Maestro_ARL_Sin_Asistenciales.to_csv(path_salida2 + '\Arl_No_registra_Asistenciales.csv', index = False, sep = '|')

#%%

Pagos_ARL = Pagos[Pagos['CODFUE'] == 8]
#Maestro_ARL3['EGRESOSDC'] = Maestro_ARL3['EGRESOSDC'].fillna(0).astype(int).astype(str)
#Maestro_ARL3['EGRESOSDC'] = Maestro_ARL3['EGRESOSDC'].str.strip('-')
Maestro_ARL4 = Maestro_ARL3[Maestro_ARL3['EGRESOSDC'].isin(Pagos_ARL['NUREFE'])]



#%%
a = a.sample(1048576)
a.to_excel(path_temporal + '\Radicado.xlsx', index = False)

Pagos_ARL1 = Pagos_ARL.sample(1048576)
Pagos_ARL1.to_excel(path_temporal + '\Pagos_Arl.xlsx')
print("Tiempo del Proceso: " , datetime.now()-Tiempo_Total)



