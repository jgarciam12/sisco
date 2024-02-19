 # -*- coding: utf-8 -*-
"""
Created on Mon Aug 22 15:51:40 2022

@author: jcgarciam


El siguiente codigo fue diseniado con el fin de pasar de access a Python un proceso que se llama SISCO el 
cual tiene el proposito de determinar en que estado se encuntran las Facturas del Maestro y Pagos Parciales
y si han sido pagas o no.
"""
import pandas as pd
from datetime import datetime
import Extraccion as Ex
import Transformaciones as Tr
import SISCO_Salud as TrS
import SISCO_ARL as TrA
import Validaciones
import Devoluciones
import pyttsx3

#%%
Tiempo_Total = datetime.now()

Current_Date = datetime.today().date()

print('La fecha del archivo de extracción es: ', Current_Date)
Current_Date = Current_Date.strftime('%Y')+Current_Date.strftime('%m')+ Current_Date.strftime('%d')
    
# Ruta donde se encuentra el Maestro de facruras y los Pagos parciales de las facturas. Siempre que se ejecuta
# el codigo lee los archivos del dia presente
path_entrada1 = r'\\dc1pcadfrs1\Reportes_Activa\axa'
# Ruta donde se guardan los archivos de ASISTENCIALES y PAGOS que se descargan de AS400
path_entrada2 = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\19-Soat-Salud-Arl\4-TRANSVERSAL\SISCO\SISCO\General\Entradas\AS400'
# Ruta donde se guardan los archivos que se descargan los informes de glosas que se descargan de ACTIVA
path_entrada3 = r'\\dc1pcadfrs1\Reportes_Activa'
# Ruta donde se guarda el archivo de Radicaciones Rapidas de Salud que se descarga de BH
path_entrada4 = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\19-Soat-Salud-Arl\4-TRANSVERSAL\SISCO\SISCO\General\Entradas\BH'
# Ruta donde se guardan los archivos de Radicacion Rapida de ARL que llega siempre por correo en un archivo zip
path_entrada5 = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\19-Soat-Salud-Arl\4-TRANSVERSAL\SISCO\SISCO\General\Entradas\goanywhere'
# Ruta donde se guarda el archivo de gestion documental
path_entrada6 = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\19-Soat-Salud-Arl\4-TRANSVERSAL\SISCO\SISCO\General\Entradas\Gestion_Documental'


# Ruta donde se guardan los archivos de SISCO de salida que se dividen en Salud
path_salida1 = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\19-Soat-Salud-Arl\4-TRANSVERSAL\SISCO\SISCO\General\Salidas\SALUD'
# Ruta donde se guardan los archivos de SISCO de salida que se dividen en ARL
path_salida2 = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\19-Soat-Salud-Arl\4-TRANSVERSAL\SISCO\SISCO\General\Salidas\ARL'


#%%
print('\n Iniciando extracción de los archivos de Maestro')
Maestro = Ex.ExtraccionMaestro(path_entrada1, Current_Date)
print('Archivos de Maestro extraídos')

#%%
# Se valida la data del Maestro comparandola con el anterior que se uso para ejecutar SISCO
Tiempo_Validacion = datetime.now()

with open(r'\\DC1PVFNAS1\Autos\BusinessIntelligence\19-Soat-Salud-Arl\4-TRANSVERSAL\SISCO\SISCO\General\Entradas/ultima_fecha.txt', 'r') as ultima_fecha:
    last_date = ultima_fecha.read()
print('La última fecha del Maestro con la que se ejecuto SISCO fue: ', datetime.strptime(last_date, '%Y%m%d').date())

Maestro = Validaciones.ValidacionMaestro(Maestro, last_date, path_entrada1)
print('El tiempo de validación del Maestro tardó: ', datetime.now() - Tiempo_Validacion)

#%%

print('\n Iniciando extracción de los archivos de Pagos Parciales')
Pagos_Parciales = Ex.ExtraccionPagosParciales(path_entrada1, Current_Date)
print('Archivos de Pagos Parciales extraídos')
print('Se cargan ','{:,.0f}'.format(Pagos_Parciales['Cod_Barra_Pago_Parcial'].nunique()), ' facturas únicas')


# Se valida la data del Pagos_Parciales comparandola con el anterior que se uso para ejecutar SISCO
Tiempo_Validacion = datetime.now()

Pagos_Parciales = Validaciones.ValidacionPagosParciales(Pagos_Parciales, last_date, path_entrada1)
print('El tiempo de validación del Maestro tardó: ', datetime.now() - Tiempo_Validacion)
#%%
print('\n Iniciando unión de Pagos Parciales con Maestro')
Maestro_Consolidado = Tr.MaestroConsolidado(Pagos_Parciales, Maestro)
print('Pagos Parciales con Maestro consolidado en una sola tabla')
print('Se consolidan ','{:,.0f}'.format(Maestro_Consolidado['Cod_Barra'].nunique()), ' facturas únicas')


print('\n La fecha máxima de radicación es: ',str(pd.to_datetime(Maestro_Consolidado['Fecha_Radicacion'], format = '%d/%m/%Y', dayfirst = True).max().date()) )
print(' La fecha mínima de radicación es: ',str(pd.to_datetime(Maestro_Consolidado['Fecha_Radicacion'], format = '%d/%m/%Y', dayfirst = True).min().date()) )

del Pagos_Parciales

#%%
print('\n Iniciando extracción de los archivos de Pagos')
Pagos = Ex.ExtraccionPagos(path_entrada2)
Pagos = Tr.PagosFinal(Pagos)
print('Archivos de Pagos extraídos')

print('\n La fecha máxima de Pagos es: ',str(pd.to_datetime(Pagos['Fecha_OP'], format = '%d/%m/%Y', dayfirst = True).max().date()) )
print(' La fecha mínima de Pagos es: ',str(pd.to_datetime(Pagos['Fecha_OP'], format = '%d/%m/%Y', dayfirst = True).min().date()) )

#%%
print('\n Iniciando extracción de los archivos de Glosas')
Glosas = Ex.ExtraccionGlosas(path_entrada3, Current_Date)
print('\n Archivos de Glosas extraídos')

print('\n Iniciando extracción del archivo de Gestion Documental') 
devoluciones_entrada = Ex.devoluciones_entrada(path_entrada6)
print('Archivo de Gestion Documental extraído')

print('\n Iniciando extracción de los archivos de Conciliaciones')
Conciliaciones = Ex.ExtraccionConciliaciones(path_entrada3, Current_Date)
print('\n Archivos de Conciliaciones extraídos')

#%%

print('Iniciando crecación de SISCO Salud')
Maestro_Salud = TrS.CreacionSISCO_Salud(Maestro_Consolidado,Pagos, Glosas, Conciliaciones, path_entrada4)
print('SISCO Salud creado')

print('Extrayendo archivo para la reserva Salud')
Salud_No_registra_egresos = TrS.SaludNoCruzados(Maestro_Salud)
print('Archivo para la reserva generado')

#%%

print('Iniciando a guardar SISCO Salud por años según la fecha de radicación')
years = pd.to_datetime(Maestro_Salud['Fecha_Radicacion'], format = '%d/%m/%Y').dt.year.unique()
        
for i in range(min(years), max(years) + 1):  
    print('Guardando SISCO Salud año ' + str(i))
    a = Maestro_Salud[Maestro_Salud['Fecha_Radicacion'].str.contains(str(i))]
    a.to_csv(path_salida1 + '\CONSOLIDADO_SISCO\SISCO_Salud ' + str(i) +'.csv', index = False, sep = '|', decimal = ',')
    print('SISCO Salud año ' + str(i) + ' guardado \n')
    
print('SISCO Salud completo guardado')

engine = pyttsx3.init()
engine.say('SISCO Salud a finalizado ')
engine.runAndWait()

#%%

print('\n Iniciando a guardar archivo para la Reserva Salud')
Salud_No_registra_egresos.to_excel(path_salida1 + '\MARCACIONES\Salud_No_registra_egresos.xlsx', index = False)
print('Archivo para la Reserva Salud guardado\n')

del Salud_No_registra_egresos
#%%

print('Iniciando crecación de SISCO ARL')
Maestro_ARL = TrA.CreacionSISCO_ARL(Maestro_Consolidado, Pagos, Glosas, Conciliaciones, path_entrada2, path_entrada5)
print('SISCO ARL creado')
        
#print('Extrayendo archivos que no cruzaron')
#ARL_No_Registra_RPT, ARL_No_Registra_Asistenciales, ARL_No_Registra_Egresos = TrA.ARL_No_Cruzados(Maestro_ARL)
#print('Archivos que no cruzaron generados')

print('Iniciando a guardar SISCO ARL por años según la fecha de radicación')
years = pd.to_datetime(Maestro_ARL['Fecha_Radicacion'], format = '%d/%m/%Y').dt.year.unique()

for i in range(2022, max(years) + 1):  
    print('Guardando SISCO ARL año ' + str(i))
    a = Maestro_ARL[Maestro_ARL['Fecha_Radicacion'].str.contains(str(i))]
    a.to_csv(path_salida2 + '\CONSOLIDADO_SISCO\SISCO_ARL ' + str(i) +'.csv', index = False, sep = '|', decimal = ',')
    print('SISCO ARL año ' + str(i) + ' guardado \n')
    
print('SISCO ARL completo guardado \n')

engine = pyttsx3.init()
engine.say('SISCO a r l a finalizado ')
engine.runAndWait()

#%%

del Maestro_Consolidado, Pagos

print('Generando el consolidado de devoluciones para ARL y Salud')
devoluciones_arl, devoluciones_salud = Devoluciones.Devoluciones(devoluciones_entrada, Maestro, Glosas)
print('Archivo de devoluciones generado \n')

del devoluciones_entrada
#%%

print('Guardando Historico de Devoluciones Salud')
devoluciones_salud.to_excel(path_salida1 + '\MARCACIONES/Historico Devoluciones Salud.xlsx', index = False)
print('Historico de Devoluciones Salud guardado \n')

print('Guardando Historico de Devoluciones ARL')
devoluciones_arl.to_excel(path_salida2 + '/Historico Devoluciones ARL.xlsx', index = False)
print('Historico de Devoluciones ARL guardado \n')

del devoluciones_arl


print('Generando el reporte mensual de Devoluciones Vs Radicados')
DevVsRad, FacturasXPrestador, FacturasXCedula, FacturasXNIT, MotivoXCedula, MotivoXNIT = Devoluciones.RadicadoVsDevolucionesSalud(devoluciones_salud, Maestro)
print('Reporte mensual de Devoluciones Vs Radicados generado \n')

del devoluciones_salud, Maestro



print('Guardando Reporte mensual de Devoluciones Vs Radicados')
writer = pd.ExcelWriter(path_salida1 + '\MARCACIONES/Informe Devoluciones Mensual.xlsx')

DevVsRad.to_excel(writer, index = False, sheet_name = 'Detalle')
FacturasXPrestador.to_excel(writer, sheet_name = 'Facturas x Prestador')
FacturasXCedula.to_excel(writer, sheet_name = 'CC')
MotivoXCedula.to_excel(writer, sheet_name = 'Motivo CC')
FacturasXNIT.to_excel(writer, sheet_name = 'NIT')
MotivoXNIT.to_excel(writer, sheet_name = 'Motivo NIT')

writer.save()
writer.close()
print('Reporte mensual de Devoluciones Vs Radicados guardado \n')

del DevVsRad, FacturasXPrestador, FacturasXCedula, MotivoXCedula, FacturasXNIT, MotivoXNIT

"""
print('Generando el archivo de Cartera Salud')
cartera_salud = TrS.Reporte_Cartera(Maestro_Salud, Glosas)
print('Archivo de Cartera Salud generado \n')

print('Guardando el archivo de Cartera Salud')
cartera_salud.to_excel(path_salida1 + '\MARCACIONES/Resumen_Cartera.xlsx', index = False)
print('Archivo de Cartera Salud guardado \n')

del cartera_salud
"""


print('Generando archivo para el tablero Glosas Salud')
Archivo_Salud = TrS.Tablero_Glosas(Maestro_Salud)
print('Archivo para el tablero Glosas Salud Generado\n')

print('Guardando el archivo para el tablero Glosas Salud')
Archivo_Salud.to_csv(path_salida1 + '/Maestro_Salud.csv', index = False, sep = '|')
print('Archivo para el tablero Glosas Salud guardado \n')

del Archivo_Salud, Maestro_Salud



print('Generando archivo para el tablero Glosas Salud')
Archivo_ARL = TrA.Tablero_Glosas(Maestro_ARL)
print('Archivo para el tablero Glosas Salud Generado\n')

print('Guardando el archivo para el tablero Glosas ARL')
Archivo_ARL.to_csv(path_salida2 + '/Maestro_ARL.csv', index = False, sep = '|', encoding = 'ANSI')
print('Archivo para el tablero Glosas ARL guardado \n')

del Archivo_ARL, Maestro_ARL


with open(r'\\DC1PVFNAS1\Autos\BusinessIntelligence\19-Soat-Salud-Arl\4-TRANSVERSAL\SISCO\SISCO\General\Entradas/ultima_fecha.txt', 'w') as ultima_fecha:
    ultima_fecha.write(Current_Date)
print('Variable fecha guardada: ',Current_Date)

engine = pyttsx3.init()
engine.say('SISCO a finalizado ')
engine.runAndWait()
a = datetime.now()-Tiempo_Total
engine.say('El proceso tardó' + str(int((a.seconds/60 - a.seconds/60%60)/60)) + ' horas y ' + str(round(a.seconds/60%60)) + ' minutos')
engine.runAndWait()

print('Proceso finalizado')
print("Tiempo del Proceso: " , datetime.now()-Tiempo_Total)







        
    
    