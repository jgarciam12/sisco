# -*- coding: utf-8 -*-
"""
Created on Wed Jan 17 08:59:17 2024

@author: jcgarciam
"""

import pandas as pd
import glob
from datetime import datetime
import tkinter as tk
from tkinter import ttk

Tiempo_Total = datetime.now()

# las variables path_entrada1 y path_entrada2 contienen las rutas donde se encuentra la informacion de SISCO
path_entrada1 = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\19-Soat-Salud-Arl\4-TRANSVERSAL\SISCO\SISCO\General\Salidas\SALUD\CONSOLIDADO_SISCO'
path_entrada2 = r'\\DC1PVFNAS1\Autos\BusinessIntelligence\19-Soat-Salud-Arl\4-TRANSVERSAL\SISCO\SISCO\General\Salidas\ARL\CONSOLIDADO_SISCO'

path_salida = r'D:\DATOS\Users\jcgarciam\OneDrive - AXA Colpatria Seguros\Documentos\Informes\SISCO\Output'

#%%
dic = {}

columnas = ['Numero_Factura_Original','NIT','Razon_Social','Fecha_Radicacion','Valor_Bruto',
            'Valor_Neto','Valor_Glosa','Regimen','Estado_actual','Fecha_Ultimo_estado',
            'Fac_Norint','Numero_conciliacion','Cod_Barra','Numero_OP','Valor_Pagado_Egresos',
            'Rete_ICA','Rete_Fuente','Rete_IVA','Fecha_OP','Cod_Barra_Pago_Parcial']

formatos = {'NIT':str,'Numero_OP':str,'Cod_Barra':str,'Fac_Norint':str,
            'Cod_Barra_Pago_Parcial':str,'Radicado':str}


print('Inicio de carga de archivos SISCO Salud \n')
for Archivo in glob.glob(path_entrada1 + '\*csv'):
    print('Cargando archivo ', Archivo[len(path_entrada1) + 1:-4])
    df = pd.read_csv(Archivo, sep = '|', header = 0, dtype = formatos, usecols = columnas)
    print('Archivo ', Archivo[len(path_entrada1) + 1:-4], ' cargado \n')
    dic[Archivo] = df

columnas = ['Numero_Factura_Original','NIT','Razon_Social','Fecha_Radicacion','Valor_Bruto',
            'Valor_Neto','Valor_Glosa','Regimen','Estado_actual','Fecha_Ultimo_estado',
            'Fac_Norint','Numero_conciliacion','Cod_Barra','Numero_OP','Valor_Pagado_Egresos',
            'Rete_ICA','Rete_Fuente','Rete_IVA','Fecha_OP','Cod_Barra_Pago_Parcial','Radicado']    

for Archivo in glob.glob(path_entrada2 + '\*csv'):
    print('Cargando archivo ', Archivo[len(path_entrada2) + 1:-4])
    df = pd.read_csv(Archivo, sep = '|', header = 0, dtype = formatos, usecols = columnas)
    print('Archivo ', Archivo[len(path_entrada2) + 1:-4], ' cargado \n')
    dic[Archivo] = df

print('El tiempo de carga de las bases de datos tardo: ', datetime.now() - Tiempo_Total)

#%% 
sisco = pd.concat(dic).reset_index(drop = True)

sisco['Llave'] = sisco['Numero_OP'].astype(str) + sisco['Cod_Barra'].astype(str) + sisco['Cod_Barra_Pago_Parcial'].astype(str) + sisco['Radicado'].astype(str)
sisco = sisco.drop_duplicates('Llave')
del dic

valores = ['Valor_Pagado_Egresos','Rete_ICA','Rete_Fuente','Rete_IVA']

for i in valores:
    sisco[i] = sisco[i].astype(str).str.replace(',','.').astype(float)
    
lista_regimenes = list(sisco['Regimen'].unique())

sisco['Fecha_Radicacion'] = pd.to_datetime(sisco['Fecha_Radicacion'], format = '%d/%m/%Y', dayfirst = True)

lista_anios_radicacion = list(sisco['Fecha_Radicacion'].astype(str).unique())
lista_anios_radicacion.sort()

sisco['Valor_Glosa_Inicial'] = sisco['Valor_Glosa'].copy()
sisco['Valor_Glosa_Actual'] = sisco['Valor_Glosa'].copy()

sisco['Fecha_Radicacion_Inicial'] = sisco['Fecha_Radicacion'].copy()
sisco['Fecha_Radicacion_Final'] = sisco['Fecha_Radicacion'].copy()

sisco['Numero_conciliacion'] = sisco['Numero_conciliacion'].fillna(0.0).astype(str).str.replace(',0','.0').astype(float).astype(int)

sisco = sisco.sort_values('Fecha_Radicacion', ascending = False)

lista_numeros_conciliaciones = list(sisco['Numero_conciliacion'].unique())
lista_numeros_conciliaciones.sort()
#%%
datos = {'Numero_Factura_Original':'first','NIT':'first','Razon_Social':'first',
         'Fecha_Radicacion_Inicial':'last','Fecha_Radicacion_Final':'first',
         'Valor_Neto':'first','Valor_Pagado_Egresos':sum,'Rete_ICA':sum,'Rete_IVA':sum,
         'Rete_Fuente':sum,'Valor_Glosa_Inicial':'last','Valor_Glosa_Actual':'first',
         'Regimen':'first','Estado_actual':'first','Fecha_Ultimo_estado':'first',
         'Fac_Norint':'first','Numero_conciliacion':'first'}

sisco_agrupado = sisco.groupby('Cod_Barra', as_index = False).agg(datos)

#%%

class ConsultaSISCO:
    def __init__(self, root, dataframe1, dataframe2):
        self.root = root
        self.root.title('Consulta SISCO')
        
        self.dataframe1 = dataframe1
        self.dataframe2 = dataframe2
        
        self.nit = tk.StringVar()
        self.regimen = tk.StringVar()
        self.anio_radicacion = tk.StringVar()
        self.anio_radicacion2 = tk.StringVar()
        self.numero_factura = tk.StringVar()
        self.numero_conciliaciones = tk.StringVar()
        
        tk.Label(root, text = 'NIT:').grid(row = 0, column = 0, padx = 5, pady = 5)
        tk.Entry(root, textvariable = self.nit).grid(row = 0, column = 1, padx = 5, pady = 5)
        
        tk.Label(root, text = 'Regimen:').grid(row = 1, column = 0, padx = 5, pady = 5)
        self.regimen_combobox = ttk.Combobox(root, textvariable = self.regimen, values = lista_regimenes, width = 43)
        self.regimen_combobox.grid(row = 1, column = 1, padx = 5, pady = 5)
        
        tk.Label(root, text = 'Fecha de Radicación Inicial:').grid(row = 2, column = 0, padx = 5, pady = 5)
        self.regimen_combobox = tk.ttk.Combobox(root, textvariable = self.anio_radicacion, values = lista_anios_radicacion)
        self.regimen_combobox.grid(row = 2, column = 1, padx = 5, pady = 5)
        
        tk.Label(root, text = 'Fecha de Radicación Final:').grid(row = 3, column = 0, padx = 5, pady = 5)
        self.regimen_combobox = tk.ttk.Combobox(root, textvariable = self.anio_radicacion2, values = lista_anios_radicacion)
        self.regimen_combobox.grid(row = 3, column = 1, padx = 5, pady = 5)
        
        tk.Label(root, text = 'Numero Factura:').grid(row = 4, column = 0, padx = 5, pady = 5)
        tk.Entry(root, textvariable = self.numero_factura).grid(row = 4, column = 1, padx = 5, pady = 5)
        
        tk.Label(root, text = 'Número de conciliaciones (mayor o igual):').grid(row = 5, column = 0, padx = 5, pady = 5)
        self.regimen_combobox = tk.ttk.Combobox(root, textvariable = self.numero_conciliaciones, values = lista_numeros_conciliaciones)
        self.regimen_combobox.grid(row = 5, column = 1, padx = 5, pady = 5)
        
        tk.Button(root, text = 'Realizar Consulta', command = self.realizar_consulta).grid(row = 6, column = 0, columnspan = 2, pady = 10)
        
        self.resultados = tk.Text(root, height = 10, width = 80)
        self.resultados.grid(row = 7, column = 0, columnspan = 2, padx = 5, pady = 5)
        
    def realizar_consulta(self):
        
        nit = self.nit.get()
        regimen = self.regimen.get()
        anio_radicacion = self.anio_radicacion.get()
        anio_radicacion2 = self.anio_radicacion2.get()
        numero_factura = self.numero_factura.get()
        numero_conciliaciones = self.numero_conciliaciones.get()
        
        self.resultado = self.dataframe1.copy()
        self.resultado2 = self.dataframe2.copy()
        
        a = 'Consulta'
        
        if nit:            
            #self.resultado = self.resultado[self.resultado['NIT'] == nit]
            self.resultado2 = self.resultado2[self.resultado2['NIT'] == nit]

            a = a + '_' + str(nit)
        if regimen:
            #self.resultado = self.resultado[self.resultado['Regimen'] == regimen]
            self.resultado2 = self.resultado2[self.resultado2['Regimen'] == regimen]

            a = a + '_' + regimen
        
        if anio_radicacion:
            #self.resultado = self.resultado[self.resultado['Fecha_Radicacion_Inicial'].between(anio_radicacion, anio_radicacion2) == True]
            self.resultado2 = self.resultado2[self.resultado2['Fecha_Radicacion'].between(anio_radicacion, anio_radicacion2) == True]
            a = a + '_' + str(anio_radicacion)
            a = a + '_' + str(anio_radicacion2)
        if numero_factura:
            #self.resultado = self.resultado[self.resultado['Numero_Factura_Original'] == str(numero_factura)]
            self.resultado2 = self.resultado2[self.resultado2['Numero_Factura_Original'] == str(numero_factura)]
            a = a + '_' + str(numero_factura)
        if numero_conciliaciones:
            #self.resultado = self.resultado[self.resultado['Numero_conciliacion'].astype(float) >= float(numero_conciliaciones)]
            self.resultado2 = self.resultado2[self.resultado2['Numero_conciliacion'].astype(float) >= float(numero_conciliaciones)]
            a = a + '_' + str(numero_conciliaciones)
        
        self.resultado = self.resultado[self.resultado['Cod_Barra'].isin(self.resultado2['Cod_Barra']) == True]

        self.resultados.delete(1.0, tk.END)
        self.resultados.insert(tk.END, self.resultado.to_string(index = False))
        writer = pd.ExcelWriter(path_salida + '/' + a + '.xlsx')
        self.resultado.to_excel(writer, index = False, sheet_name = 'datos agrupados')
        self.resultado2.to_excel(writer, index = False, sheet_name = 'datos desagrupados')
        writer.save()
        writer.close()
        tk.messagebox.showinfo('Archivo guardado')
#%%

root = tk.Tk()
app = ConsultaSISCO(root, sisco_agrupado, sisco)
root.mainloop()

