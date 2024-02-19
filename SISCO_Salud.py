# -*- coding: utf-8 -*-
"""
Created on Tue Aug 23 16:23:55 2022

@author: jcgarciam
"""

import Transformaciones as Tr
import Extraccion as Ex
import pandas as pd
import numpy as np

def CambioFormato(df, a = 'a'):
    df[a] = df[a].astype(str)
    df[a] = np.where(df[a].str[-2::] == '.0', df[a].str[0:-2], df[a])
    df[a] = np.where(df[a] == 'nan', np.nan, df[a])
    return df[a] 

def CreacionSISCO_Salud(Maestro_Consolidado, Pagos, Glosas, Conciliaciones, path_entrada4): 
    
    Glosas = Glosas[Glosas['Origen_Glosa'] == 'Salud']
    Glosas = Tr.GlosasFinal(Glosas)
    
    print('\n Extracción y transformación de la base de Radicaciones Rápidas para Salud')
    RR_Cuentas_Medicas = Ex.ExtraccionCuentasMedicas(path_entrada4)
    RR_Cuentas_Medicas = Tr.RR_Cuentas_medicasFinal(RR_Cuentas_Medicas)
    print('Base de Radicaciones Rápidas para Salud extraída y transformada')
    
    Pagos_Salud = Pagos[Pagos['CODCOM'].isin([13,17])]
    
    Pagos_Salud['NUREFE'] = CambioFormato(Pagos_Salud, a = 'NUREFE')
    Pagos_Salud['Numero_OP'] = CambioFormato(Pagos_Salud, a = 'Numero_OP')
    Pagos_Salud['Llave'] = Pagos_Salud['NUREFE'].astype(str) + Pagos_Salud['Numero_OP'].astype(str)
    
    Pagos_Salud = Pagos_Salud.drop_duplicates('Llave', keep = 'last')
    Pagos_Salud = Pagos_Salud.drop(columns = ['Llave'])
    
    Conciliaciones = Conciliaciones[Conciliaciones['Origen'].astype(str).str.upper().str.contains('ARL') == False]
    Conciliaciones = Conciliaciones[Conciliaciones['Codigo_barras'].isnull() == False]
    Conciliaciones['Codigo_barras']  = CambioFormato(Conciliaciones, a = 'Codigo_barras')
    Conciliaciones = Conciliaciones.drop_duplicates('Codigo_barras', keep = 'last')
    Conciliaciones = Conciliaciones.rename(columns = {'Codigo_barras':'Cod_Barra'})
    del(Conciliaciones['Origen'])
    
    Maestro_Salud = Maestro_Consolidado[~Maestro_Consolidado['Regimen'].str.upper().str.contains('ARL')]
    
    Maestro_Salud['Cod_Barra_Pago_Parcial'] = CambioFormato(Maestro_Salud, a = 'Cod_Barra_Pago_Parcial')
    Maestro_Salud['Cod_Barra'] = CambioFormato(Maestro_Salud, a = 'Cod_Barra')
    RR_Cuentas_Medicas['FOLIO'] = CambioFormato(RR_Cuentas_Medicas, a = 'FOLIO')
    
    Maestro_Salud1 = Maestro_Salud[Maestro_Salud['Origen'] == 'Pagos Parciales'].copy()
    Maestro_Salud1 = Maestro_Salud1.merge(RR_Cuentas_Medicas, how = 'left', left_on = 'Cod_Barra_Pago_Parcial', right_on = 'FOLIO')
    Maestro_Salud1 = Maestro_Salud1.merge(Pagos_Salud, how = 'left', left_on = 'Cod_Barra_Pago_Parcial', right_on = 'NUREFE')
    
    Maestro_Salud2 = Maestro_Salud[Maestro_Salud['Origen'] == 'Maestro'].copy()
    Maestro_Salud2 = Maestro_Salud2.merge(RR_Cuentas_Medicas, how = 'left', left_on = 'Cod_Barra', right_on = 'FOLIO')       
    Maestro_Salud2 = Maestro_Salud2.merge(Pagos_Salud, how = 'left', left_on = 'Cod_Barra', right_on = 'NUREFE')
    
    # Hay datos que no van a cruzar con Pagos por que el NUREFE puede empezar con un 0 y en el codigo de barra,
    # ya se parcial o no, no empieza con este cero. Por lo tanto debemos hacer un proceso cuidadoso para
    # limpiar el NUREFE si es numerico
    
    Maestro_Salud = pd.concat([Maestro_Salud1, Maestro_Salud2]).reset_index(drop = True)
    

    Pagos_Salud2 = Pagos_Salud.copy()
    
    Pagos_Salud2['NUREFE'] = np.where(Pagos_Salud2['NUREFE'].str[0] == '0', Pagos_Salud2['NUREFE'].str[1::],Pagos_Salud2['NUREFE'])
     
    Maestro_Salud_a = Maestro_Salud.copy()
    Maestro_Salud_a = Maestro_Salud_a[Maestro_Salud_a['NUREFE'].isnull() == False]
    
    Maestro_Salud_b = Maestro_Salud.copy()
    Maestro_Salud_b = Maestro_Salud_b[Maestro_Salud_b['NUREFE'].isnull() == True]
    Maestro_Salud_b = Maestro_Salud_b.drop(columns = ['NUREFE', 'Fecha_OP', 'Numero_OP', 'Valor_Pagado_Egresos',
                                                      'Valor_OP','IVA', 'Rete_Fuente', 'Rete_ICA', 'Rete_IVA',
                                                      'Estado_Pago','Codigo_Forma_Pago', 'Estado_Egresos','CODCOM',
                                                      'CODFUE','Fecha de causacion','Fecha de registro'])
    Maestro_Salud_b_1 = Maestro_Salud_b.copy()
    Maestro_Salud_b_1 = Maestro_Salud_b_1[Maestro_Salud_b_1['Origen'] == 'Pagos Parciales']
    Maestro_Salud_b_1 = Maestro_Salud_b_1.merge(Pagos_Salud2, how = 'left', left_on = 'Cod_Barra_Pago_Parcial', right_on = 'NUREFE')
    
    Maestro_Salud_b_1_a = Maestro_Salud_b_1.copy()
    Maestro_Salud_b_1_a= Maestro_Salud_b_1_a[Maestro_Salud_b_1_a['NUREFE'].isnull() == False]
    
    Maestro_Salud_b_1_b = Maestro_Salud_b_1.copy()
    Maestro_Salud_b_1_b = Maestro_Salud_b_1_b[Maestro_Salud_b_1_b['NUREFE'].isnull() == True]
    Maestro_Salud_b_1_b = Maestro_Salud_b_1_b.drop(columns = ['NUREFE', 'Fecha_OP', 'Numero_OP', 'Valor_Pagado_Egresos',
                                                      'Valor_OP','IVA', 'Rete_Fuente', 'Rete_ICA', 'Rete_IVA',
                                                      'Estado_Pago','Codigo_Forma_Pago', 'Estado_Egresos','CODCOM',
                                                      'CODFUE'])
    Maestro_Salud_b_1_b = Maestro_Salud_b_1_b.merge(Pagos_Salud2, how = 'left', left_on = 'Cod_Barra', right_on = 'NUREFE')
    Maestro_Salud_b_1_b = Maestro_Salud_b_1_b.drop_duplicates('Cod_Barra_Pago_Parcial')
    
    Maestro_Salud_b_2 = Maestro_Salud_b.copy()
    Maestro_Salud_b_2 = Maestro_Salud_b_2[Maestro_Salud_b_2['Origen'] == 'Maestro']
    Maestro_Salud_b_2 = Maestro_Salud_b_2.merge(Pagos_Salud2, how = 'left', left_on = 'Cod_Barra', right_on = 'NUREFE')
    
    Maestro_Salud = pd.concat([Maestro_Salud_a, Maestro_Salud_b_1_a, Maestro_Salud_b_1_b, Maestro_Salud_b_2]).reset_index(drop = True)
    
    Maestro_Salud['Fac_Norint'] = CambioFormato(Maestro_Salud, a = 'Fac_Norint')
    Glosas['Numero_Interno'] = CambioFormato(Glosas, a = 'Numero_Interno')
    
    Maestro_Salud = Maestro_Salud.merge(Glosas, how = 'left', left_on = 'Fac_Norint', right_on = 'Numero_Interno')
    
    Maestro_Salud = Maestro_Salud.rename(columns = {'FOLIO':'Folio','ESTADO':'Estado','FECHA INGRESO':'Fecha Ingreso'})
    
    Maestro_Salud['Folio'] = Maestro_Salud['Folio'].fillna('Sin FOLIO') 
    
    Maestro_Salud['Marcacion_RR'] = np.where(Maestro_Salud['Folio'] != 'Sin FOLIO', 'Si', 'No')
    
    Maestro_Salud['NUREFE'] = Maestro_Salud['NUREFE'].fillna('Sin NUREFE')
    Maestro_Salud['Marcacion_Egresos_Imp'] = np.where(Maestro_Salud['NUREFE'] != 'Sin NUREFE', 'Si', 'No')
    
    Maestro_Salud['Numero_Interno'] = Maestro_Salud['Numero_Interno'].fillna('Sin Numero_Interno')  
    Maestro_Salud['Marcacion_F_Glosas'] = np.where(Maestro_Salud['Numero_Interno'] != 'Sin Numero_Interno', 'Si', 'No')
     
    lista = ['Numero_Factura_Original','NIT','Razon_Social','Fecha_Radicacion','Valor_Bruto','Valor_Neto','Valor_Glosa',
         'Numero_OP','Fecha_OP','Valor_Pagado_Egresos','Valor_OP','IVA','Rete_Fuente','Rete_ICA','Rete_IVA','Estado_Pago',
         'Fecha Ingreso','Regimen','Numero_Factura_Virtual','Folio','Estado','Auditor_conciliación','Cod_Barra',
         'Estado_actual','Fecha_Ultimo_estado','Fac_Norint','Valor_Descuento','Codigo_Forma_Pago','Fecha_Auditoria',
         'Fecha_Envio','Nro_Guia','Codigo_Glosa','Estado_Egresos','Origen','Cod_Barra_Pago_Parcial',
         'Tipo_Cuenta_Medica','Numero_Interno','Departamento','Municipio','Zona','Usuario_Inicia_Radicacion',
         'Usuario_Finaliza_Radicacion','Valor_Copago','Valor_Cuota_Moderadora','Valor_Iva',
         'Marcacion_F_Glosas','Marcacion_RR','Marcacion_Egresos_Imp','Observacion','Tipo_Doc_Prestador',
         'Fecha_Ingreso','Fecha_Factura','TipoCuentaMedica','Descuento_Pronto_Pago','Nota_crédito','Nombre_Estado_Actual',
         'Fecha_Ultimo_estado_glosa','Valor_Neto_Pago_Parcial','Auditor','Fecha de registro','Fecha de causacion'
         ]    

    Maestro_Salud = Maestro_Salud[lista]
    
    Maestro_Salud = Maestro_Salud.merge(Conciliaciones, how = 'left', on = 'Cod_Barra')
    
    Maestro_Salud = Maestro_Salud.rename(columns = {'Estado':'Estado_RR', 'Fecha Ingreso':'Fecha_Ingreso_BH', 'Folio':'Folio_BH'})
    
    Campos_A_Cambiar = ['Valor_Bruto','Valor_Neto','Valor_Glosa','Valor_Pagado_Egresos','Valor_OP','IVA','Rete_Fuente',
                        'Rete_ICA','Rete_IVA','Valor_Descuento','Valor_Copago','Valor_Cuota_Moderadora',
                        'Valor_Iva']

    
    for i in Campos_A_Cambiar:
        Maestro_Salud[i] = Maestro_Salud[i].astype(float)

    Maestro_Salud['Numero_OP'] = CambioFormato(Maestro_Salud, a = 'Numero_OP')
    
    return Maestro_Salud


def SaludNoCruzados(Maestro_Salud):
    
    """
     El siguiente codigo busca calcular las facturas que se deben tener en cuenta para la Reserva Salud
    """
    
    # Lista de campos que se usan para la Reserva
    Salud_Reserva = Maestro_Salud[['Folio_BH','NIT','Razon_Social','Tipo_Doc_Prestador','Estado_RR',
                                    'Fecha_Radicacion','Fecha_Ingreso','Numero_Factura_Original',
                                    'Valor_Bruto','Valor_Neto','Valor_Copago','Valor_Descuento','Estado_actual',
                                    'Fecha_Ultimo_estado','Fecha_Auditoria','Departamento','Municipio',
                                    'Zona','Fecha_Factura','Valor_Glosa','Cod_Barra','Cod_Barra_Pago_Parcial',
                                    'TipoCuentaMedica','Fac_Norint','Descuento_Pronto_Pago','Regimen',
                                    'Marcacion_Egresos_Imp','Fecha_OP','Valor_Pagado_Egresos','Valor_OP']]
    # No se deben tener en cuenta para la Reserva los Pagos Manuales
    Salud_Reserva = Salud_Reserva[Salud_Reserva['Regimen'] != 'AXA Colpatria - Salud - Pagos Manuales']
    # Se ordenas los datos del mas reciente al mas antigo por fecha de radicacion
    Salud_Reserva['Fecha_Radicacion'] = pd.to_datetime(Salud_Reserva['Fecha_Radicacion'], format = '%d/%m/%Y')
    Salud_Reserva = Salud_Reserva.sort_values('Fecha_Radicacion', ascending = False)
    
    # Se realiza una copia de Salud_Reserva para quitar duplicados del Cod_Barra dejando el primero
    # que como ya se ordeno por fecha se queda el mas reciente
    a = Salud_Reserva.copy()        
    a = a.drop_duplicates('Cod_Barra', keep = 'first') 
    
    # Esta es la lista de estados en que pueden estar las facturas para ser Reservadas
    filtro_estados = ['AUDITADA: Auditada con Glosa, espera p/envio a IPS',
                      'AUDITADA: Factura OK Sin Glosa',
                      'AUDITADA: Pendiente de enviar Resumen de Auditoria Parcial',
                      'AUDITADA: Pendiente de informar Orden de pago al Pagador.',
                      'AUDITADA: Radicada para Pago.',
                      'AUDITADA: Resp.Glosa recibida y No Procesada',
                      'EN AUDITORIA: En espera de ser asignada a Auditor',
                      'EN AUDITORIA: Factura asignada a Medico Auditor.',
                      'EN RADICACION: Revision Calidad de Radicación']
    
    
    # Se dejan solo las facturas que no tengan los estados anteriores, esto con el fin de sacar
    # de la reserva las facturas que en definitiva no se van a reservar por su ultimo estado
    a = a[a['Estado_actual'].isin(filtro_estados) == False]
    a = a[['Cod_Barra']]    
    
    # Se sacan de la reserva las facturas que por su ultimo estado no se van a reservar
    Salud_Reserva = Salud_Reserva[Salud_Reserva['Cod_Barra'].isin(a['Cod_Barra']) == False]
    Salud_Reserva = Salud_Reserva.sort_values('Fecha_Radicacion', ascending = False)
    
    # Existen facturas que ya se pagaron totalmente o tienen una nota crédito entonces ya cruzaron con Pagos y su glosa es cero
    # por lo tanto se deben sacar de l reserva porque ya estan libres de reserva
    b = Salud_Reserva.copy()
    b = b.drop_duplicates('Cod_Barra', keep = 'first')
    b = b[((b['Marcacion_Egresos_Imp'] == 'Si') & (b['Valor_Glosa'].astype(float) == 0.0)) |
          ((b['Marcacion_Egresos_Imp'] == 'Si') & (b['Estado_actual'] == 'AUDITADA: Radicada para Pago.'))]
    
    # Se sacan las facturas ya pagadas en su totalidad
    Salud_Reserva = Salud_Reserva[Salud_Reserva['Cod_Barra'].isin(b['Cod_Barra']) == False]
    
    
    # Existen facturas que ya han tenido pagos parciales pero no estan pagadas en su totatlidad por lo que
    # aun tienen una glosa por Pagar. El valor de esa glosa debe reservarse pero no el el valor neto de la 
    # factura por lo que ya ha tenido pagos. Asi, se toma la ultima glosa pendiente por pagar y esto lo conseguimos
    # con la ultima fecha de pago. Por otro lado, deben salir de la reserva las facturas que tienen OP y
    # su ultimo estado dice Radicada Para Pago porque ya la factura ha sido sellada en su totalidad
    Salud_Reserva_Con_Pago = Salud_Reserva.copy()
    Salud_Reserva_Con_Pago = Salud_Reserva_Con_Pago[Salud_Reserva_Con_Pago['Marcacion_Egresos_Imp'] == 'Si']    
    Salud_Reserva_Con_Pago['Fecha_OP'] = pd.to_datetime(Salud_Reserva_Con_Pago['Fecha_OP'], format = '%d/%m/%Y')
    Salud_Reserva_Con_Pago = Salud_Reserva_Con_Pago.sort_values('Fecha_OP', ascending = False)    
    Salud_Reserva_Con_Pago['Fecha_OP'] = Salud_Reserva_Con_Pago['Fecha_OP'].dt.strftime('%d/%m/%Y')
    Salud_Reserva_Con_Pago = Salud_Reserva_Con_Pago.drop_duplicates('Cod_Barra', keep = 'first')
    Salud_Reserva_Con_Pago['Valor a Reservar'] = Salud_Reserva_Con_Pago['Valor_Glosa'].copy()
    
    # De los Cod_Barra que no han tenido Pagos Parciales ni Pagos Totales se deb reservar el valor Neto de
    # la factura
    Salud_Reserva_Sin_Pago = Salud_Reserva.copy()
    # Tomamos los cod_barra que no tienen Pagos Parciales
    Salud_Reserva_Sin_Pago = Salud_Reserva_Sin_Pago[Salud_Reserva_Sin_Pago['Cod_Barra'].isin(Salud_Reserva_Con_Pago['Cod_Barra']) == False]   
    Salud_Reserva_Sin_Pago = Salud_Reserva_Sin_Pago.sort_values('Fecha_Radicacion', ascending = False)
    Salud_Reserva_Sin_Pago = Salud_Reserva_Sin_Pago.drop_duplicates('Cod_Barra', keep = 'first')
    Salud_Reserva_Sin_Pago['Valor a Reservar'] = Salud_Reserva_Sin_Pago['Valor_Neto'].copy()

    # Ahora concatenamos los Datos que no tienen Pagos con los que tienen Pagos Parciales para obtener la reserva
    Reserva = pd.concat([Salud_Reserva_Sin_Pago,Salud_Reserva_Con_Pago]).reset_index(drop = True)
    Reserva['Fecha_Radicacion'] = Reserva['Fecha_Radicacion'].dt.strftime('%d/%m/%Y')
    Reserva['Valor a Reservar'] = Reserva['Valor a Reservar'].astype(float)
    Reserva = Reserva.rename(columns = {'Numero_Factura_Original':'Numero_Factura',
                                        'Valor_Glosa':'Valor_Ultima_Glosa'})    
    
    # Garantizamos nuevamente que la Reserva tenga los estados correctos
    Reserva = Reserva[Reserva['Estado_actual'].isin(filtro_estados) == True]  
    Reserva = Reserva[Reserva['Valor_Neto'] > 1]
    Reserva = Reserva[Reserva['Estado_RR'].astype(str).str.upper().isin(['ANULADA','DEVUELTA']) == False]
    
    # Se cargan una lista de facturas que no se van a reservar porque ya tienen un ticket
    # ya sea anuladas, devueltas o por pagos manuales
    
    facturas_no_reservar = pd.read_excel(r'\\DC1PVFNAS1\Autos\BusinessIntelligence\19-Soat-Salud-Arl\4-TRANSVERSAL\SISCO\SISCO\General\Entradas/Registros_No_Reservar.xlsx', header = 0, usecols = ['Cod_Barra', 'Cod_Barra_Pago_Parcial'])
    
    facturas_no_reservar['Cod_Barra_Pago_Parcial'] = CambioFormato(facturas_no_reservar, a = 'Cod_Barra_Pago_Parcial')
    facturas_no_reservar['Cod_Barra'] = CambioFormato(facturas_no_reservar, a = 'Cod_Barra')
    
    Cod_Barra_Pago_Parcial = facturas_no_reservar.loc[facturas_no_reservar['Cod_Barra_Pago_Parcial'].isnull() == False,'Cod_Barra_Pago_Parcial'].unique()
    Cod_Barra = facturas_no_reservar.loc[facturas_no_reservar['Cod_Barra'].isnull() == False,'Cod_Barra'].unique()

    
    Reserva = Reserva[Reserva['Cod_Barra'].isin(Cod_Barra) == False]
    Reserva = Reserva[Reserva['Cod_Barra_Pago_Parcial'].isin(Cod_Barra_Pago_Parcial) == False]

    print('Resumen: Reserva Salud \n')    

    a = Reserva.copy()
    a['Cantidad de facturas'] = a['Cod_Barra'].copy()
    a['Fecha_Radicacion'] = pd.to_datetime(a['Fecha_Radicacion'], format = '%d/%m/%Y')
    a['Año de radicación'] = a['Fecha_Radicacion'].dt.year
    a = a.groupby('Año de radicación').agg({'Cantidad de facturas':'count','Valor_Bruto':'sum','Valor a Reservar':'sum'})
    totales = Reserva.copy()
    totales['Cantidad de facturas'] = totales['Cod_Barra'].copy()
    totales = totales.agg({'Cantidad de facturas':'count','Valor a Reservar':'sum'})
    totales.name = 'Total'
    a = a.append(totales)
    a['Valor_Bruto'] = '$' + a['Valor_Bruto'].map('{:,.0f}'.format)
    a['Valor a Reservar'] = '$' + a['Valor a Reservar'].map('{:,.0f}'.format)
    a['Cantidad de facturas'] = a['Cantidad de facturas'].map('{:,.0f}'.format)
    print(a)

    return Reserva 

def Reporte_Cartera(Maestro_Salud, Glosas):
    
    df_salud = Maestro_Salud.copy()

    df_salud['Fecha_Radicacion'] = pd.to_datetime(df_salud['Fecha_Radicacion'], format = '%d/%m/%Y', dayfirst = True)

    df_salud = df_salud.sort_values('Fecha_Radicacion', ascending = False) 

    df_salud['Cod_Barra'] = CambioFormato(df_salud, a = 'Cod_Barra')
    df_salud['Cod_Barra_Pago_Parcial'] = CambioFormato(df_salud, a = 'Cod_Barra_Pago_Parcial')
    df_salud['Fac_Norint'] = CambioFormato(df_salud, a = 'Fac_Norint')

    df_salud['Numero_OP'] = CambioFormato(df_salud, a = 'Numero_OP')
    df_salud['Numero_Interno'] = CambioFormato(df_salud, a = 'Numero_Interno')

    df_salud['Numero de Radicados'] = df_salud['Cod_Barra'].astype(str) + df_salud['Cod_Barra_Pago_Parcial'].astype(str)
    df_salud['Llave'] = df_salud['Cod_Barra'].astype(str) + df_salud['Cod_Barra_Pago_Parcial'].astype(str) + df_salud['Numero_OP'].astype(str)


    def Cantidad(df, a):
        df2 = df.copy()
        df2['Cantidad de '+ a] = df2[a].copy()
        df2 = df2.groupby(a, as_index = False)['Cantidad de ' + a].count()
        df = df.merge(df2, how = 'left', on = a)
        df = df.sort_values(['Cantidad de ' + a, a], ascending = False)
        return df

    df_salud = Cantidad(df_salud, a = 'Numero_OP')
    df_salud['Cantidad de Numero_OP'] = df_salud['Cantidad de Numero_OP'].fillna(0.0)
    df_salud = Cantidad(df_salud, a = 'Llave')

    df_prueba = df_salud.drop_duplicates('Numero de Radicados')
    df_prueba = df_prueba.groupby('Cod_Barra', as_index = False)['Numero de Radicados'].count()

    valores = ['Valor_Neto','Valor_Pagado_Egresos','Valor_Glosa','Nota_crédito','Rete_Fuente','Rete_ICA']

    for i in valores:
        df_salud[i] = df_salud[i].astype(str).str.replace(',','.').astype(float)
        
    df_resumen_a = df_salud.copy()
    df_resumen_a = df_resumen_a[df_resumen_a['Cantidad de Numero_OP'] <= 1]
    df_resumen_a['Pago Multiple'] = 'No'
    
    df_resumen_b = df_salud.copy()
    df_resumen_b = df_resumen_b[df_resumen_b['Cantidad de Numero_OP'] > 1]
    df_resumen_b['Valor_Pagado_Egresos'] = df_resumen_b['Valor_Pagado_Egresos'] / df_resumen_b['Cantidad de Numero_OP']
    df_resumen_b['Rete_Fuente'] = df_resumen_b['Rete_Fuente'] / df_resumen_b['Cantidad de Numero_OP']
    df_resumen_b['Rete_ICA'] = df_resumen_b['Rete_ICA'] / df_resumen_b['Cantidad de Numero_OP']
    df_resumen_b['Pago Multiple'] = 'Si'
    
    df_resumen = pd.concat([df_resumen_a,df_resumen_b])
    df_resumen = df_resumen.sort_values('Fecha_Radicacion', ascending = False)

    df_resumen['Valor Glosa Actual'] = df_resumen['Valor_Glosa'].copy()
    df_resumen = df_resumen.groupby('Cod_Barra', as_index = False).agg({'Numero_Factura_Original':'last','Fac_Norint':'last','Valor_Neto':'last','Valor_Pagado_Egresos':'sum','Valor_Glosa':'last','Valor Glosa Actual':'first','Nota_crédito':'first','Estado_actual':'first','NIT':'last','Razon_Social':'last','Rete_Fuente':'sum','Rete_ICA':'sum','IVA':'sum','Rete_IVA':'sum','Fecha_Radicacion':'last','Cantidad de Numero_OP':'sum'})

    df_resumen = df_resumen.merge(df_prueba, how = 'left', on = 'Cod_Barra')
    
    df_resumen['Total Valor Pagado'] = df_resumen['Valor_Pagado_Egresos'] + df_resumen['Rete_ICA'] + df_resumen['Rete_Fuente']

    glosas = Glosas.copy()
    glosas = glosas[glosas['Origen_Glosa'] == 'Salud']

    
    glosas.loc[glosas['Codigo_Glosa'].str[1] == '1','Homo_Rubro'] = '1. Facturación'
    glosas.loc[glosas['Codigo_Glosa'].str[1] == '2','Homo_Rubro'] = '2. Tarifas'
    glosas.loc[glosas['Codigo_Glosa'].str[1] == '3','Homo_Rubro'] = '3. Soportes'
    glosas.loc[glosas['Codigo_Glosa'].str[1] == '4','Homo_Rubro'] = '4. Autorizaciones'
    glosas.loc[glosas['Codigo_Glosa'].str[1] == '5','Homo_Rubro'] = '5. Cobertura'
    glosas.loc[glosas['Codigo_Glosa'].str[1] == '6','Homo_Rubro'] = '6. Pertinencia'
    glosas.loc[glosas['Codigo_Glosa'].str[1] == '8','Homo_Rubro'] = '8. Devolución'

    glosas['Numero_Interno'] = CambioFormato(glosas, a = 'Numero_Interno')
    
    glosas2 = glosas.pivot_table(index = 'Numero_Interno', 
                        values = ['Valor_Glosa_Detalle'], 
                        columns = 'Homo_Rubro', fill_value = 0.0,
                        aggfunc = 'sum'
                        )

    glosas2.columns = glosas2.columns.get_level_values(1)
    glosas2 = glosas2.reset_index()

    glosas3 = glosas.pivot_table(index = 'Numero_Interno', 
                        values = ['Valor_Glosa_Detalle'], 
                        columns = 'Homo_Rubro', fill_value = 0.0,
                        aggfunc = 'count'
                        )

    glosas3.columns = glosas3.columns.get_level_values(1)
    glosas3 = glosas3.reset_index()

    glosas3 = glosas3.rename(columns = {'1. Facturación':'1. Cantidad Facturación',
                                        '2. Tarifas':'2. Cantidad Tarifas',
                                        '3. Soportes':'3. Cantidad Soportes',
                                        '4. Autorizaciones':'4. Cantidad Autorizaciones',
                                        '5. Cobertura':'5. Cantidad Cobertura',
                                        '6. Pertinencia':'6. Cantidad Pertinencia',
                                        '8. Devolución':'8. Cantidad Devolución'})
 
    df_resumen['Pago con impuestos'] = df_resumen['Valor_Pagado_Egresos'] + df_resumen['Rete_Fuente'] + df_resumen['Rete_ICA']
    devoluciones = ['AUDITADA: Devuelta sin posibilidad de re-ingreso.','EN AUDITORIA: Factura Anulada','AUDITADA: Factura Anulada']

    df_resumen.loc[df_resumen['Estado_actual'].isin(devoluciones) == True,'Grupo'] = 'Devueltas o Anuladas'
    df_resumen.loc[(df_resumen['Grupo'].isnull() == True) & (df_resumen['Cantidad de Numero_OP'] == 0),'Grupo'] = 'Sin pago'
    df_resumen.loc[df_resumen['Grupo'].isnull() == True,'Grupo'] = 'Con Pago'
    
    df_resumen['Valor_Glosa_prueba'] = np.where(df_resumen['Nota_crédito'] == df_resumen['Valor_Glosa'],0,df_resumen['Valor_Glosa'])

    df_resumen = df_resumen[df_resumen['Fecha_Radicacion'].dt.year >= 2021]
    
    df_resumen['NIT'] = CambioFormato(df_resumen, a = 'NIT')

    df_resumen2 = df_resumen.merge(glosas2, how = 'left', left_on = 'Fac_Norint', right_on = 'Numero_Interno')
    df_resumen2 = df_resumen2.drop(columns = ['Numero_Interno'])
    df_resumen2 = df_resumen2.merge(glosas3, how = 'left', left_on = 'Fac_Norint', right_on = 'Numero_Interno')    
    df_resumen2 = df_resumen2.drop(columns = ['Numero_Interno'])

    return df_resumen2


def Tablero_Glosas(Maestro_Salud):

    columnas = ['Fecha_OP','Numero_OP','Valor_Pagado_Egresos','Estado_Pago',
                    'Rete_ICA','Rete_Fuente','Cod_Barra','Rete_IVA']

    sisco_pagos = Maestro_Salud[columnas].copy()
                    
    sisco_pagos['Fecha_OP'] = pd.to_datetime(sisco_pagos['Fecha_OP'], format = '%d/%m/%Y', dayfirst = True)
    sisco_pagos['Numero_OP'] = CambioFormato(sisco_pagos, a = 'Numero_OP')
    sisco_pagos = sisco_pagos[sisco_pagos['Numero_OP'].isnull() == False]
    sisco_pagos['Valor_Pagado_Egresos'] = sisco_pagos['Valor_Pagado_Egresos'].astype(str).str.replace(',','.').astype(float)
    sisco_pagos['Rete_ICA'] = sisco_pagos['Rete_ICA'].astype(str).str.replace(',','.').astype(float)
    sisco_pagos['Rete_Fuente'] = sisco_pagos['Rete_Fuente'].astype(str).str.replace(',','.').astype(float)
    sisco_pagos['Rete_IVA'] = sisco_pagos['Rete_IVA'].astype(str).str.replace(',','.').astype(float)

    sisco_pagos['Total Valor Pagado'] = sisco_pagos['Valor_Pagado_Egresos'] + sisco_pagos['Rete_ICA'] + sisco_pagos['Rete_Fuente'] + sisco_pagos['Rete_IVA']

    sisco_pagos['Cantidad de OPs'] = sisco_pagos['Numero_OP'].copy()

    sisco_pagos = sisco_pagos.sort_values('Fecha_OP', ascending = False)

    sisco_pagos = sisco_pagos.groupby('Cod_Barra', as_index = False).agg({
                        'Total Valor Pagado':sum,'Fecha_OP':'first','Cantidad de OPs':'count',
                        'Valor_Pagado_Egresos':'sum','Estado_Pago':'last','Rete_ICA':'sum',
                        'Rete_Fuente':'sum'})

    sisco_pagos = sisco_pagos.rename(columns = {'Fecha_OP':'Ultima Fecha de Pago'})
    sisco_pagos['Total Valor Pagado'] = sisco_pagos['Total Valor Pagado'].astype(str).str.replace('.',',')
    sisco_pagos['Valor_Pagado_Egresos'] = sisco_pagos['Valor_Pagado_Egresos'].astype(str).str.replace('.',',')
    sisco_pagos['Rete_ICA'] = sisco_pagos['Rete_ICA'].astype(str).str.replace('.',',')
    sisco_pagos['Rete_Fuente'] = sisco_pagos['Rete_Fuente'].astype(str).str.replace('.',',')
    sisco_pagos['Cantidad de OPs'] = sisco_pagos['Cantidad de OPs'].astype(int)
    
    columnas = ['Regimen','NIT','Razon_Social','Tipo_Doc_Prestador','Fecha_Radicacion','Fecha_Ingreso',
                'Numero_Factura_Original','Valor_Neto','Estado_actual','Fecha_Ultimo_estado','Fecha_Auditoria',
                'Auditor','Fecha_Factura','Valor_Glosa','Cod_Barra','TipoCuentaMedica',
                'Fac_Norint','Valor_Bruto','Valor_Copago','Valor_Cuota_Moderadora','Valor_Descuento',
                'Valor_Iva','Descuento_Pronto_Pago','Auditor_conciliación','Departamento','Municipio',
                'Zona','Nota_crédito','Fecha de registro','Fecha de causacion','Numero_conciliacion']
        
    Maestro2 = Maestro_Salud[columnas].copy()
    Maestro2 = Maestro2.rename( columns = {'Numero_Factura_Original':'Numero_Factura'})
        
    Maestro2['Valor ultima Glosa'] = Maestro2['Valor_Glosa'].copy()
    
    Maestro2['Cod_Barra'] = CambioFormato(Maestro2, a = 'Cod_Barra')
    
    
    total_ops = Maestro_Salud[['Cod_Barra','Numero_OP']].copy()
    
    total_ops['Cod_Barra'] = CambioFormato(total_ops, a = 'Cod_Barra')

    total_ops['Numero_OP'] = CambioFormato(total_ops, a = 'Numero_OP')

    total_ops['llave'] = total_ops['Cod_Barra'].astype(str) + total_ops['Numero_OP'].astype(str)
    
    total_ops['Numero_OP'] = total_ops['Numero_OP'].astype(str) + '-'
    
    total_ops = total_ops.drop_duplicates('llave')
    
    total_ops = total_ops.drop(columns = ['llave'])
    
    total_ops = total_ops.groupby('Cod_Barra', as_index = False)['Numero_OP'].sum()
    
    total_ops['Numero_OP'] = total_ops['Numero_OP'].replace({'nan-',''})
    
    total_ops['Numero_OP'] = np.where(total_ops['Numero_OP'].str[-1] == '-',total_ops['Numero_OP'].str[0:-1], total_ops['Numero_OP'])

    total_ops = total_ops.rename(columns = {'Numero_OP':'Ordenes de pago'})

    Maestro2['Fecha_Ultimo_estado'] = pd.to_datetime(Maestro2['Fecha_Ultimo_estado'], format = '%d/%m/%Y', dayfirst = True)
    
    Maestro2['Fecha_Radicacion'] = pd.to_datetime(Maestro2['Fecha_Radicacion'], format = '%d/%m/%Y', dayfirst = True)
    
    Maestro2 = Maestro2.sort_values('Fecha_Radicacion', ascending = False)
    
    Maestro2 = Maestro2.drop_duplicates('Cod_Barra', keep = 'first')
    
    Maestro2 = Maestro2.merge(total_ops, how = 'left', on = 'Cod_Barra')
    
    del total_ops

    Maestro2 = Maestro2[Maestro2['Fecha_Radicacion'].dt.year >= 2021]

    Maestro2['Cod_Barra'] = CambioFormato(Maestro2, a = 'Cod_Barra')

    columnas = ['Valor_Neto','Valor_Copago','Valor_Cuota_Moderadora','Valor_Descuento','Valor_Iva',
                    'Valor_Glosa','Valor_Bruto','Descuento_Pronto_Pago']

    for i in columnas:
        print(i)
        Maestro2[i] = Maestro2[i].astype(str).str.replace('.',',')
            
    Maestro2 = Maestro2.merge(sisco_pagos, how = 'left', on = 'Cod_Barra')

    return Maestro2


    
    