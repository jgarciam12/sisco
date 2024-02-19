# -*- coding: utf-8 -*-
"""
Created on Tue Aug 23 12:03:03 2022

@author: jcgarciam
"""
import pandas as pd
import numpy as np

def CambioFormato(df, a = 'a'):
    df[a] = df[a].astype(str).str.strip('\x02').str.strip('').str.strip()
    df[a] = np.where(df[a].str[-2::] == '.0', df[a].str[0:-2], df[a])
    df[a] = np.where(df[a] == 'nan', np.nan, df[a])
    return df[a]

def MaestroConsolidado(Pagos_Parciales, Maestro):
    
    Pagos_Parciales['Origen'] = 'Pagos Parciales'

    Maestro_Sin_PP = Maestro[~Maestro['Cod_Barra'].isin(Pagos_Parciales['Cod_Barra'].unique())]
    Maestro_Sin_PP['Origen'] = 'Maestro'
    
    # A la base de Pagos parciales se le requiere adjuntar el Estado_actual, junto con su misma 'Fecha_Ultimo_estado
    # que viene de la base Maestro
    Pagos_Parciales = Pagos_Parciales.rename(columns = {'Fac_Nroint':'Fac_Norint','Valor_Neto':'Valor_Neto_Pago_Parcial'})
    Maestro_temp = Maestro[['Cod_Barra','Estado_actual','Fecha_Ultimo_estado','TipoCuentaMedica','Valor_Neto']]
    
    Maestro_temp = Maestro_temp[Maestro_temp['Cod_Barra'].astype(str).str.lower() != 'nan']
    Maestro_temp = Maestro_temp[Maestro_temp['Cod_Barra'].isnull() == False]
    
    Pagos_Parciales2 = Pagos_Parciales.merge(Maestro_temp, how = 'left', on = 'Cod_Barra')
    Maestro_Sin_PP = Maestro_Sin_PP.rename(columns = {'Numero_Factura':'Numero_Factura_Original'})
    
    Maestro_Consolidado = pd.concat([Pagos_Parciales2,Maestro_Sin_PP]).reset_index(drop = True)
    
    Maestro_Consolidado['Fac_Norint'] = Maestro_Consolidado['Fac_Norint'].replace('Medicos',0)
    

    Maestro_Consolidado['Fac_Norint'] = CambioFormato(Maestro_Consolidado, a ='Fac_Norint')
    
    Maestro_Consolidado['Numero_Factura_Original'] = Maestro_Consolidado['Numero_Factura_Original'].str.strip('\x02')
    
    Maestro_Consolidado.Fecha_Auditoria = pd.to_datetime(Maestro_Consolidado.Fecha_Auditoria, format = '%Y-%m-%d')
    Maestro_Consolidado.Fecha_Auditoria = Maestro_Consolidado.Fecha_Auditoria.dt.strftime('%d/%m/%Y')
    
    Maestro_Consolidado['Fecha_Radicacion'] = pd.to_datetime(Maestro_Consolidado['Fecha_Radicacion'], format = '%d/%m/%Y')
    Maestro_Consolidado['Fecha_Radicacion'] = Maestro_Consolidado['Fecha_Radicacion'].dt.strftime('%d/%m/%Y')
    
    return Maestro_Consolidado

def RR_Cuentas_medicasFinal(RR_Cuentas_medicas):
    
    #Se realiza un tratamiento a los datos de radicación rapida ya que esta base
    # viene con se parador coma, pero hay registros que tienen coma, entonces hay
    # que hacerle el siguiente tratamiento.
    # Este tratamiento se hizo cuando la base al cargarla generaba 12 columnas, si esto
    # llega aumentar en algun momento debe revisarse nuevamente el tratamiento ya que aparecieron
    # mas comas que no son separadores de las que habian cuando se realizo este proceso
    
    print('\n Corrigiento datos de la base de Radicación Rápida de Salud')
    
    if len(RR_Cuentas_medicas.columns) > 12:
        import pyttsx3
        import time
        
        engine = pyttsx3.init()
        engine.say('La base de Radicación Rápida debería revisarse ya que tiene más de 12 columnas')
        engine.runAndWait()
        
        print('La base de Radicación Rápida debería revisarse ya que tiene más de 12 columnas')
        time.sleep(30)
    
    a = RR_Cuentas_medicas[RR_Cuentas_medicas[10].astype(str).str.upper().str.isupper() == True]
    a = a[[5,8,9]]
    b = RR_Cuentas_medicas[RR_Cuentas_medicas[10].astype(str).str.upper().str.isupper() == False]
    b = b[[4,7,8]]

    b.columns = b.iloc[0]
    b = b[1:]

    a = a.rename(columns = {5:'FOLIO',8:'FECHA INGRESO',9:'ESTADO'})

    RR_Cuentas_medicas = pd.concat([a,b])
    
    if len(RR_Cuentas_medicas.columns) > 3:
        import pyttsx3
        import time
        
        engine = pyttsx3.init()
        engine.say('La base de Radicación Rápida debería revisarse ya que quedó con más de 3 columnas')
        engine.runAndWait()
        
        print('La base de Radicación Rápida debería revisarse ya que quedó con más de 3 columnas')
        print(RR_Cuentas_medicas.columns)
        time.sleep(30)
    
    RR_Cuentas_medicas = RR_Cuentas_medicas.drop_duplicates('FOLIO', keep = 'last')
    RR_Cuentas_medicas['FOLIO'] = RR_Cuentas_medicas['FOLIO'].astype(str)
    RR_Cuentas_medicas['FOLIO'] = RR_Cuentas_medicas['FOLIO'].str.strip()
    RR_Cuentas_medicas = RR_Cuentas_medicas[RR_Cuentas_medicas['FOLIO'].astype(str).str.lower() != 'nan']
    RR_Cuentas_medicas = RR_Cuentas_medicas[RR_Cuentas_medicas['FOLIO'].isnull() == False]
    
    return RR_Cuentas_medicas

def RPT_Radicados_ARL_Final(RPT_RADICADOS_ARL):
    
    RPT_RADICADOS_ARL['RADICADO'] = CambioFormato(RPT_RADICADOS_ARL, a = 'RADICADO')    
    
    RPT_RADICADOS_ARL = RPT_RADICADOS_ARL[RPT_RADICADOS_ARL['RADICADO'].isnull() == False]
    
    RPT_RADICADOS_ARL['CODIGO_BARRAS'] = CambioFormato(RPT_RADICADOS_ARL, a = 'CODIGO_BARRAS')
    
    RPT_RADICADOS_ARL = RPT_RADICADOS_ARL[RPT_RADICADOS_ARL['CODIGO_BARRAS'].isnull() == False]    
    
    RPT_RADICADOS_ARL['Llave'] =  RPT_RADICADOS_ARL['RADICADO'].astype(str) + RPT_RADICADOS_ARL['CODIGO_BARRAS'].astype(str)
    
    RPT_RADICADOS_ARL = RPT_RADICADOS_ARL.drop_duplicates('Llave', keep = 'last')
    
    RPT_RADICADOS_ARL = RPT_RADICADOS_ARL.drop(columns = ['Llave'])

    return RPT_RADICADOS_ARL
    
def PagosFinal(Pagos):
    
    #Se quitan NUREFEs vacios 
    
    Pagos['NUREFE'] = CambioFormato(Pagos,a = 'NUREFE')
    Pagos['NUORPA'] = CambioFormato(Pagos,a = 'NUORPA')
    Pagos = Pagos[Pagos['NUREFE'].isnull() == False]

    # Se hace la siguente llave para luego quitar duplicados y calcular los impuestos
    Pagos['Llave'] = Pagos['NUREFE'].astype(str) + Pagos['NUORPA'].astype(str) + Pagos['NUMCCT'].astype(str) + Pagos['CODIM1'].astype(str) + Pagos['CODIM2'].astype(str) + Pagos['CODIM3'].astype(str) + Pagos['CODIM4'].astype(str)
    Pagos = Pagos.drop_duplicates('Llave', keep = 'last')


    Pagos['IVA'] = np.where(Pagos['CODIM1'] == 1, Pagos['VALIM1'],0)

    conditions = [
                (Pagos['CODIM1'] == 2),
                (Pagos['CODIM2'] == 2)
                ]
    choices = [Pagos['VALIM1'], Pagos['VALIM2']] 

    Pagos['RTE_FUE'] = np.select(conditions,choices)

    conditions = [
                (Pagos['CODIM1'] == 3),
                (Pagos['CODIM2'] == 3),
                (Pagos['CODIM3'] == 3)
                ]
    choices = [Pagos['VALIM1'], Pagos['VALIM2'], Pagos['VALIM3']] 

    Pagos['RTE_ICA'] = np.select(conditions,choices)

    conditions = [
                (Pagos['CODIM1'] == 4),
                (Pagos['CODIM2'] == 4),
                (Pagos['CODIM3'] == 4),
                (Pagos['CODIM4'] == 4)
                ]
    choices = [Pagos['VALIM1'], Pagos['VALIM2'], Pagos['VALIM3'], Pagos['VALIM4']] 

    Pagos['RTE_IVA'] = np.select(conditions,choices)

    Pagos.loc[:,['IVA','RTE_FUE','RTE_ICA','RTE_IVA']] = Pagos.loc[:,['IVA','RTE_FUE','RTE_ICA','RTE_IVA']].fillna(0)

    #Un NUREFE puede tener diferentes NUORPAs porque puede ser una factura multiusuario,
    #Un NUORPA puede tener diferentes NUORPAs porque puede realizarse un pago multiple
    #Por eso la llave unica deberia ser NUREFE-NUORPA
    Pagos2 = Pagos[['NUREFE','NUORPA','IVA','RTE_FUE','RTE_ICA','RTE_IVA']]

    Pagos2 = Pagos2.groupby(['NUREFE','NUORPA'], as_index = False)['IVA','RTE_FUE','RTE_ICA','RTE_IVA'].sum().reset_index(drop = True)

    Pagos3 = Pagos.drop( columns = ['IVA','RTE_FUE','RTE_ICA','RTE_IVA'])
    Pagos3['Llave'] = Pagos3['NUREFE'].astype(str) + Pagos3['NUORPA'].astype(str)
    Pagos3['FECCAU'] = Pagos3['FECCAU'].astype(str)
    Pagos3['FECCAU'] = np.where(Pagos3['FECCAU'].str.contains('9490328') == True, Pagos3['FECGRA'], Pagos3['FECCAU'])
    Pagos3['FECGRA'] = pd.to_datetime(Pagos3['FECGRA'], format = '%Y%m%d')
    Pagos3['FECCAU'] = pd.to_datetime(Pagos3['FECCAU'], format = '%Y%m%d')
    Pagos3 = Pagos3.sort_values(['FECCAU','FECGRA'], ascending = False)
    Pagos3 = Pagos3.drop_duplicates('Llave', keep = 'first')
    Pagos3 = Pagos3.drop(columns = ['Llave'])
    
    Pagos4 = Pagos3.merge(Pagos2, how = 'inner', on = ['NUREFE','NUORPA'])
    Pagos = Pagos4[['NUREFE','FEESOP','NUORPA','VANEPG','VAORPA','IVA','RTE_FUE','RTE_ICA','RTE_IVA','ESTOPG','CODFPG',
                'ESTCAU','CODCOM','CODFUE','FECGRA','FECCAU']]
    
    # Un dato estaba viniendo con la fecha mal
    Pagos['FEESOP'] = Pagos['FEESOP'].replace(50210915,20210915)
    
    Pagos = Pagos.rename(columns = {'FEESOP':'Fecha_OP','NUORPA':'Numero_OP','VAORPA':'Valor_OP','VANEPG':'Valor_Pagado_Egresos',
                                    'ESTOPG':'Estado_Pago','CODFPG':'Codigo_Forma_Pago','ESTCAU':'Estado_Egresos',
                                    'RTE_FUE':'Rete_Fuente','RTE_ICA':'Rete_ICA','RTE_IVA':'Rete_IVA',
                                    'FECGRA':'Fecha de registro','FECCAU':'Fecha de causacion'})
    
    Pagos['Fecha de registro'] = Pagos['Fecha de registro'].dt.strftime('%d/%m/%Y')
    Pagos['Fecha de causacion'] = Pagos['Fecha de causacion'].dt.strftime('%d/%m/%Y')

    Pagos['Fecha_OP'] = np.where(Pagos['Fecha_OP'] != 0, Pagos['Fecha_OP'], np.nan)
    Pagos['Fecha_OP'] = pd.to_datetime(Pagos['Fecha_OP'], format = '%Y%m%d')
    Pagos['Fecha_OP'] = Pagos['Fecha_OP'].dt.strftime('%d/%m/%Y')

    return Pagos


def AsistencialesFinal(Asistenciales):
    
    Asistenciales = Asistenciales.drop_duplicates('RADICADO', keep = 'last')
    Asistenciales['RADICADO'] = Asistenciales['RADICADO'].astype(str)
    Asistenciales['RADICADO'] = Asistenciales['RADICADO'].str.strip()
    
    Asistenciales = Asistenciales[Asistenciales['RADICADO'].astype(str).str.lower() != 'nan']    
    Asistenciales = Asistenciales[Asistenciales['RADICADO'].isnull() == False]
    
    Asistenciales['EGRESOSDC'] = Asistenciales['EGRESOSDC'].astype(str)
    Asistenciales['EGRESOSDC'] = Asistenciales['EGRESOSDC'].str.strip()
    
    Asistenciales['DELNRE'] = Asistenciales['DELNRE'].astype(str)
    Asistenciales['DELNRE'] = Asistenciales['DELNRE'].str.strip()
    
    Asistenciales['F_LOTE'] = Asistenciales['F_LOTE'].astype(str)
    Asistenciales['F_LOTE'] = Asistenciales['F_LOTE'].str.strip()
    Asistenciales['F_LOTE'] = np.where(Asistenciales['F_LOTE'].str[6:8] == '00', Asistenciales['F_LOTE'].str[0:6] + '01',Asistenciales['F_LOTE'])

    Asistenciales['F_LOTE'] = pd.to_datetime(Asistenciales['F_LOTE'], format = '%Y%m%d')
    Asistenciales['F_LOTE'] = Asistenciales['F_LOTE'].dt.strftime('%d/%m/%Y')
    
    Asistenciales = Asistenciales.rename(columns = {'RAAEST':'Estado_Asitenciales','EGRESOSDC':'Referencia_Egresos',
                                                    'DELNRE':'Siniestro','DELSRE':'Secuencia','F_LOTE':'Fecha_Lote'})

    return Asistenciales

def GlosasFinal(Glosas):
    
    Glosas.Numero_Interno = Glosas.Numero_Interno.astype(int).astype(str)
    Glosas.Numero_Interno = Glosas.Numero_Interno.str.strip()
    Glosas = Glosas[Glosas['Numero_Interno'].astype(str).str.lower() != 'nan']    
    Glosas = Glosas[Glosas['Numero_Interno'].isnull() == False]    
    Glosas = Glosas.groupby('Numero_Interno', as_index = False).agg({'Auditor_conciliación':'last','Fecha_Envio':'last','Nro_Guia':'last','Tipo_Cuenta_Medica':'last','Codigo_Glosa':'last','Observacion':'last','Nota_crédito':'sum','Nombre_Estado_Actual':'last','Nombre_Estado_Actual':'last','Fecha_Ultimo_estado':'last'})
    Glosas.Fecha_Envio = pd.to_datetime(Glosas.Fecha_Envio, format = '%d/%m/%Y')
    Glosas.Fecha_Envio = Glosas.Fecha_Envio.dt.strftime('%d/%m/%Y')
    Glosas['Fecha_Ultimo_estado'] = pd.to_datetime(Glosas['Fecha_Ultimo_estado'], unit = 'd', origin = '1899-12-30').dt.date
    Glosas = Glosas.rename(columns = {'Fecha_Ultimo_estado':'Fecha_Ultimo_estado_glosa'})
    return Glosas


def UnificacionCampos(df, campo = 'a'):
    
    df[campo + '_x'] = df[campo + '_x'].fillna('Sin ' + campo)
    df[campo + '_y'] = df[campo + '_y'].fillna('Sin ' + campo)
    
    df[campo.title()] = np.where(df[campo + '_x'] != ('Sin ' + campo), df[campo + '_x'], df[campo + '_y'])
    
    return df


    
