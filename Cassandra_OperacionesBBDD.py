# -*- coding: utf-8 -*-
"""
@author: José Manuel
"""

#Importación de librerias necesarias para conexión con Cassandra y gestión de fechas
from cassandra.cluster import Cluster
from datetime import date
from datetime import datetime as dt

###########################  CLASES MODELO   #########################

class Provincia:
    
    def __init__(self, procod,jefes_provinciales, nombre):
        self.procod = procod
        self.jefes_provinciales = jefes_provinciales
        self.nombre = nombre
    #nuevo constructor que incluya la coleccion de jefes
    """
    def __init__(self, procod,jefes_provinciales, nombre,jefes):
        self.procod = procod
        self.jefes_provinciales = jefes_provinciales
        self.nombre = nombre
        self.jefes = jefes
    """
        
class Zona:
    
    def __init__(self, zoncod,nombre, municipios):
        self.zoncod = zoncod
        self.nombre = nombre
        self.municipios = municipios
"""        
    #constructor con respecto a relacion 1:N entre provincia-zona   
    def __init__(self, zoncod,nombre, municipios,id_provincia):
        self.zoncod = zoncod
        self.nombre = nombre
        self.municipios = municipios
        self.id_provincia = id_provincia 
"""        
            
class Dist_SubZona:   

    def __init__(self, zoncod,codsub, cantidad,fecha):
        self.zoncod = zoncod
        self.codsub = codsub
        self.cantidad = cantidad
        self.fecha = fecha
        
class Subestacion:
    def __init__(self, codsub,capacidad):
        self.codsub = codsub
        self.capacidad = capacidad
        
    #constructor con respecto a relacion 1:N entre Linea-Subestacion    
    def __init__(self, codsub,capacidad,id_linea):
        self.codsub = codsub
        self.capacidad = capacidad
        self.id_linea = id_linea


class Linea:
    
    def __init__(self, codLin, longitud):
        self.codLin = codLin
        self.longitud = longitud
        

class DistribucionRed:
          
    def __init__(self, coddis, longitud_max):
        self.coddis = coddis
        self.longitud_max = longitud_max
        
    #Constructor con respecto a relacion 1:N entre Estacion-Distribucion Red    
    def __init__(self, coddis, longitud_max,id_estacion):
        self.coddis = coddis
        self.longitud_max = longitud_max
        self.id_estacion = id_estacion
        
class Estacion:
    
    def __init__(self, codest, nombre):
        self.codest = nombre
        self.nombre = nombre
        
class Prov_EstProd:
    def __init__(self, codest, codpro):
        self.codest = codest
        self.codpro = codpro
        

class Productor:
    def __init__(self, codpro, media_produccion, maximo_produccion, nombre, pais, origen_energia):
        self.codpro = codpro
        self.media_produccion = media_produccion
        self.maximo_produccion = maximo_produccion
        self.nombre = nombre
        self.pais = pais
        self.origen_energia = origen_energia
       
        
########################  FUNCIONES PARA SOLICITUDES  ###################


#Función para pedir datos de un proveedor e insertarlos en la BBDD
def insertProvincia ():
    #Pedimos al usuario del programa los datos de la provincia
    nombre = input ("Dame nombre de la provincia : ")
    id_prov = input("Dame ID de la provincia : ")
    jefes = set() #iniciamos la colección (set) que contendra las preferencias a insertar
    jefe = input ("Introduzca el nombre de un nuevo jefe, vacío para parar : ")
    while (jefe != ""):
        jefes.add(jefe)
        jefe = input("Introduzca el nombre de un nuevo jefe, vacío para parar : ")
        
    p = Provincia(id_prov, jefes, nombre)
    
    # Según mi modelado debe ser incluido en las tablas 1 y 5 
    T1_insertStatement = session.prepare ("INSERT INTO tb_jefes_provincia (provincia_nombre, provincia_jefes_provinciales,provincia_procod) VALUES (?, ?,?)")
    session.execute (T1_insertStatement, [p.nombre, p.jefes_provinciales,p.procod])
    
    T5_insertStatement = session.prepare ("INSERT INTO tb_detalle_jefes_provincia  (provincia_jefes_provinciales, provincia_procod, provincia_nombre) VALUES (?, ?, ?)")
    for jef in jefes:
        session.execute(T5_insertStatement, [jef, p.procod, p.nombre])
    return p
 
def insertProductor():
    #Pedimos al usuario los datos del productor
    id_p = input("Dame ID del productor : ")
    med_produccion = float(input("Dame produccion media : "))
    max_produccion = float(input("Dame produccion maxima : "))
    nombre = input("Dame nombre del productor : ")
    pais = input("Dame pais del productor : ")
    origen = input("Dame origen del productor (eolica, nuclear, carbon, solar o gas): ")
    
    p = Productor(id_p, med_produccion, max_produccion, nombre, pais, origen)
    
    #Segun el modelo desarrollado debe ser incluido en la tabla 12 (TABLAS QUE NO DEPENDAN DE RELACIONES u  CAMPOS DE OTRAS TABLAS DESCONOCIDOS)
    T12_insertStatement = session.prepare ("INSERT INTO tb_productores_origen_energia (productor_origen_energia , productor_pais,productor_nombre,productor_media_produccion,productor_maximo_produccion,productor_codpro) VALUES (?, ?, ?,?,?,?)")
    session.execute (T12_insertStatement, [p.origen_energia, p.pais, p.nombre,p.media_produccion,p.maximo_produccion,p.codpro])

#Relacion Divide con Integridad de los datos
def insertProvinciaZona():
    
    id_zona = input("Dame ID de la zona : ")
    zona_nombre = input("Dame nombre de la zona : ")
    municipios = set()
    municipio = input ("Introduzca el nombre de un nuevo municipio, vacío para parar : ")
    while (municipio != ""):
        municipios.add(municipio)
        municipio = input("Introduzca el nombre de un nuevo municipio, vacío para parar : ")

    z = Zona(id_zona,zona_nombre,municipios)
    nombre_prov = input("Dame nombre de la provincia a la que pertenece la zona insertada : ")
    #Creacion de objeto Zona
    p = extraerProvincia(nombre_prov)
      
    if p !=  None:
        
        #Segun el modelo desarrollado debe ser incluido en la tabla 2 (TABLAS 1 Y 5 ya tienen esta info INTEGRIDAD)
        T2_insertStatement = session.prepare ("INSERT INTO tb_jefes_zona  (zona_nombre  , provincia_procod  , provincia_jefes_provinciales, provincia_nombre  ) VALUES (?, ?, ? ,?)")
        session.execute (T2_insertStatement, [z.nombre, p.procod, p.jefes_provinciales, p.nombre])
    
    else:
        
        #DEBEMOS INSERTAR LA PROVINCIA PORQUE NO EXISTE - Y DESPUES REPETIR LO MISMO QUE SI EXISTIERA
        print("Es necesario incluir previamente la provincia ya que no existe en la base de datos : ")
        p = insertProvincia () 
        T2_insertStatement = session.prepare ("INSERT INTO tb_jefes_zona  (zona_nombre  , provincia_procod  , provincia_jefes_provinciales,provincia_nombre) VALUES (?, ?, ?,?)")
        session.execute (T2_insertStatement, [z.nombre, p.procod, p.jefes_provinciales,p.nombre ])
        
#Extraer provincias por nombre de provincia 
def extraerProvincia(prov):
    
    #CONSULTO EN TABLA DE PROVINCIAS
    p = None
    select = session.prepare("SELECT provincia_nombre , provincia_jefes_provinciales, provincia_procod  FROM tb_jefes_provincia  WHERE provincia_nombre = ?") 
    filas = session.execute(select, [prov, ])
    for fila in filas: #Solo habrá un valor de existir consideramos nombre suficiente para no repetir valores
        p = Provincia(fila.provincia_nombre, fila.provincia_jefes_provinciales, fila.provincia_procod)
        return p
    return p


#Relacion Cabecera
def insertEstacionDistribucion():
    
    codest = input("Dame ID de la estacion : ")
    est_nombre = input("Dame nombre de la estacion : ")
    #codDis = input("Dame ID de la distribucion : ")
    #longitud_max = float(input("Dame longitud maxima de la distribucion : "))
    
    e = Estacion(codest, est_nombre)
    #d = DistribucionRed(codDis, longitud_max, codest)
    
    #Debemos insertar en tabla 3 - que relaciona estas dos entidades.
    T3_insertStatement = session.prepare ("UPDATE tb_distribuciones_estacion SET numdistribuciones = numdistribuciones +1 WHERE estacion_codest = ?")
    session.execute (T3_insertStatement, [e.codest])
    
#Relacion Suple
def insertLineaSubestacion():
    
    codlin = input("Dame ID de la linea : ")
    longitud = float(input("Dame longitud de la linea : "))
    codsub = input("Dame ID de la Subestacion : ")
    capacidad = float(input("Dame capacidad de la subestacion: "))
    
    l = Linea(codlin,longitud)
    s = Subestacion(codsub, capacidad, codlin)
    
    #Debemos insertar en tabla 4 que relaciona estas dos entidades
    T4_insertStatement = session.prepare ("INSERT INTO tb_capacidad_subest_lineas (subestacion_capacidad ,subestacion_codsub ,linea_longitud ,linea_codlin ) VALUES (?,?,?,?)")
    session.execute (T4_insertStatement, [s.capacidad,s.codsub,l.longitud,l.codLin])
    
#Relacion Distribuye
def insertSubestacionZona():
    
    codsub = input("Dame ID de la subestacion : ")
    zoncod = input("Dame ID de la zona : ")
    cantidad = float(input("Dame cantidad distribuida : "))
    f = input("Dame fecha con formato %m/%d/%y ")
    fecha = dt.strptime(f, '%m/%d/%y')
    d = Dist_SubZona(zoncod, codsub, cantidad, fecha)
    #Debemos insertar en tabla 6 que se corresponde con esta relacion
    T6_insertStatement = session.prepare ("INSERT INTO tb_suministro_subestacion_zona (distribuye_fecha  ,subestacion_codsub  ,zona_zoncod  ,distribuye_cantidad  ) VALUES (?,?,?,?)")
    session.execute (T6_insertStatement, [d.fecha,d.codsub,d.zoncod,d.cantidad])


#Relacion Cabecera-Consiste-Suple
def insertEstDistLinSubest():
    
    codest = input("Dame ID de la Estacion : ")
    estacion_nombre = input("Dame nombre de la Estacion : ")
    e = Estacion(codest, estacion_nombre)
    
    codlin = input("Dame ID de la Linea : ")
    longitud = float(input("Dame longitud de la Linea : "))
    l = Linea(codlin, longitud)
    
    codsub = input("Dame ID de la Subestacion : ")
    capacidad = float(input("Dame capacidad de la Subestacion : "))
    s = Subestacion(codsub, capacidad, codlin)
    
    #insertamos en tabla 7 que almacena parte de esta inforamacion
    T7_insertStatement = session.prepare ("INSERT INTO tb_suministro_estacion_subestacion  (estacion_nombre   ,linea_codlin  ,subestacion_codsub ,linea_longitud  ) VALUES (?,?,?,?)")
    session.execute (T7_insertStatement, [e.nombre,l.codLin,s.codsub,l.longitud])
    
#Relacion Suple-Distribuye-Divide   
def insertLinSubZonProv():
    
    codLin = input("Dame ID de la Linea : ")
    longitud = float(input("Dame la longitud de la linea : "))
    l = Linea(codLin, longitud)
    
    codsub = input("Dame ID de la subEst : ")
    sub_capacidad = int(input("Dame la capacidad de la subestacion : "))
    s = Subestacion(codsub, sub_capacidad, codLin)
    
    f = input("Dame fecha con formato %m/%d/%y ")
    fecha = dt.strptime(f, '%m/%d/%y')
    distribuye_cantidad = float(input("Introduzca cantidad de la distribución : "))
    
    zoncod = input("Dame ID de la zona : ")
    zon_nombre = input("Dame nombre de la zona : ") 
    municipios = set() 
    municipio = input ("Introduzca el nombre de un nuevo municipio, vacío para parar : ")
    while (municipio != ""):
        municipios.add(municipio)
        municipio= input ("Introduzca el nombre de un nuevo municipio, vacío para parar : ")
    
    d = Dist_SubZona(zoncod, codsub, distribuye_cantidad, fecha)
    
    #CHEQUEAMOS INTEGRIDAD DE PROVINCIA EMPLEANDO TABLA 2   
    p = extraerProvincia_Zona(zon_nombre)
    if p!= None:
        z = Zona(zoncod, zon_nombre, municipios)
    
    else:
        p = insertProvincia () #se crea objeto p de prov y se inserta en tablas 1 y 5
        z = Zona(zoncod, zon_nombre, municipios)
        
    #Añadimos en tablas 2, 4, 6, 8 y 9 
    #Tabla 2    
    T2_insertStatement = session.prepare ("INSERT INTO tb_jefes_zona  (zona_nombre  , provincia_procod  , provincia_jefes_provinciales  ) VALUES (?, ?, ?)")
    session.execute (T2_insertStatement, [z.nombre, p.procod, p.jefes_provinciales])
    #Tabla 4
    T4_insertStatement = session.prepare ("INSERT INTO tb_capacidad_subest_lineas (subestacion_capacidad ,subestacion_codsub ,linea_longitud ,linea_codlin ) VALUES (?,?,?,?)")
    session.execute (T4_insertStatement, [s.capacidad,s.codsub,l.longitud,l.codLin])
    #Tabla 6
    T6_insertStatement = session.prepare ("INSERT INTO tb_suministro_subestacion_zona (distribuye_fecha  ,subestacion_codsub  ,zona_zoncod  ,distribuye_cantidad  ) VALUES (?,?,?,?)")
    session.execute (T6_insertStatement, [d.fecha,d.codsub,d.zoncod,d.cantidad])
    #Tabla 8
    T8_insertStatement = session.prepare ("INSERT INTO tb_provincias_linea (linea_longitud  ,linea_codlin  ,zona_zoncod  ,provincia_nombre  ) VALUES (?,?,?,?)")
    session.execute (T8_insertStatement, [l.longitud,l.codLin,z.zoncod,p.nombre])
    #Tabla 9
    T9_insertStatement = session.prepare ("UPDATE tb_capacidad_zona SET capacidad = capacidad + ? WHERE zona_nombre = ?")
    session.execute (T9_insertStatement, [s.capacidad,z.nombre])

        
def extraerProvincia_Zona(zona):
    
    #CONSULTO EN TABLA 2
    p = None
    select = session.prepare("SELECT provincia_nombre , provincia_jefes_provinciales, provincia_procod  FROM tb_jefes_zona  WHERE zona_nombre = ?") 
    filas = session.execute(select, [zona, ])
    for fila in filas:
        p = Provincia(fila.provincia_procod, fila.provincia_jefes_provinciales, fila.provincia_nombre)
        return p
    return p
    
 
#Relacion Provee-Cabecera

def insertProdEstDist():
    
    codest = input("Dame ID de la Estacion : ")
    estacion_nombre = input("Dame nombre de la Estacion : ")
    e = Estacion(codest, estacion_nombre)
    
    coddis = input("Dame ID de la distribucion : ")
    longitud_max = float(input("Dame la longitud de la distribucion : "))
    d = DistribucionRed(coddis, longitud_max, codest)
    
    codpro = input("Dame ID del productor : ")
    media_produccion = float(input("Dame produccion media del productor : "))
    maximo_produccion = float(input("Dame produccion maxima del productor : "))
    nombre = input("Dame nombre del productor : ")
    pais = input("Dame pais del productor : ")
    origen = input("Dame origen del productor : ")   
    p = Productor(codpro, media_produccion, maximo_produccion, nombre, pais, origen)
    
    #Insertamos en tablas 11 y 12
    #Tabla 11    
    T11_insertStatement = session.prepare ("INSERT INTO tb_distribucion_red_proveedores  (distribucion_longitud  , estacion_codest  , productor_codpro, productor_nombre  ) VALUES (?, ?, ?,?)")
    session.execute (T11_insertStatement, [d.longitud_max, e.codest, p.codpro,p.nombre])
    #Tabla 12
    T12_insertStatement = session.prepare ("INSERT INTO tb_productores_origen_energia  (productor_origen_energia  , productor_pais  , productor_nombre,productor_media_produccion, productor_maximo_produccion, productor_codpro) VALUES (?, ?, ?, ?, ?)")
    session.execute (T12_insertStatement, [p.origen_energia, p.pais, p.nombre,p.media_produccion,p.maximo_produccion,p.codpro])

########################## ACTUALIZACIONES

def actualizaProv(): #Actualizar tablas 1 y 2
    
    # mi ID espera un texto similar a "codigo de barras" que es 'friendly' para las consultas de usuario
    
    codpro = input("Dame ID de la provincia a actualizar : ")
    new_nombre = input("Dame el nombre de la nueva provincia ")
       
    select_statement = session.prepare ("SELECT * FROM tb_jefes_provincia  WHERE provincia_procod = ? ALLOW FILTERING")
    fila = session.execute (select_statement, [codpro,])
    p = Provincia(fila.provincia_procod, fila.provincia_jefes_provinciales, fila.provincia_nombre)
    
    #Tabla 1 con INSERT Y DELETE
    delete_statement = session.prepare ("DELETE FROM tb_jefes_provincia WHERE provincia_nombre = ? and provincia_procod = ? ")
    session.execute (delete_statement, [p.nombre,p.procod])
    insert_statement = session.prepare ("INSERT INTO tb_jefes_provincia (provincia_nombre, provincia_jefes_provinciales, provincia_procod) VALUES (?, ?, ?)")
    session.execute (insert_statement, [new_nombre,p.jefes_provinciales,p.procod])
    
    #Tabla 2 - UPDATE
    zona = extraerZonaId(codpro)
    update_statement = session.prepare ("UPDATE tb_jefes_zona SET provincia_nombre = ? WHERE zona_nombre = ? and provincia_procod = ?")
    session.execute (update_statement, [new_nombre, zona, codpro])
    

def extraerZonaId(codpro):

    select_statement = session.prepare ("SELECT * FROM tb_jefes_zona WHERE provincia_procod = ? ALLOW FILTERING")
    fila = session.execute (select_statement, [codpro,])   
    nomZona = fila.zona_nombre
    return nomZona
    

def actualizaCapacidadSubEst():
    
    codsub = input("Dame ID de la subestacion a actualizar : ")
    new_capacidad = float(input("Dame nueva longitud : "))
    
    ## Al no existir ninguna tabla con partition key con este indice (codsub) empleo aunquesea mas ineficiente ALLOW FILTERING
    
    s,l = extraerSubestLinea(codsub) 
    if s != None:
        
        delete_statement = session.prepare ("DELETE FROM tb_capacidad_subest_lineas WHERE subestacion_capacidad = ? and subestacion_codsub = ? ")
        session.execute (delete_statement, [s.capacidad,s.codsub])
        insert_statement = session.prepare ("INSERT INTO tb_capacidad_subest_lineas (subestacion_capacidad, subestacion_codsub, linea_longitud, linea_codlin) VALUES (?, ?, ?, ?)")
        session.execute (insert_statement, [new_capacidad,s.codsub,l.longitud ,l.codLin])
    
        #Actualizo tabla 9 también, para ello es necesario extraerlo de la tabla de la relación distribuye
        dif_capacidad = new_capacidad - s.capacidad
        
        zonas = extraerZonas(codsub)
        
        for zona in zonas:
            
            update_statement = session.prepare ("UPDATE tb_capacidad_zona SET capacidad = capacidad + dif_capacidad WHERE zona_nombre = ?")
            session.execute (update_statement, [dif_capacidad,zona])
        
def extraerSubestLinea(id_sub):
    
    s = l = None
    select_statement = session.prepare ("SELECT * FROM tb_capacidad_subest_lineas WHERE subestacion_codsub = ? ALLOW FILTERING")
    filas = session.execute (select_statement, [id_sub,])
    
    for fila in filas:
        
        l = Linea(fila.linea_codlin, fila.linea_longitud)
        s = Subestacion(fila.subestacion_codsub,fila.subestacion_capacidad ,l.codLin)
        
        return s,l
    return s,l

def extraerZonas(id_sub):
    
    zonas = []
    select_statement = session.prepare ("SELECT * FROM tb_suministro_subestacion_zona WHERE subestacion_codsub = ? ALLOW FILTERING")
    filas = session.execute (select_statement, [id_sub,])
    
    for fila in filas: 
	
		#PARA PODER HACER ESTE PASO  AÑADIR A LA TABLA tb_suministro_subestacion_zona -> zona_nombre   
        zonas.append(fila.zona_nombre)
    
    return zonas
    
    
def actualizaOrigenProveedor():
    
    productor = input("Dame id de proveedor que quiere ser modificado ")
    new_origen = input("Dame nuevo origen para este proveedor : ")
    
    #Tabla 12
    select_statement = session.prepare ("SELECT * FROM tb_productores_origen_energia WHERE productor_codpro = ? ALLOW FILTERING")
    fila = session.execute (select_statement, [productor,])
    p = Productor(fila.productor_codpro, fila.productor_media_produccion, fila.productor_maximo_produccion, fila.productor_nombre, fila.productor_pais, fila.productor_origen_energia)
    #Al ser clave primero borramos y despues insertamos
    delete_statement = session.prepare ("DELETE FROM tb_productores_origen_energia WHERE productor_origen_energia = ? and productor_pais = ? and productor_nombre = ?")
    session.execute (delete_statement, [p.origen_energia,p.pais,p.nombre])
    insert_statement = session.prepare ("INSERT INTO tb_productores_origen_energia (productor_origen_energia,productor_pais,productor_nombre,productor_media_produccion, productor_maximo_produccion, productor_codpro) VALUES (?, ?, ?, ?, ?, ?)")
    session.execute (insert_statement, [new_origen,p.pais,p.nombre,p.media_produccion, p.maximo_produccion,p.codpro])    

######################### CONSULTAS -  Se realizan las consultas por las que fueron diseñadas las tablas (consultas simples y rapidas)

def consultas(numero):
    
    if numero == 10: 
        
        nombre = input ("Dame nombre de provincia")
        p = extraerProvincia(nombre)
        if (p != None): 
            print ("Jefes Provinciales: ", p.jefes_provinciales)
        else:
            print("No existe la provincia en la BBDD")
            
            
    elif numero == 11:
        
        zona = input ("Dame zona ")
        select = session.prepare("SELECT provincia_procod , provincia_jefes_provinciales, provincia_nombre  FROM tb_jefes_zona  WHERE zona_nombre = ?") 
        filas = session.execute(select, [zona, ])
        for fila in filas:
            print("Jefes : " , fila.provincia_jefes_provinciales)
            print("Provincia : ",fila.provincia_nombre)
        
    elif numero == 12:
        
        print("A continuación se mostraran el numero de distribuciones por estacion")
        
        select = session.prepare("SELECT *  FROM tb_distribuciones_estacion") 
        filas = session.execute(select)
        for fila in filas:
            print("Estacion : ", fila.estacion_codest)
            print("Numero de DIstribuciones : ", fila.numdistribuciones)
        
        
    elif numero == 13:
        
        capacidad = float(input ("Dame capacidad "))
        select = session.prepare("SELECT subestacion_capacidad , subestacion_codsub, linea_longitud, linea_codlin  FROM tb_capacidad_subest_lineas  WHERE subestacion_capacidad = ?") 
        filas = session.execute(select, [capacidad, ])
        for fila in filas:
            print("Codigo Subestacion : ", fila.subestacion_codsub)
            print("Linea Longitud : ", fila.linea_longitud)
            print("Codigo de linea : ", fila.linea_codlin)
        
    elif numero == 14:
        
        nombre_jefe = input("Dame el nombre de un jefe provincial : ")
        select = session.prepare("SELECT provincia_jefes_provinciales , provincia_procod, provincia_nombre  FROM tb_detalles_jefes_provincia  WHERE provincia_jefes_provinciales = ?") 
        filas = session.execute(select, [nombre_jefe, ])
        for fila in filas:
            print("Codigo de Provincia : ", fila.provincia_procod)
            print("Nombre de provincia : " , fila.provincia_nombre)
        
    elif numero == 15:
        
       f = input("Dame fecha con formato %m/%d/%y ")
       fecha = dt.strptime(f, '%m/%d/%y')
       select = session.prepare("SELECT subestacion_codsub , zona_zoncod, distribuye_cantidad  FROM tb_suministro_subestacion_zona  WHERE distribuye_fecha = ?") 
       filas = session.execute(select, [fecha, ])
       for fila in filas:
           print("Codigo Subestacion : ", fila.subestacion_codsub)
           print("Codigo de la zona : ", fila.zona_zoncod)
           print("Cantidad suministrada : ", fila.distribuye_cantidad)
    
    elif numero == 16:
        
        nombre = input("Dame nombre de la estacion ")
        select = session.prepare("SELECT subestacion_codsub , linea_codlin, linea_longitud  FROM tb_suministro_estacion_subestacion  WHERE estacion_nombre = ?") 
        filas = session.execute(select, [nombre, ])
        for fila in filas:
            print("Subestación: ",fila.subestacion_codsub)
            print("Codigo de linea : ", fila.linea_codlin)
            print("Longitud de la linea : ",fila.linea_longitud)
        
    elif numero == 17:
        
        long = float(input ("Longitud de la linea "))
        select = session.prepare("SELECT linea_codlin , zona_zoncod, provincia_nombre  FROM tb_provincias_linea  WHERE linea_longitud = ?") 
        filas = session.execute(select, [long, ])  
        for fila in filas:
            print("Codigo de linea : ", fila.linea_codlin)
            print("Codigo de zona : ",fila.zona_zoncod)
            print("Nombre de la provincia asociada : ", fila.provincia_nombre)
        
    elif numero == 18:
        
        zona = input ("Dame un nombre de la zona ")
        select = session.prepare("SELECT zona_nombre , capacidad FROM tb_capacidad_zona  WHERE zona_nombre = ?") 
        filas = session.execute(select, [zona, ])
        for fila in filas:
            print("La capacidad de esta zona es : ",fila.zona_nombre)
        
    elif numero == 19:
        
        long = float(input ("Longitud de la distribución: "))
        select = session.prepare("SELECT estacion_codest , productor_codpro, productor_nombre  FROM tb_distribucion_red_proveedores  WHERE distribucion_longitud = ?") 
        filas = session.execute(select, [long, ]) 
        for fila in filas:
            print("Codigo de la estacion : ",fila.estacion_codest)
            print("Codigo del productor : ", fila.productor_codpro)
            print("Nombre del productor : ", fila.productor_nombre)
        
    elif numero == 20:
        
        origen = input ("Dame origen del productor: ")
        pais = input("Dame pais del productor : ")
        select = session.prepare("SELECT productor_nombre , productor_media_produccion, productor_maximo_produccion, productor_codpro  FROM tb_productores_origen_energia  WHERE productor_origen_energia = ? and productor_pais = ?") 
        filas = session.execute(select, [origen,pais, ]) 
        for fila in filas: 
            print("Codigo del productor : ", fila.productor_codpro)
            print("Nombre del productor : ", fila.productor_nombre)
            print("Media produccion : ", fila.productor_media_produccion)
            print("Maxima produccion: ", fila.productor_maximo_produccion)
            
        
    
##################### PROGRAMA PRINCIPAL 

if __name__ == "__main__":
    
    print("Bienvenido")
    
    cluster = Cluster()
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect('josecuesta')
    numero = -1

    #Sigue pidiendo operaciones hasta que se introduzca 0
    while (numero != 0):
        
        #Inserción de datos
        print ("Introduzca un número para ejecutar una de las siguientes operaciones:")
        print ("1. Insertar una provincia")
        print ("2. Insertar un productor")
        print ("3. Insertar relación entre Provincia y Zona")
        print ("4. Insertar nueva distribución asociada a una estación")
        print ("5. Insertar nueva relación Linea-Subestación")
        print ("6. Insertar nueva relación Subestacion-Zona")
        print ("7. Insertar nueva relación Estación-Subestación")
        print ("8. Insertar nueva relación Linea-Provincia")
        print ("9. Insertar nueva relación Productor - Distribucion de red")
        
        #Consultas de las tablas
        print("10. Obtener los jefes provinciales asociados a una provincia")
        print("11. Obtener los jefes provinciales asociados a una zona en concreto")
        print("12. Obtener cuantas distribuciones de red tiene cada estacion")
        print("13. Obtener la capacidad de cada subestacion la longitud de las lineas a las que esta asociado")
        print("14. Obtener la informacion de las provincias segun los jefes provinciales")
        print("15. Consultar las zonas y subestaciones en las que estas ultmas han suministrado energia a una zona segun la fecha ")
        print("16. Consultar segun el nombre de una estacion las subestaciones que esta suministra, incluyendo la longitud de la linea ")
        print("17. Determina las provincias de cada linea buscando por la longitud de la linea ")
        print("18. Obtener la capacidad sumada de todas las subestaciones que se encuentran en una zona determinada ")
        print("19. Obtener los productores según longitud maxima de la distribucion ")
        print("20. Buscar productores según el origen de la energía ")
        
        #Actualizaciones de las tablas
        print("21. Actualiza el nombre de una provincia : ") 
        print("22. Actualiza la capacidad de una subestación : ") 
        print("23. Actualiza el origen de la energia de un productor : ")  

        #Cerrar aplicacion
        print ("0. Cerrar aplicación")
        
        numero = int (input()) #Pedimos numero al usuario
        if (numero == 1):
            insertProvincia ()
        elif (numero == 2):
            insertProductor()
            
        elif (numero == 3):
            insertProvinciaZona()
        
        elif (numero == 4):
            insertEstacionDistribucion()
            
        elif (numero == 5):
            insertLineaSubestacion() 
        
        elif (numero == 6):
            insertSubestacionZona()
            
        elif (numero == 7):
            insertEstDistLinSubest()
            
        elif (numero == 8):
            insertLinSubZonProv()  
            
        elif (numero == 9):
            insertProdEstDist()
            
        elif (numero > 9 and numero <= 20):
            consultas(numero)
            
        elif (numero == 21):
            actualizaProv()
            
        elif (numero == 22):
            actualizaCapacidadSubEst()
            
        elif (numero == 23):
            actualizaOrigenProveedor()
                
        else:
            print ("Número incorrecto")
             
    cluster.shutdown() #cerramos conexion