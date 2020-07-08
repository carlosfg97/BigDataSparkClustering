#Importar librerías para mrjob:

from mrjob.job import MRJob
from mrjob.step import MRStep

#Importar librerías para las operaciones matemáticas:

import math
from math import sqrt
import numpy as np
import random
import operator

#Archivos txt a emplear

archivo_datos = '/Users/carlosfabbri/documents/UP/2020-1/Data Analytics/proy_final/export-1.txt'  #Será llamado para generar los primeros centroides.

archivo_centroides= '/Users/carlosfabbri/documents/UP/2020-1/Data Analytics/proy_final/centroides_fuzzy.txt' #Centroides para cada clúster

archivo_membresia= '/Users/carlosfabbri/documents/UP/2020-1/Data Analytics/proy_final/archivo_membresia.txt' #Grados de membresía para cada punto


#Definir variables globales:

global k #Número de clústers
k=3

global n #Cantidad de registros
n=4339

global m #Parámetro para el algoritmo Fuzzy C-Means
m=1.7

global p #Parámetro p para el cálculo de las distancias de cada punto hacia un cluster según FuzzyCMeans
p = float(2/(m-1))


global listadc #Lista que guardará los centroides
listadc=[]


def MatrizdeMembresia():
    '''Generación aleatoria de matriz de membresía inicial. Indica k valores para cada punto'''
    MatrizMembresia = []

    for i in range(n):
        random_num_list = [random.random() for i in range(k)]
        sumatoria = sum(random_num_list)
        listat = [x/sumatoria for x in random_num_list]

        flag = listat.index(max(listat))
        for j in range(0,len(listat)):
            if(j == flag):
                listat[j] = 1
            else:
                listat[j] = 0

        MatrizMembresia.append(listat)

    '''Se guardará la lista MatrizMembresia en un archivo txt para posteriormente actualizar los valores'''

    f=open(archivo_membresia, 'w')
    for i in MatrizMembresia:
        a=i[0]
        b=i[1]
        c=i[2]
        w=float(a),float(b),float(c)
        w=str(w)
        w=w.replace('(','')
        w=w.replace(')','')
        f.write(w+'\n')
    f.close()

    return MatrizMembresia


def get_centroides(MatrizMembresia):
    ''' Se agrupan los grados de pertenencia para cada cluster y se genera una lista '''
    cluster_mem_val = list(zip(*MatrizMembresia))
    cluster_centers = []

    '''Recuperar los datos (puntos) del txt para la generación de centroides iniciales'''

    f=open(archivo_datos,'r')
    puntos = [[float(num) for num in line.split(",")] for line in f]
    f.close()

    ''' Se calcularán los valores de los centroides tentativos por clúster'''
    for j in range(k):
        x = list(cluster_mem_val[j])
        xraised = [p ** m for p in x]
        denominator = sum(xraised)
        temp_num = []

        for i in range(n):
            punto = puntos[i]
            prod = [xraised[i] * val for val in punto]
            temp_num.append(prod)

        numerator = map(sum, list(zip(*temp_num)))
        center = [z/denominator for z in numerator]
        cluster_centers.append(center)
    '''Se generan los centroides iniciales para poder luego actualizarlos a través del cálculo de las distancias y optimización
    de centroides por iteraciones'''

    f=open(archivo_centroides, 'w')
    for i in cluster_centers:
        a=i[0]
        b=i[1]
        c=i[2]
        d=i[3]
        w=float(a),float(b),float(c),float(d)
        w=str(w)
        w=w.replace('(','')
        w=w.replace(')','')
        f.write(w+'\n')
    f.close()
    '''Guardar los centroides en un archivo txt'''
    return cluster_centers

def actualizar_membresias():
    '''Método para guardar los grados de membresía obtenidos a partir del cálculo de los centroides en mapreduce'''

    f=open(archivo_membresia, 'r')
    MatrizMembresia = []
    for line in f.read().split('\n'):
         if line:
            x, y, z= line.split(', ')
            MatrizMembresia.append([float(x),float(y),float(z)])
    f.close()

    f=open(archivo_centroides, 'r')
    centroides = []
    for line in f.read().split('\n'):
         if line:
            a,b,c,d= line.split(', ')
            centroides.append([float(a),float(b),float(c),float(d)])
    f.close()

    f=open(archivo_datos,'r')
    puntos = [[float(num) for num in line.split(",")] for line in f]
    f.close()

    for i in range(n):
        punto = puntos[i]
        distances = [np.linalg.norm(np.array(list(map(operator.sub, punto, centroides[j])))) for j in range(k)]
        for j in range(k):
            den = sum([math.pow(float(distances[j]/distances[c]), p) for c in range(k)])
            MatrizMembresia[i][j] = float(1/den)

    '''Guardar los valores en el archivo txt de membresías'''
    f=open(archivo_membresia, 'w')

    for i in MatrizMembresia:
        a=i[0]
        b=i[1]
        c=i[2]
        w=float(a),float(b),float(c)
        w=str(w)
        w=w.replace('(','')
        w=w.replace(')','')
        f.write(w+'\n')

    f.close()
    return MatrizMembresia


def distancia(v1, v2):
    '''Método para calcular distancia entre dos puntos'''
    d0=v2[0] - v1[0]
    d1=v2[1] - v1[1]
    d2=v2[2] - v1[2]
    d3=v2[3] - v1[3]

    distancia=sqrt(d0*d0 + d1*d1 + d2*d2 + d3*d3)

    return distancia

def diferencia (c1,c2):
    '''Método para hallar la diferencia de la distancia entre dos puntos diferentes'''
    dist_max=0.0
    for i in range (len(c1)):
        dist=distancia(c1[i],c2[i])
        if dist > dist_max:
            dist_max=dist

    return dist_max


''' Se declara la clase FuzzyCMeans que ejecutará el algoritmo heredando la clase MrJob'''
class FuzzyCMeans(MRJob):

    def steps(self):
        '''Se definen el orden de la ejecución de los pasos'''

        return[
            MRStep(mapper=self.mapper_fuzzy,
                 combiner=self.combiner_fuzzy,
                 reducer=self.reducer_fuzzy)
                   ]

    def guarda_centroides(centroides):
        '''Método para guardar los centroides en el archivo txt'''
        f=open(archivo_centroides, 'w')
        for i in centroides:
            a=i[0]
            b=i[1]
            c=i[2]
            d=i[3]
            w=float(a),float(b),float(c),float(d)
            w=str(w)
            w=w.replace('(','')
            w=w.replace(')','')
            f.write(w+'\n')
        f.close()


    def mapper_fuzzy(self, _, line):

        clusters_labels = 0

        '''Lectura de puntos como lista'''
        a,b,c,d = line.split(';')
        punto = [float(a),float(b),float(c),float(d)]

        '''Lectura de archivo de valores de membresía iniciales'''
        f=open(archivo_membresia,'r')
        MatrizMembresia=[]
        for line in f.read().split('\n'):
            if line:
                a, b, c= line.split(',')
                MatrizMembresia.append([float(a), float(b), float(c)])
        f.close()

        '''Lectura de centroides iniciales'''
        f=open(archivo_centroides,'r')
        centroides=[]
        for line in f.read().split('\n'):
            if line:
                a,b,c,d= line.split(', ')
                centroides.append([float(a), float(b), float(c), float(d)])
        f.close()

        '''Cálculo de distancia de cada punto para cada centroide de cluster'''

        distances = [np.linalg.norm(np.array(list(map(operator.sub, punto, centroides[j])))) for j in range(k)]
        lista_values = []
        for j in range(k):
            '''Se calcularán los grados de membresía para cada punto'''
            den = sum([math.pow(float(distances[j]/distances[c]), p) for c in range(k)])
            matriz_value = float(1/den)
            lista_values.append(matriz_value)

        '''Se otorga una etiqueta para cada punto a partir del mayor grado de membresía obtenido'''

        max_val, idx = max((val, idx) for (idx, val) in enumerate (lista_values))
        clusters_labels=idx


        yield clusters_labels, punto


    def combiner_fuzzy(self, clusters_labels, puntos):

        '''Combiner: Para cada salida del mapper (clusters_labels,punto) se calcula el punto
        medio (centroide) de cada clase'''

        count= 0
        m_x = 0.0
        m_y = 0.0
        m_w = 0.0
        m_z = 0.0

        for t in puntos:
            count+= 1
            m_x+= t[0]
            m_y+= t[1]
            m_w+= t[2]
            m_z+= t[3]

        punto_medio = (m_x/count, m_y/count, m_w/count, m_z/count)

        yield clusters_labels , punto_medio


    def reducer_fuzzy(self, clusters_labels , punto_medio):

        '''Reducer: Para cada clase, obtenemos los centroides temporales
        de cada salida del combiner y calculamos los nuevos centroides'''

        count = 0
        m_x = 0.0
        m_y = 0.0
        m_w = 0.0
        m_z = 0.0

        for t in punto_medio:

            count += 1
            m_x += t[0]
            m_y += t[1]
            m_w += t[2]
            m_z += t[3]

        centroide_x = m_x / count
        centroide_y = m_y / count
        centroide_w = m_w / count
        centroide_z = m_z / count
        centros=[centroide_x,centroide_y, centroide_w, centroide_z]

        '''Se guardan los nuevos centroides en la listadc'''
        listadc.append(centros)

        yield clusters_labels, centros


if __name__=='__main__':
    '''Se calculan primero los centroides iniciales para ejecutar el algoritmo en MrJob'''
    MatrizMembresia=MatrizdeMembresia()
    primer_centroide=get_centroides(MatrizMembresia)
    '''Se definen parámetros'''
    i=0 #Contador de iteraciones
    dist_max=1   #Distancia máxima definida

    '''Se busca la convergencia de los centroides'''
    while dist_max > 0.4:
        FuzzyCMeans.run()
        nuevo_centroide = listadc
        listadc = []
        FuzzyCMeans.guarda_centroides(nuevo_centroide)
        dist_max = diferencia(nuevo_centroide, primer_centroide)
        primer_centroide=nuevo_centroide
        i+=1
    print('Se alcanzó la convergencia')
    print('Fueron necesarias '+str(i)+' iteraciones')
    '''Se actualizarán los valores de membresía en el archivo txt'''
    MatrizMembresia=actualizar_membresias()
    print('¡Matriz actualizada!')
    print('Fin')

#python MrJobFuzzy_Convergencia_Final1.py datos_pca.csv
