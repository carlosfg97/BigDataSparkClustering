# Abordaje o metodología

Partiendo de la descripción de la problemática y el objetivo propuesto, se ha determinado la siguiente metodología para el desarrollo del programa: El preprocesamiento de datos empleando Spark, la construcción del algoritmo Fuzzy C Means empleando MRJob en python, y finalmente, la visualización y presentación de resultados obtenidos. A continuación se detalla cada fase de la metodología.

### 1. Preprocesamiento de datos con PySpark en Databricks
En esta primera etapa ingresan los registros de la base de datos de clientes de un e-commerce y se eliminan los registros nulos y duplicados. Además, las órdenes canceladas fueron eliminadas de la base de datos al igual que sus contrapartidas. 

Se realiza cierta ingeniería de variables como por ejemplo llevar la descripción de productos a su representación vectorial y posteriormente generar 5 categorías de productos con un KMeans. Se agrupan las órdenes de compra por cliente para obtener una sola fila por cliente y se consideran las siguientes variables: cantidad de órdenes, fecha de la primera compra, fecha de la última compra, el monto total, mínimo, máximo y promedio de sus compras, y los montos asignados para cada una de las cinco categorías de productos. 

Posteriormente, se estandarizan las variables. Debido al número de variables que cuenta la base de datos, se emplea la técnica PCA para reducir a cuatro dimensiones, a partir de dicho resultado, el vector obtenido se divide en cuatro columnas y es renombrado. Finalmente, la base de datos limpia es exportada en formato csv.

### 2. Desarrollo del algoritmo Fuzzy C Means

En esta etapa se trabajará con los datos obtenidos en el preprocesamiento, los cuales se recuperan como datos de entrada para el algoritmo de clasificación Fuzzy C Means. Primero se define una matriz aleatoria de membresía que se actualizará al final de la ejecución del algoritmo. Esta matriz tiene una dimensión del total de filas por el número de grupos a segmentar (3). A continuación, se definirán los centroides iniciales a partir de los datos de entrada. Estos centroides serán posteriormente actualizados para obtener una convergencia de distancia mínima entre el centroide de cada cluster y los puntos con mayor grado de membresía. 

Dentro del algoritmo se calcularán los centroides representativos de cada clúster a través de la técnica MapReduce, con la finalidad de procesar paulatina y ordenadamente los datos de entrada. En el mapper se realizará un split de los datos y a continuación, para cada punto o registro de cliente se etiquetará la pertenencia a un cluster (será aquel que otorgue una mayor grado de membresía o pertenencia a un grupo específico). Luego, en el combiner se reciben como entrada las salidas del mapper: los puntos del path de datos junto a su label de cluster asignado. Lo que se busca es encontrar el promedio de puntos para cada grupo de datos generado. Finalmente, a través del reducer, una vez promediados los valores de cada grupo, se generan los centroides tentativos para el path de datos. El algoritmo desarrollado seguirá un proceso iterativo hasta lograr la convergencia.

### 3. Visualización de resultados

En esta última etapa, ingresa el dataset con los grados de membresía resultante del algoritmo Fuzzy C Means en MrJob y se crea una nueva columna “prediction” para indicar el cluster con mayor grado de membresía por cada cliente. Luego, se concatena el dataset con las variables originales para permitir el análisis de los clusters. Finalmente, se generan gráficas de barras, radar plots e histogramas para realizar un análisis sobre el comportamiento de compra para cada grupo de clientes. También se ubican los centroides resultantes en un plano con ayuda de la técnica t-SNE y se obtiene un indicador de los clusters (Silhouette Score)
