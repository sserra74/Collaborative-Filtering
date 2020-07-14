# coding=utf-8
from pprint import pprint
from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import SQLContext
import sys
import time

#Inizio tempo di esecuzione
startTime=time.time()

#Spark richiede una configurazione per far si che l'applicazione venga eseguita dal cluster
conf=SparkConf().setAppName("MoviesReccomandations baseline")
#Quando eseguiamo un'applicazione Spark, viene avviato un Contesto Spark che permette di eseguire le 
#operazioni all'interno degli esecutori. sc conterrà il contesto della nostra applicazione Spark
sc=SparkContext(conf=conf)
#Visualizza solo gli errori
sc.setLogLevel("ERROR")
#Apriamo un contesto SQL per eseguire le query tramite l'applicazione Spark
sqlContext=SQLContext(sc)


#Prendiamo il tempo di inizio esecuzione
startTime=time.time()

#n° partizioni in cui splittare l'RDD. sys permette di prendere il parametro specificato nello step "Run Baseline"
n_partitions = int(sys.argv[1])

#Creo l'RDD dal file csv identificato tramite il percorso contenuto in S3 specificando anche il n° di partizioni
#in cui l'RDD verrà splittato
ratings_data_temp=sc.textFile("s3://bucketbigdataemr/files/ratings.csv",n_partitions)

#Otteniamo l'header del file ratings.csv
ratings_data_header=ratings_data_temp.take(1)[0]
#print(ratings_data_header)


#Otteniamo un nuovo RDD escludendo l'header, splittando le righe con una virgola e prendendo il movie_id e il rating
#ad esso associato andando a escludere il timestamp e l'user_id che non ci interessano per questo tipo di analisi
ratings_data = ratings_data_temp.filter(lambda line: line!=ratings_data_header)\
    .map(lambda line: line.split(",")).map(lambda tokens: (int (tokens[1]),float(tokens[2]))).cache()
#Avremo tuple del tipo <UserId,MovieId, Rating>
#pprint(ratings_data.take(10))
#pprint("------------------")

# Creo l'RDD dal file csv identificato tramite il percorso contenuto in S3 specificando anche il n° di partizioni
#in cui l'RDD verrà splittato
movie_data_temp = sc.textFile("s3://bucketbigdataemr/files/movies.csv", n_partitions)

# Otteniamo l'header del file movies.csv
movie_data_header = movie_data_temp.take(1)[0]

# #Otteniamo un nuovo RDD escludendo l'header, splittando le righe con una virgola e prendendo le prime tre colonne
movie_data = movie_data_temp.filter(lambda line: line != movie_data_header) \
    .map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]), tokens[1], tokens[2])).cache()

#Otteniamo un RDD con soli i movie che tra i generi hanno Mystery
movie_data = movie_data.filter(lambda x: "Mystery" in x[2]).cache()

#Dopo aver eseguito il join dei due RDD, lo mappiamo scalando le tuple di un livello
temp_RDD=ratings_data.join(movie_data).map(lambda x: (x[0], ) +x[1])

#Per eseguire la media dei ratings per film, l'RDD è stato temporaneamente convertito in DataFrame per facilitare
#l'esecuzione del calcolo
df_movie=temp_RDD.toDF(schema=["movie_id", "rating", "title"])
df_avg=df_movie.groupby("title").avg("rating")

#Riconvertiamo il DataFrame in RDD andando a prendere i primi 25 movie secondo un rating discendente
#(-x[1], dove meno indica l'ordinamento decrescente e x[1] la colonna rating della tupla x)
topMistery_movies=df_avg.rdd.takeOrdered(25, key=lambda x: -x[1])
print('Top Movie Recommandation:\n%s' % '\n'.join(map(str, topMistery_movies)))
print('---- %s seconds ----' % (time.time()-startTime))
