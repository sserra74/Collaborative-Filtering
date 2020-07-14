# coding=utf-8
from methods import *
from pprint import pprint
#import findspark
#findspark.init()

#Libreria per allenare il modello
from pyspark.mllib.recommendation import ALS

from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import SQLContext

import pyspark
import time
import sys
from operator import add

#Spark richiede una configurazione per far si che l'applicazione venga eseguita dal cluster
conf=SparkConf().setAppName("MoviesReccomandations")
#Quando eseguiamo un'applicazione Spark, viene avviato un Contesto Spark che permette di eseguire le 
#operazioni all'interno degli esecutori. sc conterrà il contesto della nostra applicazione Spark
sc=SparkContext(conf=conf)
#Visualizza solo gli errori
sc.setLogLevel("ERROR")
#Apriamo un contesto SQL per eseguire le query tramite l'applicazione Spark
sqlContext=SQLContext(sc)


#Prendiamo il tempo di inizio esecuzione
startTime=time.time()

#n° di partizioni in cui verrà splitato l'RDD. sys prende il parametro specificato nello step Run Code o Run Baseline
n_partitions = int(sys.argv[1])

#Otteniamo i due RDD dai ratings passando come parametro lo SparkContext e il numero di partizioni con cui verrà splittao l'RDD
ratings_data =ObtainRDD_Ratings(sc,n_partitions)
#Avremo tuple del tipo <UserId,MovieId, Rating>
#pprint(ratings_data.take(10))
#pprint("------------------")

#Otteniamo i due RDD dai movies e l'RDD dei titoli passando come parametro lo SparkContext e il numero di partizioni con cui verrà splittao l'RDD
movies_data, movie_titles=ObtainRDD_Movies(sc,n_partitions)
#Avremo tuple del tipo <MovieId,Title, Genres>
#pprint(movies_data.take(400))


#RDD contenente per ogni movie il numero di rating associato. ReduceByKey è stato utilizzato per ridurre lo shuffle
IDRatings = ratings_data.map(lambda row: (int(row[1]), 1)).reduceByKey(add)
#pprint(IDRatings.takeOrdered(25, key=lambda x: x[1]))
#pprint(IDRatings.take(10))



#Per applicare il collaborative-filtering, è necessario creare un nuovo utente per ottenere le raccomandazioni
#su nuovi film sulla base di interessi in comune con altri utenti
user_ratings, user_ID, list_user_ratings =DefNewUser(sc)
#print(list_user_ratings)


training_final, test_final=ratings_data.randomSplit([8,2], seed=42)
training_userRatings,test_userRatings=user_ratings.randomSplit([5,5], seed=42)

final_test=test_final.union(test_userRatings)
final_training=training_final.union(training_userRatings)
#Ora rialleniamo il modello con i nuovi rating e con i parametri di default
new_ratings_model = ALS.train(final_training, 20, seed=5, iterations=10, lambda_=0.1)
GetRealAndPreds(new_ratings_model, final_test, 1)

#Lo applichiamo una volta sui film che l'utente non ha visto e una volta su  quelli che ha visto per fare i confronti e vedere se il modello sta
#predicendo bene

#Reccomandations
#Prendiamo l'id dei movie che l'utente ha valutato
newUser_movies_rated= map(lambda x: x[1], list_user_ratings)

newUser_unrated_movies_RDD=sc.emptyRDD()
#Filtriamo i film in un nuovo RDD che conterrà quei film non valutati dall'utente
newUser_unrated_movies_RDD=FilterMovie(newUser_unrated_movies_RDD, newUser_movies_rated, movies_data)
newUser_unrated_movies_RDD = newUser_unrated_movies_RDD.map(lambda x: (user_ID, x[0]))

#Utiliziamo il modello allenato con i rating originali per predirre i nuovi ratings sui film non votati dall'utente
#Questo RDD conterrà quindi le predizioni previste per il nuovo utente con tuple del tipo <UserID,movieId,Rating>
newUser_moviePrediction_RDD = new_ratings_model.predictAll(newUser_unrated_movies_RDD)

#Dal precedente RDD prendiamo solo l'ID del film e il suo rating
newUser_moviePrediction_rating_RDD = newUser_moviePrediction_RDD.map(lambda x: (x.product, x.rating))
#pprint(newUser_moviePrediction_rating_RDD.take(3))

#Eseguendo i join tra l'RDD precedente, quello dei titoli dei film e quello del conteggio dei ratings, otteniamo un RDD
#con tuple del tipo <movieID,<ratings predetto, nome del film>,countRatings>
newUser_join_recommendations_RDD = \
    newUser_moviePrediction_rating_RDD.join(movie_titles).join(IDRatings)

#Riorganizzazione delle tuple con un livello in meno. Avremo tuple del tipo <Nome Film, Predizione Rating, Count di rating per quel film>
new_final_recommendations_RDD = \
    newUser_join_recommendations_RDD.map(lambda r: (r[1][0][1], r[1][0][0], r[1][1]))
#pprint(new_final_recommendations_RDD.take(4))

#Prendiamo le prime 25 migliori raccomandazioni con 25 o più review in ordine decrescente
#(-x[1], con x[1] che equivale al campo rating e - che indica l'ordinamento decrescente)
top_movies = new_final_recommendations_RDD.filter(lambda r: r[2]>=25).takeOrdered(25, key=lambda x: -x[1])


print('Top Movie Recommandation:\n%s' % '\n'.join(map(str, top_movies)))
print('---- %s seconds ----' % (time.time()-startTime))
#Fine nuova parte

#Altra nuova parte: modello sui film che l'utente ha visto. APPLICHIAMO LO STESSO MODELLO
GetRealAndPreds(new_ratings_model, test_userRatings, 2)


#QUERY



#Prendiamo il tempo corrente
#startTime=time.time()

#Definiamo un dataframe dagli RDD e una vista temporanea che è possibile utilizzare come tabella e che non persiste in memoria 
#Per salvarla in memoria sarebbe necessario utilizzare il metodo .cache() come fatto per gli RDD
"""schemaMovies=sqlContext.createDataFrame(movies_data)
schemaMovies.createOrReplaceTempView("movies")

schemaRatings=sqlContext.createDataFrame(ratings_data)
schemaRatings.createOrReplaceTempView("ratings")


#pprint("-------------------------MOVIES WITH MYSTERY GENRES ----------------------------")
query_misteryMovies=sqlContext.sql("SELECT movies._2 FROM movies WHERE movies._3 LIKE '%Mystery%'")
#pprint(query_misteryMovies.take(query_misteryMovies.count()))

#print("-------------------------MOVIES of 2018----------------------------")

query_misteryMovies=sqlContext.sql("SELECT movies._2 FROM movies WHERE movies._2 LIKE '%(2018)%' ")
#pprint(query_misteryMovies.take(query_misteryMovies.count()))


#pprint("-------------------------MOVIES BETWEEN 2016 AND 2018-----------------------------------")
query_misteryMovies=sqlContext.sql("SELECT movies._2 FROM movies WHERE movies._2 LIKE '%(2016)%' OR movies._2 LIKE '%(2018)%' ")
#pprint(query_misteryMovies.take(query_misteryMovies.count()))

#pprint("-------------------------MOVIES WITH RATINGS 5-------------------------------")
query_join=sqlContext.sql("SELECT DISTINCT movies._2 FROM movies JOIN ratings ON movies._1=ratings._2 WHERE ratings._3=5.0")
#pprint(query_join.take(query_join.count()))

#pprint("--------------------MOVIES OF FANTASY AND ADVENTURE GENRES AND WITH RATINGS 5-------------------------------")
query_join=sqlContext.sql("SELECT DISTINCT movies._2 FROM movies JOIN ratings ON movies._1=ratings._2 WHERE movies._3 LIKE '%Fantasy%' AND movies._3 LIKE '%Adventure%' AND ratings._3=5.0")
#pprint(query_join.take(query_join.count()))

#pprint("--------------------MOVIES WITH RATINGS 1 OR 2 AND OF 1995-------------------------------")
query_join=sqlContext.sql("SELECT movies._2 FROM movies JOIN ratings ON movies._1=ratings._2 WHERE ratings._3=1 OR ratings._3=2 AND movies._2 LIKE '%(1995)%'")
#pprint(query_join.take(query_join.count())

#pprint("--------------------MOVIES AND RATINGS IN A DESCENDING ORDER-------------------------------")
query_join=sqlContext.sql("SELECT DISTINCT movies._2, ratings._3 FROM movies JOIN ratings ON movies._1=ratings._2 ORDER BY ratings._3 DESC")
#pprint(query_join.take(query_join.count()))"""

#tempo di esecuzione
#print('---- %s seconds ----' % (time.time()-startTime)

