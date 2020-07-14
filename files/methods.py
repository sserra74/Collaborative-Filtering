# coding=utf-8
import os
from pprint import pprint
#import findspark
#findspark.init()
import math

#Metodo che permette di ottenere un RDD dai dati letti dal file csv
def ObtainRDD_Ratings(sc,n_partitions):

    
    
        #Creo l'RDD dal file csv identificato tramite il percorso contenuto in S3
        #ratings_data_temp=sc.textFile("ratings.csv",n_partitions)
        ratings_data_temp = sc.textFile("s3://bucketbigdataemr/files/ratings.csv", n_partitions)
        #Otteniamo l'header del file ratings.csv
        ratings_data_header=ratings_data_temp.take(1)[0]
        #print(ratings_data_header)


        #Otteniamo un nuovo RDD escludendo l'header, splittando le righe con una virgola e prendendo le prime tre colonne
        #Andando a escludere il timestamp che non ci interessa per questo tipo di analisi.
        #Il metodo cache permette di rendere disponibile in memoria l'RDD senza doverlo leggere nuovamente
        ratings_data = ratings_data_temp.filter(lambda line: line!=ratings_data_header)\
            .map(lambda line: line.split(",")).map(lambda tokens: (tokens[0],tokens[1],tokens[2])).cache()

        #Restituiamo i due RDD
        return ratings_data

#Metodo che permette di ottenere un RDD dai dati letti dal file csv
def ObtainRDD_Movies(sc,n_partitions):



        # Creo l'RDD dal file csv identificato tramite il percorso contenuto in S3
        #movie_data_temp = sc.textFile("movies.csv",n_partitions)
        movie_data_temp = sc.textFile("s3://bucketbigdataemr/files/movies.csv", n_partitions)
        # Otteniamo l'header del file movies.csv
        movie_data_header = movie_data_temp.take(1)[0]
        #print(movie_data_header)

        #Otteniamo un nuovo RDD escludendo l'header, splittando le righe con una virgola e prendendo le prime tre colonne
        #Il metodo cache permette di rendere disponibile in memoria l'RDD senza doverlo leggere nuovamente
        movie_data = movie_data_temp.filter(lambda line: line != movie_data_header) \
            .map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]),tokens[1],tokens[2])).cache()

        movie_titles= movie_data.map(lambda x: (int(x[0]), x[1]))


        #Restituiamo i due RDD e l'RDD con soli i titoli dei movie
        return movie_data,  movie_titles


#Per applicare il collaborative-filtering, è necessario creare un nuovo utente per ottenere le raccomandazioni
#su nuovi film sulla base di interessi in comune con altri utenti
def DefNewUser(sc):
    #Utilizzaimo come id 0 perchè è l'unico che non è presente nel dataset
    new_user_ID = 0

    # Aggiungiamo informazioni del tipo (userID, movieID, rating), dove avremo per l'utente 0 il voto che ha dato
    # su un particolare film 
    new_user_ratings = [

    (0,260,4),
     (0,1,3), 
     (0,16,3),
     (0,25,4),
     (0,32,4), 
     (0,335,1),
     (0,379,1),
     (0,296,3),
     (0,858,5) , 
     (0,50,4) 

    ]


    """
    (0, 158, 1.5),
    (0, 160, 2.0),
    (0, 161, 4.0),
    (0, 163, 1.5),
    (0, 165, 5.0),
    (0, 169, 1.0),
    (0, 170, 3.5),
    (0, 353, 3.5),
    (0, 364, 2.0),
    (0, 367, 3.0),
    (0, 380, 2.5),
    (0, 420, 3.5),
    (0, 434, 1.5),
    (0, 440, 3.0),
    (0, 442, 4.0),
    (0, 454, 4.5),
    (0, 457, 3.0),
    (0, 515, 5.0),
    (0, 552, 3.5),
    (0, 555, 2.5),
    (0, 741, 4.5),
    (0, 745, 4.0),
    (0, 750, 4.5),
    (0, 837, 3.0),
    (0, 902, 5.0),
    (0, 903, 4.5),
    (0, 904, 5.0),
    (0, 908, 4.0),
    (0, 910, 4.0),
    (0, 912, 4.5),
    (0, 913, 5.0),
    (0, 916, 4.5),
    (0, 318, 4.0),
    (0, 356, 5.0),
    (0, 296, 3.5),
    (0, 593, 4.5),
    (0, 2571, 3.0),
    (0, 480, 3.0),
    (0, 527, 4.0),
    (0, 110, 5.0),
    (0, 1210, 3.5),
    (0, 1196, 4.5),
    (0, 50, 5.0),
    (0, 4993, 3.0),
    (0, 858, 3.5),
    (0, 2858, 2.0),
    (0, 780, 3.0),
    (0, 150, 4.0),
    (0, 457, 5.0),
    (0, 1270, 3.0),
    (0, 7153, 4.0),
    (0, 5952, 3.0),
    (0, 1091, 2.0),
    """

    #Creiamo l'RDD con dati con cui si può lavorare in parallelo
    new_user_ratings_RDD = sc.parallelize(new_user_ratings)

    #Restituiamo quest'ultimo RDD, l'ID dell'utente e la lista di valutazioni da egli effettuate
    return new_user_ratings_RDD, new_user_ID, new_user_ratings

def FilterMovie(newUser_unrated_movies_RDD, newUser_movies_rated, movies_data ):
    cont = 0
    for y in list(newUser_movies_rated):
        #print(y)
        if cont == 0:
            newUser_unrated_movies_RDD = movies_data.filter(lambda x: x[0] != y).cache()
            cont = cont + 1
            # pprint(newUser_unrated_movies_RDD.take(18))
        else:
            newUser_unrated_movies_RDD = newUser_unrated_movies_RDD.filter(lambda x: x[0] != y).cache()
            # pprint(newUser_unrated_movies_RDD.take(18))
    return newUser_unrated_movies_RDD



def GetRealAndPreds(final_model, final_test, mode):
    test_for_predict_RDD = final_test.map(lambda x: (x[0], x[1]))
    predictions = final_model.predictAll(test_for_predict_RDD).map(lambda r: ((r[0], r[1]), round(r[2]*2)/2))

    rates_and_preds = final_test.map(lambda r: ((int(r[0]), int(r[1])), float(r[2]))).join(predictions)
    error = math.sqrt(rates_and_preds.map(lambda r: (r[1][0] - r[1][1]) ** 2).mean())

    print('For testing data the RMSE is %s' % (error))
    mse = rates_and_preds.map(lambda r: (r[1][0] - r[1][1]) ** 2).reduce(
        lambda x, y: x + y) / rates_and_preds.count()
    print('For testing data the MSE is %s' % (mse))
    if mode == 2:
        print(rates_and_preds.take(rates_and_preds.count()))

