Scalable and Cloud Programming Project A.A 2020/2021

Implementazione in Scala-Spark di un sistema di raccomandazione di film utilizzando diversi metodi.
I metodi utilizzati sono:

-PlotBased: vengono consigliati i film con la trama più simile al film/ai film preferito/i dall'utente. 
Per determinare la similarità tra due trame viene usato il tf-idf.

-ContentBased: Vengono consigliati i film con le caratteristiche (anno, genere, cast e regista) più simili al film/ai film preferito/i dall'utente.
Per determinare la similarità tra caratteristiche viene usato un metodo simile al dot product pesato.

-Collaborative: vengono consigliati i film che sono stati apprezzati dagli utenti più simili.
La similarità tra due utenti è calcolata attraverso il metodo del Coseno (o alternativamente il Pearson), mentre la valutazione del rating atteso è fatto mediante tre diversi metodi di aggregazione.

-RatingBased: vengono consigliati i film più apprezzati che soddisfano i gusti di genere dell'utente.

I dataset usati sono:
1. Movielens Dataset (https://grouplens.org/datasets/movielens/)
2. Imdb Dataset (https://www.kaggle.com/stefanoleone992/imdb-extensive-dataset?select=IMDb+ratings.csv)
