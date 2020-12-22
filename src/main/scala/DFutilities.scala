import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataType, DoubleType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.parallel.mutable.ParArray

object DFutilities {
  //percorso locale dei dataset
  val bucket="D:\\ME\\universit\\raccomandazione_film\\usati\\"
  //percorso s3 dei dataset
  //val bucket= "s3://aws-logs-692247210354-us-east-1/"
  val ratingsFile1 = bucket+"IMDbmovies.csv"
  //RATINGFILE CON 1MLN DI RECENSIONI
  //val ratingsFile2 = bucket+"ratingoneMilion.csv"
  val ratingsFile2 = bucket+"ratings_small.csv"
  val ratingsFile3 = bucket+"links.csv"
  val ratingsFile4 = bucket+"movies.csv"
  val ratingsFile5 = bucket+ "IMDbratings.csv"
  val caratteriMinimi=30
  val NumeroFilm=2  //quanti miglori film tornare
  val ParForExecutor=3

  //Caricamento dei dataset
  def CaricamentoFile(spark: SparkSession, Path: String): DataFrame={
    spark.read.format("csv").option("header", "true").load(Path)
  }

  //Usato per castare il tipo statico di una colonna di un dataframe
  def CastColumnTo( df: DataFrame, cn: String, tpe: DataType ) : DataFrame = {
    df.withColumn( cn, df(cn).cast(tpe) )
  }

  //Arrotondamento alla seconda cifra decimale
  def Arrotonda(num: Float)={
    (num*100).round / 100.toFloat
  }

  //Estrae da Imdb movie solo le caratteristiche dei film non visti dall'utenteX
  def OttieniImdbFeatureRDD(FilmUtente: Array[String], imdbData:  RDD[(String, (String, Int, Array[String], String, Array[String], String))]):
    ( RDD[(String, (String, String))], RDD[(String, (String, Int, Array[String], String, Array[String]))]) ={
    val imdbFinale= imdbData.filter(row=> !FilmUtente.contains(row._1)).cache()
    val ImdbTrama=imdbFinale.map(x=>(x._1, (x._2._6, x._2._1))).cache()
    val IMDbFeature=imdbFinale.map(x=>(x._1, (x._2._1, x._2._2,x._2._3, x._2._4,x._2._5))).cache()
    imdbFinale.unpersist()
    (ImdbTrama, IMDbFeature)
  }

  //Estra informazioni sull'utente, quali film visti, voti dati ad essi e caratteristiche per ogni film
  def EstraiUtenteX(ratings: RDD[(String, (String, Double))],
                    imdb: RDD[(String, (String, Int, Array[String], String, Array[String], String))],
                    link: RDD[(String, String)], utenteMio: String):
      (Array[String], ParArray[((String, Double), (String, Int, Array[String], String, Array[String]), String)],
      RDD[(String, (String, Double))], Map[String, Double]) ={

    val RatingsUtenteX=  ratings
                        .filter(row=> row._1.equals(utenteMio))
                        .map(row=> row._2).collect().toMap.par
    // AltriUtenti: RDD relativo ai film visti dagli utenti
    val AltriUtenti= ratings
                    .filter(row=> row._1!=utenteMio).cache()

    val linkColl= link.filter(row=> RatingsUtenteX.contains(row._1)).map(row=> (row._2, row._1)).collect().toMap.par
    val imdbColl= imdb.filter(row=> linkColl.contains(row._1)).collect().toMap.par
    //UtenteX= Array con tutte le informazioni
    val UtenteX= imdbColl.map(elem=>
      ((elem._1, RatingsUtenteX(linkColl(elem._1))),(elem._2._1, elem._2._2, elem._2._3, elem._2._4, elem._2._5), elem._2._6 )).toArray
      .sortWith((a, b)=>a._1._2>b._1._2)

    ( UtenteX.map(elem=> elem._1._1),
      UtenteX.take(NumeroFilm).par,
      AltriUtenti,
      UtenteX.map(elem=> elem._1).toMap
    )
  }

  //Caricamento e inizializzazione dei dati su cui lavorare
  def InizializzazioneDataset(SC: SparkContext, SS: SparkSession)={
    import SS.implicits._
    val IMDbData = DFutilities.CaricamentoFile(SS, ratingsFile1)
            .select("imdb_title_id","title", "year", "genre", "director", "actors", "description")
            .filter(row=> row(0)!=null && row(1)!=null && row(2)!=null && row(3)!=null && row(4)!=null && row (5)!=null && row(6)!=null && row(6).toString.size>caratteriMinimi)
            .rdd.map(row=> (row(0).toString, (row(1).toString, try{row(2).toString.toInt} catch {case e: Exception => 0 },
            (row(3).toString).split(",").map(elem=>elem.trim), row(4).toString,
            ((row(5).toString).split(",")).map(elem=>elem.trim), row(6).toString)))

    val ratingsData = CaricamentoFile(SS, ratingsFile2).drop("timestamp")
    val RatingsFloat = CastColumnTo(ratingsData, "rating", DoubleType)
                      .rdd
                      .map(row=> (row(0).toString, (row(1).toString, row(2).asInstanceOf[Double])))
                      .partitionBy(new HashPartitioner(SC.getExecutorMemoryStatus.size*ParForExecutor))

    val LinksData=DFutilities.CaricamentoFile(SS, ratingsFile3)
                  .drop("tmdbId")
                  .rdd
                  .map(row=> (row(0).toString, "tt"+row(1).toString))

    val Review = DFutilities.CaricamentoFile(SS, ratingsFile5)
                .select("imdb_title_id", "total_votes", "votes_10",
                  "votes_9", "votes_8", "votes_7", "votes_6", "votes_5", "votes_4", "votes_3", "votes_2", "votes_1")
                .rdd
                .map(row=> (row(0).toString, (row(1).toString.toInt, ParArray(row(11).toString.toInt, row(10).toString.toInt,row(9).toString.toInt,
                  row(8).toString.toInt,  row(7).toString.toInt, row(6).toString.toInt,
                  row(5).toString.toInt,  row(4).toString.toInt, row(3).toString.toInt,
                  row(2).toString.toInt)))).join(IMDbData.mapValues(row=> (row._3, row._1)))

    val MovieData=  DFutilities.CaricamentoFile(SS, ratingsFile4)
                    .drop("genres")
                    .map(riga=> (riga(0).toString, riga(1).toString)).collect().toMap

    (IMDbData, RatingsFloat, LinksData, Review, MovieData)
  }


}
