import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object PlotBased {
  @transient lazy val Ss: SparkSession = SparkSession
                                        .builder()
                                        .getOrCreate()
  import Ss.implicits._

  //Classe che implementa un metodo trasform il quale si occupa di rimuovere le stop-word
  val remover = new StopWordsRemover()
                .setInputCol("parole")
                .setOutputCol("filtered")

  def Lemmatizza(trama: String, lemmatizzatore: Map[String, String]): Array[String] = {
    //ParoleTrama= Array, i cui elementi sono le parole della trama a meno di punteggiatura
    val ParoleTrama = trama
                      .replaceAll("""[\p{Punct}]""", "")
                      .toLowerCase
                      .split(" ")

    // Per ogni parola in ParoleTrama cerchiamo il lemma corrispondente grazie al lemmatizzatore
    ParoleTrama.map(x => lemmatizzatore.get(x) match {
      case None => x
      case Some(y) => y
      }
    )
  }

  def ValutaTrama(tramaA: Array[(String, Int)], tramaB: Array[String], nrDocumenti: Int): Double ={
    //Applicazione tf-idf
    tramaA.map(
      parola=> (tramaB.intersect(Seq(parola._1)).length/tramaB.length.toDouble)* math.log10(nrDocumenti/parola._2)
    ).sum
  }

  def PlotAlgorithm(Sc: SparkContext, Film:(String, String), ImdbTrama: RDD[(String, (String, String))], numeroDocumenti: Broadcast[Int]):
    Array[(String, (String, Double))] ={
    val bucket=DFutilities.bucket

    //lemmatization: file testuale in cui ogni riga è della forma parola \t lemma
    val lemma = Ss.sparkContext.textFile(bucket+"lemmatization.txt")

    val ParoleTrama = Film._2.split(" ").toSeq.distinct
    val lemmatizzatoreDistribuito=Sc.broadcast(lemma
                                                .map(r=>  {val s=r.split("\t"); (s(1),s(0))})
                                                .filter(r=>ParoleTrama.contains(r._1))
                                                .collect()
                                                .toMap)

    val dataSetParole= Ss.createDataFrame(Seq((1, ParoleTrama))).toDF("id", "parole")
    //Array di lemmi della trama di cui valutare la massima somiglianza
    val tramaSenzaStopWord= remover.transform(dataSetParole).map(row=> row(2).toString.replaceAll("[\\[\\]]","")).first()

    val tramaLD = Sc.broadcast(Lemmatizza(tramaSenzaStopWord, lemmatizzatoreDistribuito.value))

    //Associazione [parola=> numero di volte in cui compare in tutte le trame], solo per le parole della trama di cui massimizzare la somiglianza
    val contaParoleDistribuito=Sc.broadcast(
                                            ImdbTrama
                                            .flatMap(trama => Lemmatizza(trama._2._1, lemmatizzatoreDistribuito.value) intersect(tramaLD.value))
                                            .map(p=> (p, 1))
                                            .reduceByKey(_+_)
                                            .collect())

    ImdbTrama
      .mapValues(trama=> (trama._2, ValutaTrama(contaParoleDistribuito.value, Lemmatizza(trama._1, lemmatizzatoreDistribuito.value), numeroDocumenti.value)))
      .top(2) (Ordering.by[(String, (String, Double)), Double](_._2._2))

  }

}
