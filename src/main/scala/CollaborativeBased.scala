import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD

object CollaborativeBased{
  val numeroUtentiSimiliDaConsiderare: Int= 3
  val ParForExecutor=3
  //Funzione di aggregazione= restituisce i film migliori sulla base del valore di similarità
  def CollaborativeAlgorithm(SC: SparkContext, Metodo: String, filmVotoUtenteX: Broadcast[Map[String, Double]],
                          MovieData: Broadcast[Map[String, String]], AltriUtentiRDD: RDD[(String, (String, Double))]):
                          Array[String] = {

    val (cosenoUtenti, mediaMio)= Similarity.Coseno(SC,  filmVotoUtenteX, AltriUtentiRDD)

    val FilmDaCiclare = AltriUtentiRDD
                        .filter(row => !filmVotoUtenteX.value.contains(row._2._1))
                        //.join(cosenoUtenti) se cosenoUtenti fosse un RDD e non una Map
                        .map(row => (row._2._1, (row._1, row._2._2, cosenoUtenti.value(row._1))))
                        .partitionBy(new HashPartitioner(SC.getExecutorMemoryStatus.size*ParForExecutor))
                        .groupByKey()
                        .mapValues(row=>{ val a=row.toList.sortWith((x, y)=> x._3>y._3).take(numeroUtentiSimiliDaConsiderare);
                                          (a, 1/ a.map(y=>math.abs(y._3)).sum)
                                    }
                        )

    //Si prendono in considerazione sia i voti medi degli utenti, sia il coefficiente di similarità
    if(Metodo=="C"){
      val MediaUserX = SC.broadcast(mediaMio)
      val MediaVotiUtenti =SC.broadcast( AltriUtentiRDD
                                          .mapValues(riga =>  (riga._2, 1))
                                          .reduceByKey((r1, r2) => (r1._1 + r2._1, r1._2 + r2._2))
                                          .mapValues(riga => riga._1 / riga._2.toDouble)
                                          .collect()
                                          .toMap)

      FilmDaCiclare
                  .mapValues(x=> MediaUserX.value+ (x._2 * x._1.aggregate(0.0) ((acc, el)=>
                    acc+(el._3*(el._2-MediaVotiUtenti.value(el._1))), _+_) ))
                  .top(3)(Ordering.by[(String, Double), Double](_._2))
                  .map(elem=> (MovieData.value(elem._1)))

    }
    //Si prende in considerazione solo la media aritmetica dei voti per ogni film
    else if (Metodo=="A") {
      FilmDaCiclare .mapValues(x=> (x._1.aggregate(0.0)((acc, el)=>acc+el._3, _+_))/x._1.size)
                    .top(3)(Ordering.by[(String, Double), Double](_._2))
                    .map(elem=> (MovieData.value(elem._1)))
    }
    //Si prende in considerazione solo il coefficiente di similarità
    else  {
      FilmDaCiclare .mapValues(x=>  x._2 * x._1.aggregate(0.0) ((acc, el)=>acc+(el._3*el._2), _+_) )
                    .top(3)(Ordering.by[(String, Double), Double](_._2))
                    .map(elem=> (MovieData.value(elem._1)))
    }
  }
}