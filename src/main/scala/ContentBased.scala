import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object ContentBased {
  val numeroFilmSimiliDaConsiderare: Int= 3
  //Restituisce i migliori film sulla base delle caratteristiche
  def ContentAlgorithm(imdbFeatureFinale: RDD[(String, (String, Int, Array[String], String, Array[String]))],
                  filmFeature:Broadcast[(String, Int, Array[String], String, Array[String])]):  Array[(String, (String, Double))] ={

      imdbFeatureFinale .mapValues(x=> (x._1, valutaSimilarità(filmFeature.value, x)))
     .top(numeroFilmSimiliDaConsiderare)(Ordering.by[(String, (String, Double)), Double](_._2._2))
  }

  //Valuta la similarità tra due film sulla base di caratteristiche quali anno, genere, regista e attori
  def valutaSimilarità(FilmX:(String, Int, Array[String], String, Array[String]),
                       FilmY: (String, Int, Array[String], String, Array[String])):Double={

    val Anno: Double= 1/ (2.0+ Math.abs((FilmX._2-FilmY._2).toDouble))
    val Genere: (Double, Boolean)= {val a= FilmX._3.intersect(FilmY._3).length
                                      (a, a==(FilmX._3.length))}
    val Direttore= if (FilmX._4 == FilmY._4) 1.0 else 0.0
    val Attori: (Double, Boolean)= {val a=FilmX._5.intersect(FilmY._5).length
                                    (a, a==(FilmX._5.length))}

    if (Genere._2 && Attori._2) Anno+Genere._1+Direttore+Attori._1+1.0
    else if (Genere._2 || Attori._2) Anno+Genere._1+Direttore+Attori._1+0.5
    else if (Direttore==1.0) Anno+Genere._1+Direttore+Attori._1+0.5
    else Anno+Genere._1+Attori._1
  }

}
