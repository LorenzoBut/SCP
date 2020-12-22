import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import scala.collection.parallel.mutable.ParArray

object RatingBased{
  val numeroFilmSimiliDaConsiderare: Int= 3

  def RatingAlgorithm(review: RDD[(String, ((Int, ParArray[Int]), (Array[String], String)))],
                             FilmVisti: Broadcast[Array[String]], GeneriPreferiti: ParArray[String]): Array[(String, Double)] ={

    review
          .filter(x=>x._2._1._1>50 && x._2._2._1.intersect(GeneriPreferiti).length!=0 && ! FilmVisti.value.contains(x._1))
          .map(x=>(x._2._2._2, mediaProporzionata(x._2._1._1, x._2._1._2)))
          .top(numeroFilmSimiliDaConsiderare)(Ordering.by[(String, Double), Double](_._2))

  }
  def mediaProporzionata(nVotiTot: Int, votiCat: ParArray[Int]): Double ={
    val ap= for(i<-(0 to votiCat.length -1).par) yield{
                ((votiCat(i).toDouble/nVotiTot)*(i+1))
            }
    ap.sum
  }
}
