import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object Similarity {

  def Coseno(SC: SparkContext, filmVotoUtenteX: Broadcast[Map[String,Double]], AltriUtentiRDD: RDD[(String, (String, Double))])={
    val mediaUtenteX=filmVotoUtenteX.value.aggregate(0.0)((acc, elem)=> acc+Math.pow(elem._2, 2), _+_)
    val mediaMioDistribuito=SC.broadcast(Math.sqrt(mediaUtenteX))

    def filmInComune(filmVotoUtenteX: Map[String,Double], film: String): Boolean ={
        filmVotoUtenteX.contains(film)
    }

    def votoMio(filmVotoUtenteX: Map[String, Double], film:String):Double={
        filmVotoUtenteX.get(film) match {
          case None=>0.0
          case Some(x)=>x
        }
    }

    val coseno = AltriUtentiRDD
      .aggregateByKey((0.0, 0.0))((acc, el)=>
        ( if(filmInComune(filmVotoUtenteX.value,el._1)) {acc._1 +(el._2*votoMio(filmVotoUtenteX.value, el._1))}
        else acc._1 , acc._2+math.pow(el._2,2)),
        (x, y)=>(x._1+y._1, x._2+y._2))
      .mapValues(x=> x._1/(mediaMioDistribuito.value*Math.sqrt(x._2))).collect().toMap

    //Restituiamo una coppia (Coseno per i singoli utenti, mediaUtenteX)
    (SC.broadcast(coseno), mediaUtenteX)

  }

/*  def Pearson(SC: SparkContext, UserMioData: Map[String, Double], OtherUserData: RDD[(String, (String, Double))]) = {
    val MediaVotiUtenti = OtherUserData.map(riga => (riga._1, (riga._2._2, 1)))
                          .reduceByKey((r1, r2) => (r1._1 + r2._1, r1._2 + r2._2))
                          .map(riga => (riga._1, riga._2._1 / riga._2._2.toDouble))
    val OtherUserMedia = OtherUserData.join(MediaVotiUtenti)
    val MediaX = UserMioData.map(riga => (riga._2, 1)).reduce((elem1, elem2) => (elem1._1 + elem2._1, elem1._2 + elem2._2))
    val UserMioDataSottratto = UserMioData.map(elem => (elem._1, elem._2 - (MediaX._1 / MediaX._2)))
    val absVectorX = Math.sqrt(UserMioDataSottratto.aggregate(0.0)((acc, elem) => acc + Math.pow(elem._2, 2), _ + _))

    val UserMioDataDistribuito = SC.broadcast(UserMioDataSottratto)
    val absVectorXDistribuito = SC.broadcast(absVectorX)

    def filmInComune(FilmX: Iterable[String], film: String): Boolean = {
      FilmX.contains(film)
    }

    def votoMio(FilmX: Map[String, Double], film: String): Double = {
      FilmX.get(film) match {
        case None => 0.0
        case Some(x) => x
      }
    }

    OtherUserMedia
      .aggregateByKey((0.0, 0.0))((acc, elem) =>
         (if (filmInComune(UserMioDataDistribuito.value.keys, elem._1._1)) {
               acc._1 + ((elem._1._2 - elem._2) * votoMio(UserMioDataDistribuito.value, elem._1._1))}
          else  acc._1,
                acc._2 + Math.pow((elem._1._2 - elem._2), 2)),
         (riga1, riga2) => (riga1._1 + riga2._1, riga1._2 + riga2._2))
      .mapValues(riga => riga._1 / (absVectorXDistribuito.value * Math.sqrt(riga._2)))

  }*/
}
