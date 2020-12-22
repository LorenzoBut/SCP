import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession


object SistemaDiRaccomandazione {
  def main(args: Array[String]) {
    val Conf = new SparkConf()
                    //local mode:
                    .setMaster("local[*]")
                    .setAppName("SistemaDiRaccomandazione")

    val SC = new SparkContext(Conf)
    val SS = SparkSession.builder.appName("Simple Application").getOrCreate()
    val utenteMio="121"
    var inizio:Float=0.toFloat
    val toSec=math.pow(10,9).toFloat
    var Consigli=""

    //==Caricamento Dataset e manipolazioni iniziali====================================================================
    inizio=System.nanoTime().toFloat
    val (imdbDataRDD, ratingsDataRDD, linksDataRDD, reviewDataRDD, movieDataMap)= DFutilities.InizializzazioneDataset(SC, SS)
    val (filmUtenteX, miglioriFilmUtenteX, altriUtentiRDD, filmVotoUtenteX)=DFutilities.EstraiUtenteX(ratingsDataRDD, imdbDataRDD, linksDataRDD, utenteMio)
    val (imdbTramaFinale, imdbFeatureFinale) = DFutilities.OttieniImdbFeatureRDD(filmUtenteX, imdbDataRDD)
    val fineCaricamento=(System.nanoTime().toFloat-inizio) / toSec

    //==ALGORITMO CONTENT BASED=========================================================================================
    inizio=System.nanoTime().toFloat
    val miglioriContent=miglioriFilmUtenteX.map(
      a=>(a._2._1, ContentBased.ContentAlgorithm(imdbFeatureFinale, SC.broadcast(a._2))))
    val fineContent=(System.nanoTime().toFloat-inizio) / toSec
    //==================================================================================================================
    Consigli=Consigli+"FILM CON CARATTERISTICHE SIMILI A QUELLI CHE HAI APPREZZATO: \n"
    miglioriContent.seq.map(x=> x._2.map(y=>Consigli=Consigli+"\t-"+y._2._1+"\n"))

    //==ALGORITMO PLOT BASED===========================================================================================
    val numeroDocumenti=SC.broadcast(imdbTramaFinale.count().toInt)
    inizio=System.nanoTime().toFloat
    val miglioriTrama=miglioriFilmUtenteX.map(
      a=> (a._2._1, PlotBased.PlotAlgorithm(SC, (a._1._1, a._3), imdbTramaFinale, numeroDocumenti))
    )
    val fineTrama=(System.nanoTime().toFloat-inizio) / toSec
    //==================================================================================================================
    miglioriTrama.seq.map(
      x=> { Consigli=Consigli+"\nFILM CON TRAMA SIMILE A '"+x._1.toUpperCase+"':\n"
            x._2.map(y=>Consigli=Consigli+"\t-"+y._2._1+"\n")})

    //==ALGORITMO COLLABORATIVE=========================================================================================
    inizio=System.nanoTime().toFloat
    val miglioriCollaborative=CollaborativeBased
          .CollaborativeAlgorithm(SC, "C", SC.broadcast(filmVotoUtenteX), SC.broadcast(movieDataMap), altriUtentiRDD)
    val fineCollaborative=(System.nanoTime().toFloat-inizio) / toSec
    //==================================================================================================================
    altriUtentiRDD.unpersist(true)
    Consigli=Consigli+"\nFILM APPREZZATI DA ALTRI UTENTI CON GUSTI SIMILI:\n"
    miglioriCollaborative.foreach(x=>Consigli=Consigli+"\t -"+x.substring(0, x.length-6)+"\n")

    //==ALGORITMO RATINGS BASED=========================================================================================
    inizio=System.nanoTime().toFloat
    val miglioriApprezzamenti=RatingBased.RatingAlgorithm(reviewDataRDD, SC.broadcast(filmUtenteX), miglioriFilmUtenteX.flatMap(elem=> elem._2._3).distinct)
    val fineApprezzamenti=(System.nanoTime().toFloat-inizio) / toSec
    //==================================================================================================================
    Consigli=Consigli+"\nFILM CON MIGLIORI RECENSIONI DA PARTE DEL PUBBLICO: \n"
    for (i<-miglioriApprezzamenti){Consigli=Consigli+ "\t -"+i._1 +"\n"}


    Consigli=Consigli+
            "\n\nTEMPISTICHE:\n\t-CARICAMENTO DATI: "+DFutilities.Arrotonda(fineCaricamento)+"sec\n\t-ALGORITMO COLLABORATIVE:"+
            DFutilities.Arrotonda(fineCollaborative)+"sec\n\t-ALGORITMO TRAMA-BASED: "+DFutilities.Arrotonda(fineTrama)+
            "sec\n\t-ALGORITMO CONTENT-BASED: "+DFutilities.Arrotonda(fineContent)+"sec\n\t-ALGORITMO RATINGS-BASED:"+
            DFutilities.Arrotonda(fineApprezzamenti)+"sec"
   println(Consigli)

    /*
    Usato in deploy-mode = cluster in modo tale che i risultati vengano memorizzati su S3
    SC.parallelize(Seq(Consigli)).coalesce(1).saveAsTextFile(DFutilities.bucket+"Consigli")*/


    SS.stop()

  }
}
