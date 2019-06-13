import scala.util.Random

object test {
  def main(args: Array[String]): Unit = {
      println("hello man")
    val random=new Random()
//    for(i<-0 to 20){
//      println(random.nextInt(10))
//    }
//
    val s="dfaf|dfaf"
    s.split("\\|").foreach(println(_))
  }
}
