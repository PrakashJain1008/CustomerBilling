object Solution extends App{
    val n=scala.io.StdIn.readInt()
    for(i<-0 to n-1){
        val str=scala.io.StdIn.readLine()
        println(str.sliding(1,2).mkString("") +" "+ str.tail.sliding(1,2).mkString(""))
    }
    
}
