object Example2 extends App
{

    val numEntries = scala.io.StdIn.readInt
    var phoneBook = scala.collection.mutable.Map[String, String]()

    for(entry <- 1 to numEntries) {
        var anEntry = scala.io.StdIn.readLine
        var a = anEntry.split(" ")(0)
        var b = anEntry.split(" ")(1)
        phoneBook += (a -> b)
    }
    var query:String=""
    while(query!=null)
    {
      query=scala.io.StdIn.readLine()
      if(phoneBook.get(query)==None)
      {
        println("Not found")
      }
      else println(query + "=" + phoneBook(query))
    }
}
