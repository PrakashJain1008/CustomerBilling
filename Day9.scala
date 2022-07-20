import java.io._
import java.math._
import java.security._
import java.text._
import java.util._
import java.util.concurrent._
import java.util.function._
import java.util.regex._
import java.util.stream._
import scala.collection.immutable._
import scala.collection.mutable._
import scala.collection.concurrent._
import scala.concurrent._
import scala.io._
import scala.math._
import scala.sys._
import scala.util.matching._
import scala.reflect._

object Result extends App{

    def factorial(n: Int): Int = {
    // Write your code here
        if(n<=1) return 1
        else return n*factorial(n-1)

    }

}

object Solution {
    def main(args: Array[String]) {

        val n = StdIn.readLine.trim.toInt

        val result = Result.factorial(n)

        println(result)

    }
}
