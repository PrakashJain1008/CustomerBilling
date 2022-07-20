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



object Solution {
    def main(args: Array[String]) {
        var n = StdIn.readLine.trim.toInt
        var count=0
        while(n!=0)
        {
            n=(n & (n << 1))
            count+=1
        }
        println(count)
        
    }
}
