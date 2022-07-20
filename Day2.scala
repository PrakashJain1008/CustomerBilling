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

object Result {

    /*
     * Complete the 'solve' function below.
     *
     * The function accepts following parameters:
     *  1. DOUBLE meal_cost
     *  2. INTEGER tip_percent
     *  3. INTEGER tax_percent
     */

    def solve(meal_cost: Double, tip_percent: Int, tax_percent: Int) {
    // Write your code here
        val total=meal_cost+(meal_cost/100)*tip_percent+(meal_cost/100)*tax_percent
        println(total.toInt)

    }

}

object Solution {
    def main(args: Array[String]) {
        val meal_cost = StdIn.readLine.trim.toDouble

        val tip_percent = StdIn.readLine.trim.toInt

        val tax_percent = StdIn.readLine.trim.toInt

        Result.solve(meal_cost, tip_percent, tax_percent)
    }
}
