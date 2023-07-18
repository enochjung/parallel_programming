package reductions

import org.scalameter.*

object ParallelCountChangeRunner:

  @volatile var seqResult = 0

  @volatile var parResult = 0

  val standardConfig = config(
    Key.exec.minWarmupRuns := 20,
    Key.exec.maxWarmupRuns := 40,
    Key.exec.benchRuns := 80,
    Key.verbose := false
  ) withWarmer (Warmer.Default())

  def main(args: Array[String]): Unit =
    val amount = 250
    val coins = List(1, 2, 5, 10, 20, 50)
    val seqtime = standardConfig measure {
      seqResult = ParallelCountChange.countChange(amount, coins)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential count time: $seqtime")

    def measureParallelCountChange(
        threshold: => ParallelCountChange.Threshold
    ): Unit = try
      val fjtime = standardConfig measure {
        parResult = ParallelCountChange.parCountChange(amount, coins, threshold)
      }
      println(s"parallel result = $parResult")
      println(s"parallel count time: $fjtime")
      println(s"speedup: ${seqtime.value / fjtime.value}")
    catch
      case e: NotImplementedError =>
        println("Not implemented.")

    println("\n# Using moneyThreshold\n")
    measureParallelCountChange(ParallelCountChange.moneyThreshold(amount))
    println("\n# Using totalCoinsThreshold\n")
    measureParallelCountChange(
      ParallelCountChange.totalCoinsThreshold(coins.length)
    )
    println("\n# Using combinedThreshold\n")
    measureParallelCountChange(
      ParallelCountChange.combinedThreshold(amount, coins)
    )

object ParallelCountChange extends ParallelCountChangeInterface:

  /** Returns the number of ways change can be made from the specified list of
    * coins for the specified amount of money.
    */
  def countChange(money: Int, coins: List[Int]): Int =
    if (coins.isEmpty) {
      if (money == 0) 1 else 0
    } else {
      val withCurrentCoin =
        if (money >= coins.head) countChange(money - coins.head, coins) else 0
      val withNextCoin = countChange(money, coins.tail)
      withCurrentCoin + withNextCoin
    }

  type Threshold = (Int, List[Int]) => Boolean

  /** In parallel, counts the number of ways change can be made from the
    * specified list of coins for the specified amount of money.
    */
  def parCountChange(money: Int, coins: List[Int], threshold: Threshold): Int =
    if (coins.isEmpty) {
      if (money == 0) 1 else 0
    } else {
      if (threshold(money, coins)) countChange(money, coins)
      else {
        val (withCurrentCoin, withNextCoin) =
          parallel(
            if (money >= coins.head)
              parCountChange(money - coins.head, coins, threshold)
            else 0,
            parCountChange(money, coins.tail, threshold)
          )
        withCurrentCoin + withNextCoin
      }
    }

  /** Threshold heuristic based on the starting money. */
  def moneyThreshold(startingMoney: Int): Threshold =
    (money: Int, coins: List[Int]) =>
      money.toLong * 3 <= startingMoney.toLong * 2

  /** Threshold heuristic based on the total number of initial coins. */
  def totalCoinsThreshold(totalCoins: Int): Threshold =
    (money: Int, coins: List[Int]) => coins.length * 3 <= totalCoins * 2

  /** Threshold heuristic based on the starting money and the initial list of
    * coins.
    */
  def combinedThreshold(startingMoney: Int, allCoins: List[Int]): Threshold =
    (money: Int, coins: List[Int]) =>
      money * coins.length * 2 <= startingMoney * allCoins.length
