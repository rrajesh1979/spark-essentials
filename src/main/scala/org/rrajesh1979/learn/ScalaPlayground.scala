package org.rrajesh1979.learn

object ScalaPlayground extends App {
  println("Hello Scala")

  val incrementorLegacy: Function1[Int, Int] = new Function1[Int, Int] {
    override def apply(v1: Int): Int = v1 + 1
  }
  val x = incrementorLegacy(1)
  println(x)

  val incrementorShort: Int => Int = new Function1[Int, Int] {
    override def apply(v1: Int): Int = v1 + 1
  }

  val incrementorShorter: Int => Int = new (Int => Int) {
    override def apply(v1: Int): Int = v1 + 1
  }

  val incrementorShortest: Int => Int = v1 => v1 + 1

  val y = incrementorShortest(2)
  println(y)

  val sum: (Int, Int) => Int = (a, b) => a + b
  println(sum(1, 2))

  val diff: (Int, Int) => Int = (a, b) => a - b
  println(diff(1, 2))

  val listInt: List[Int] = List(1, 2, 3, 4, 5)
  val listIncrement: List[Int] = listInt.map(incrementorShortest)
  println(listIncrement)
  val listInc: List[Int] = listInt.map(x => x + 1)
  println(listInc)
  val listSquare: List[Int] = listInt.map(x => x * x)
  println(listSquare)

  //FlatMap example
  val listInt2: List[String] = List("a", "b", "c", "d", "e")
  val listInt3: List[Int] = List(1, 2, 3, 4, 5)

  val listInt4: List[String] = listInt2.flatMap(x => listInt3.map(y => x + y))
  println(listInt4)

  //Map example
  val listInt5: List[String] = List("a", "b", "c", "d", "e")
  val listInt6: List[String] = listInt5.map(x => x + "1")
  println(listInt6)

  //Filter example
  val listInt7: List[Int] = List(1, 2, 3, 4, 5)
  val listInt8: List[Int] = listInt7.filter(x => x % 2 == 0)
  println(listInt8)

  //Reduce example
  val listInt9: List[Int] = List(1, 2, 3, 4, 5)
  val listInt10: Int = listInt9.reduce((a, b) => a + b)
  val listInt11: Int = listInt9.reduce(_ + _)
  val listInt12: Int = listInt9.sum
  println(listInt10)
  println(listInt11)
  println(listInt12)

  //Fold example
  val listInt13: List[Int] = List(1, 2, 3, 4, 5)
  val listInt14: Int = listInt13.fold(0)((a, b) => a + b)
  val listInt15: Int = listInt13.fold(0)(_ + _)
  val listInt16: Int = listInt13.sum
  println(listInt14)
  println(listInt15)
  println(listInt16)

  //For comprehension example
  val listInt17: List[Int] = List(1, 2, 3, 4, 5)
  val listInt18: List[Int] =
    for (x <- listInt17 if x % 2 == 0)
      yield x * x
  println(listInt18)

  //Pattern matching example
  val listInt19: List[Int] = List(1, 2, 3, 4, 5)
  val listInt20: List[String] =
    listInt19.map(x => x match {
      case 1 => "one"
      case 2 => "two"
      case 3 => "three"
      case 4 => "four"
      case 5 => "five"
      case _ => "unknown"
    })
  println(listInt20)


}
