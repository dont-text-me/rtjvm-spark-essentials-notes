package part1recap

import scala.concurrent.Future
import scala.language.implicitConversions
import scala.util.{Failure, Success}

object ScalaRecap extends App {
  class Animal

  trait Carnivore {
    def eat(animal: Animal): Unit = println("Nom nom")
  }

  object MySingleton

  object Carnivore

  trait MyList[A]

  val three = 1.+(2)


  val incrementer: Int => Int = x => x + 1
  val incremented = incrementer(42)

  val processedList = List(1, 2, 3).map(incrementer)

  val unknown: Any = 45
  val ordinal = unknown match {
    case 1 => "first"
    case 2 => "second"
    case _ => "unknown"
  }

  import scala.concurrent.ExecutionContext.Implicits.global

  val aFuture = Future {
    // some expensive computation that runs on another thread
    42
  }

  aFuture.onComplete {
    case Success(value) => println(s"Computation complete with result $value")
    case Failure(exception) => println(s"I have failed with exception $exception")
  }

  def methodWithImplicitArgument(implicit x: Int) = x + 43

  implicit val implicitInt: Int = 67
  val implicitCall = methodWithImplicitArgument

  case class Person(name: String) {
    def greet = println(s"My name is $name")
  }

  implicit def fromStringToPerson(name: String) = Person(name)

  "Bob".greet // implicit conversion! the compiler looks for a suitable implicit method so that the code compiles

  implicit class Dog(name: String){
    def bark = println("Bark")
  }

  "Bobik".bark
}
