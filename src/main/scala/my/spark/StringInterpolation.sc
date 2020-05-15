package my.spark

import scala.util.matching.Regex

object StringInterpolation {

  implicit class Regex(sc: StringContext) {
    def r = new util.matching.Regex(sc.parts.mkString, sc.parts.tail.map(_ => "x"): _*)
  }

  val value = "INTRATE:IRSWAP:BASIS USD USD USd-LIBOR-BBA 3M USD-IBOR-BBA 1M 202-06-22"
                                                  //> value  : String = INTRATE:IRSWAP:BASIS USD USD USd-LIBOR-BBA 3M USD-IBOR-BBA
                                                  //|  1M 202-06-22

  value match {

    case r"([\w\:]*)$t (\w*)$first (\w*)${ second } ([\w-]*)${ third } (\d+[DMY]+)${ forth } ([\w-]*)${ fifth } (\d+[DMY]+)${ sixth } (.*)${ last }" => {
      println(t); println(first); println(second); println(third); println(forth); println(fifth);
      println(sixth); println("Last : " + last)
    }

    case r"([\w\:]*)$t (\w*)$first (\w*)${ second } ([\w-]*)${ third } (\d+[DMY]+)${ forth } (.*)${ last }" => {
      println(t); println(first); println(second); println(third); println(forth); println("Last : " + last)
    }

    case _ => {}
  }                                               //> INTRATE:IRSWAP:BASIS
                                                  //| USD
                                                  //| USD
                                                  //| USd-LIBOR-BBA
                                                  //| 3M
                                                  //| USD-IBOR-BBA
                                                  //| 1M
                                                  //| Last : 202-06-22

  "10+15" match { case r"(\d\d)${ first }\+(\d\d)${ second }" => first.toInt + second.toInt case _ => 0 }
                                                  //> res0: Int = 25

}