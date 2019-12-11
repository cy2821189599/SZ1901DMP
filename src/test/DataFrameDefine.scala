class Student(one: Int, two: Int, three: Int, four: Int, five: Int
                      , six: Int, seven: Int, eight: Int, nine: Int, ten: Int, eleven: Int, twelve: Int
                      , thirteen: Int, fourteen: Int, fifteen: Int, sixteen: Int
                      , seventeen: Int, eighteen: Int, nineteen: Int, twenty: Int, first: Int, second: Int
                      , third: Int, fourth: Int, fifth: Int, sixth: Int, seventh: Int) extends Product {
  val arrays = Array(one, two, three, four, five, six, seven, eight, nine, ten, eleven, twelve, thirteen, fourteen, fifteen, sixteen, seventeen, eighteen, nineteen, twenty, first, second, third, fourth, fifth, sixth, seventh)

  override def productElement(n: Int): Any = arrays(n)

  override def productArity: Int = arrays.length

  override def canEqual(that: Any): Boolean = arrays.contains(that)
}
