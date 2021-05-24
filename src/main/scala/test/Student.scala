package test

/**
 * @Author Do
 * @Date 2020/7/7 23:26
 */
class Student {
  private val name: String = ""
  private val age: Int = 0

}

object Student{
  private val defaultHeight: Int = 180

  def main(args: Array[String]): Unit = {
    val student: Student = new Student
    println(student.age)
  }

}