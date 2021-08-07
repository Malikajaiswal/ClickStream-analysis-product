// MALIKA JAISWAL

println("Hello")
/*
Instructions:
1. Write your code below the comment and click run. Results will be shown to your right.
2. Run the worksheet once and check if you see "hello" to your right
3. Use only val and not var
4. Try out with lazy val too
5. Try doing things in single or as less number of lines as possible.
 */

//task 01: Get the largest number from two numbers
val num1:Int = 25;
val num2:Int = 29;

val result1:Int = num1.max(num2);
println("The largest number is: "+ result1);



//task 02: Get Square of a number and add with another number
val result2: Int = (num1*num1)+num2;
println("Square of num1 and adding num2:"+result2);

//or

val result3: Double = scala.math.pow(num1,2)+num2;
println("Square of num1 and adding num2:"+result3);




//task 03: Create a List list01 from 1 to 10

val list01:List[Int] = List(1,2,3,4,5,6,7,8,9,10);
println(list01);

//or
val list01= List.range(1,11);
println(list01);



//task 04: add 11 to list01 at the end
val list02 = list01 :+11;
println(list02);


//task 05: add 0 to the beginning of the list in task 04
val list03 = 0 +:list02;
println(list03);


//task 06: Do task 03, 04, 05 in one line
val list04 = 0 +:list01 :+11;
println(list04);


//task 07: concatenate list in task 03 and task 05
val list05 = List.concat(list01,list03);
println(list05);


//task 08: reverse the list in task 06
val list06 = list04.reverse;
println("The reversed list is"+ list06)


//task 09: Write task 02 as a function instead and call the function in the next line
def task02(a:Int , b:Int):Unit = {
  val res= a*a + b;
  println(res);
}
task02(25,27);


//optional task 10: list with a square of the input list01
val list07 = list01.map(i=> i*i);
println(list07)
