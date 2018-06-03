package comp9313.ass4
import org.apache.spark.SparkContext
import collection.immutable.IndexedSeq
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object SetSimJoin {
  def main(args: Array[String]) {
    val inputFile = args(0)  // Input file
    val outputFolder = args(1)  // Output folder path
    val threshold = args(2).toDouble  // Similarity threshold
//    val reducers = args(3) // Number of reducers to be used
    val conf = new SparkConf().setAppName("SetSimJoin").setMaster("local") // Configuration
    val sc = new SparkContext(conf) // Spark Context setup
    val input = sc.textFile(inputFile)  // Reading the input
//    
//    def similarityFinder(key:String, occurances:Array[String]):IndexedSeq[Any] = { //List[String] //List[(String, String, String)]
////      var threshold = 0.4
////      var resultArray : List[(String,String, String)] = List()
//      for(i <- 0 until occurances.length-1) yield{
//        for(j <- i+1 until occurances.length) yield{
//          var rowVal1 = occurances(i).split("&")
//          var row1 = rowVal1(0)
//          var list1 = rowVal1(1).split(",")
//          var rowVal2 = occurances(j).split("&")
//          var row2 = rowVal2(0)
//          var list2 = rowVal2(1).split(",")
//          var common = 0.0
//          var total = list1.length
//          var similarity = 0.0
//          for(num<-list1){
//            if(list2 contains num){
//              common += 1
//            }else{
//              total += 1
//            }
//          }
//          similarity = common/total
//          if(similarity >= threshold)
////            resultArray:+((row1, row2, similarity.toString));
//            (row1, row2, similarity.toString)
//        }
    
    
//    //String(String, String) {
//                      var thisVal: String = null
//                      for(y<-x) 
//                      thisVal = y
//                      return (thisVal, x(0) + "|" + x
//    val v = for (i <- (l.view filter (_ < 4))) yield i*2
//    for (i <- xs.indices)
//    for((x,i) <- xs.view.zipWithIndex)
//    def similarityFinder(s1:String, s2:String):String = {var str1 = (s1.split("|"))(1).split(';').toSet;var str2= (s2.split("|"))(1).split(';').toSet; str1.union(str2).mkString(",")}
    def similarityFinder(key:String, occurances:List[String]):List[((Int, Int), String)] = { //List[String] //List[(String, String, String)]
//      var threshold = 0.4
      var resultArray : List[((Int, Int), String)] = List()
      for(i <- 0 until occurances.length-1) {
        for(j <- i+1 until occurances.length){
          var rowVal1 = occurances(i).split("&")
          var row1 = rowVal1(0)
          var list1 = rowVal1(1).split(",")
          var rowVal2 = occurances(j).split("&")
          var row2 = rowVal2(0)
          var list2 = rowVal2(1).split(",")
          var common = 0.0
          var total = list1.length
          var similarity = 0.0
          for(num<-list2){
            if(list1 contains num){
              common += 1
            }else{
              total += 1
            }
          }
          similarity = common/total
          if(similarity >= threshold){
            if(row1<=row2)
              resultArray = resultArray:+ ((row1.toInt, row2.toInt), similarity.toString)
            else
              resultArray = resultArray:+ ((row2.toInt, row1.toInt), similarity.toString)
          }
//            (row1, row2, similarity.toString)
        }
    }
//    println(resultArray.toString)
    resultArray
}
    
    val myMap = input.map(x=> x.split(" ")).flatMap(x=> for(y<-x.drop(1)) yield (y, x(0) + "&" + x.drop(1).mkString(","))) // +"|"+x
//    val valMap = myMap.reduceByKey((x,y)=>similarityFinder(x,y))
     val valMap = myMap.groupByKey().flatMap(x=>similarityFinder(x._1, x._2.toList)) // .map((x,y)=> similarityFinder(x,y)) // similarityFinder(x,y)
     valMap.reduceByKey((x,y)=> x).sortBy(k => (k._1._1, k._1._2)).map(x=>s"${x._1}\t${x._2}").foreach(x=>println(x))  // (x=>x._1._2).sortBy(x=>x._1._1)
//     valMap.foreach(x => println(x)) //(x._1, x._2)
    
//    valMap.collect()

    
// TODO: 
//     1. Pass threshold
// 2. Run on AWS
// 3. Research Paper
// 4. Graph making
// 5. Frequency Sorting
//     6. Method to calculate the similarity
// 7. Read other's approach/Optimization
    
    
//    val edgeMap = input.map(x => x.split(" ")).map(x=> (x(1).toLong, (x(3).toDouble, 1)))  // x(2).toLong (Destination) is irrelevant for this question
//    val lenAvg = edgeMap.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2)).map(x=> (x._1, x._2._1.toDouble/x._2._2))  // Reducing by Key (addding the count and sum) and then finding the average
//    val sortedLenAvg = lenAvg.sortByKey(true, 1).sortBy(_._2, false)  // First sort Key based, second sort value based. Gives the desired result format.
//    val formattedLenAvg = sortedLenAvg.filter(_._2 > (0).toDouble).map(x=>s"${x._1}\t${x._2}")  // Output formatting
//    formattedLenAvg.saveAsTextFile(outputFolder)  // Writing the output
  }
}
