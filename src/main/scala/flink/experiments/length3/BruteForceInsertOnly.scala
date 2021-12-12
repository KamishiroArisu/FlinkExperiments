package flink.experiments.length3

import java.io.{BufferedReader, FileInputStream, InputStreamReader}
import scala.collection.mutable

object BruteForceInsertOnly {
    def graphBaseResult: mutable.Map[Int, mutable.HashSet[Int]] =
        mutable.HashMap()

    def graphDecodeFun(line: String): (Int, Int) = {
        val array = line.split(",")
        val src = Integer.valueOf(array(0))
        val dst = Integer.valueOf(array(1))
        (src, dst)
    }

    def graphAccumulationFun(map: mutable.Map[Int, mutable.HashSet[Int]], record: (Int, Int)): Unit = {
        map.getOrElseUpdate(record._1, mutable.HashSet[Int]()).add(record._2)
    }

    def pathBaseResult: mutable.Set[(Int, Int, Int, Int)] =
        mutable.HashSet()

    def pathDecodeFun(line: String): (Int, Int, Int, Int) = {
        val array = line.split(",")
        val src = Integer.valueOf(array(0))
        val via1 = Integer.valueOf(array(1))
        val via2 = Integer.valueOf(array(2))
        val dst = Integer.valueOf(array(3))
        (src, via1, via2, dst)
    }

    def pathAccumulationFun(set: mutable.Set[(Int, Int, Int, Int)], record: (Int, Int, Int, Int)): Unit =
        set.add(record)

    /**
     * helper function for reading from text file
     *
     * @param file   the path of the file
     * @param base   the base result(result when the file is empty)
     * @param decode the function to decode a line into a record
     * @param acc    the accumulation function for partial result T to absorb record R
     * @tparam T type of result
     * @tparam R type of record
     * @throws
     * @return
     */
    @throws[Exception]
    def readTextFile[T, R](file: String, base: T, decode: String => R, acc: (T, R) => Unit): T = {
        val reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)))
        val result = base
        var line = reader.readLine
        while (line != null) {
            val decoded = decode(line)
            acc(result, decoded)

            line = reader.readLine
        }
        result
    }

    def main(args: Array[String]): Unit = {
        // read input Graph Table
        val graphFile = args(0)
        val graph = readTextFile(graphFile, graphBaseResult, graphDecodeFun, graphAccumulationFun)

        // compute all the paths with length = 3
        val result = for {
            src <- graph.keySet
            via1 <- graph(src)
            via2 <- graph.getOrElse(via1, Set.empty[Int])
            dst <- graph.getOrElse(via2, Set.empty[Int])
        } yield (src, via1, via2, dst)

        // read the result from csv file
        val pathFile = args(1)
        val path = readTextFile(pathFile, pathBaseResult, pathDecodeFun, pathAccumulationFun)

        // check the two result have the same elements
        assert(path.size == result.size)
        for (p <- path)
            assert(result contains p)

        println("Validation Passed.")
    }
}
