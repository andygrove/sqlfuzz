// Copyright 2022 Andy Grove
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

// http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import java.io.{BufferedWriter, File, FileWriter, FilenameFilter}

import org.apache.spark.sql.SparkSession
import org.rogach.scallop._

import scala.io.Source

//TODO use named arguments
class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val sql = trailArg[String]()
  val data = trailArg[String]()
  verify()
}

object Main {
  def main(arg: Array[String]) {
    val conf = new Conf(arg)

    val spark = SparkSession.builder()
      .master("local[*]")
      .getOrCreate()

    val dir = new File(conf.data())
    println(dir.getAbsolutePath)

    val files = dir.list(new FilenameFilter() {
      override def accept(file: File, s: String): Boolean = s.endsWith(".parquet")
    })
    if (files == null) {
      println(s"No data files found in ${dir.getAbsolutePath}")
      System.exit(-1)
    }

    for (file <- files) {
      val i = file.lastIndexOf('/')
      val tableName = file.substring(i+1, file.lastIndexOf(".parquet"))
      val x = new File(dir, file)
      spark.read.parquet(x.getAbsolutePath).createTempView(tableName)
    }

    val w = new BufferedWriter(new FileWriter("target/report.txt"))

    val source = Source.fromFile(new File(conf.sql()))
    val lines = source.getLines()
    var sql = "";

    for (line <- lines) {
      if (line.startsWith("--")) {
        w.write(line)
        w.write('\n')
      } else {
        sql += line.trim()
        sql += "\n";
        if (sql.trim().endsWith(";")) {
          w.write(sql)
          w.write('\n')
          val rows = spark.sql(sql).collect()
          w.write("-- BEGIN RESULT --\n")
          for (row <- rows) {
            w.write(row.toSeq.map(_.toString).mkString("\t"))
            w.write('\n')
          }
          w.write("-- END RESULT --\n")
          sql = ""
        }
      }
    }

    source.close()
    w.close()

  }

}
