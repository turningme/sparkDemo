package org.bytedance.omega

import java.text.SimpleDateFormat

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.collection.mutable

object TestMain {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[4]").appName("tt")
      .getOrCreate()

    /** ***************************************************************************************************************
      * 表结构
      */
    val StudentSchema: StructType = StructType(mutable.ArraySeq(  //学生表
      StructField("Sno", StringType, nullable = false),           //学号
      StructField("Sname", StringType, nullable = false),         //学生姓名
      StructField("Ssex", StringType, nullable = false),          //学生性别
      StructField("Sbirthday", StringType, nullable = true),      //学生出生年月
      StructField("SClass", StringType, nullable = true)          //学生所在班级
    ))
    val CourseSchema: StructType = StructType(mutable.ArraySeq(   //课程表
      StructField("Cno", StringType, nullable = false),           //课程号
      StructField("Cname", StringType, nullable = false),         //课程名称
      StructField("Tno", StringType, nullable = false)            //教工编号
    ))
    val ScoreSchema: StructType = StructType(mutable.ArraySeq(    //成绩表
      StructField("Sno", StringType, nullable = false),           //学号（外键）
      StructField("Cno", StringType, nullable = false),           //课程号（外键）
      StructField("Degree", IntegerType, nullable = true)         //成绩
    ))
    val TeacherSchema: StructType = StructType(mutable.ArraySeq(  //教师表
      StructField("Tno", StringType, nullable = false),           //教工编号（主键）
      StructField("Tname", StringType, nullable = false),         //教工姓名
      StructField("Tsex", StringType, nullable = false),          //教工性别
      StructField("Tbirthday", StringType, nullable = true),      //教工出生年月
      StructField("Prof", StringType, nullable = true),           //职称
      StructField("Depart", StringType, nullable = false)         //教工所在部门
    ))

    /** ***************************************************************************************************************
      * 获取当前时间函数
      */
    def getDate(time: String) = {
      val now: Long=System.currentTimeMillis()
      var df: SimpleDateFormat = new SimpleDateFormat(time)
      df.format(now)
    }

    /** ***************************************************************************************************************
      * 读取数据
      */
    val StudentData = spark.sparkContext.textFile("input/sqltable/Student").map{
      lines =>
        val line = lines.split(",")
        Row(line(0),line(1),line(2),line(3),line(4))
    }
    val CourseData = spark.sparkContext.textFile("input/sqltable/Course").map{
      lines =>
        val line = lines.split(",")
        Row(line(0),line(1),line(2))
    }
    val ScoreData = spark.sparkContext.textFile("input/sqltable/Score").map{
      lines =>
        val line = lines.split(",")
        Row(line(0),line(1),line(2).toInt)
    }
    val TeacherData = spark.sparkContext.textFile("input/sqltable/Teacher").map{
      lines =>
        val line = lines.split(",")
        Row(line(0),line(1),line(2),line(3),line(4),line(5))
    }

    /** ***************************************************************************************************************
      * 转换成表
      */
    val StudentTable = spark.createDataFrame(StudentData, StudentSchema)
    StudentTable.createOrReplaceTempView("Student")
    val CourseTable = spark.createDataFrame(CourseData, CourseSchema)
    CourseTable.createOrReplaceTempView("Course")
    val ScoreTable = spark.createDataFrame(ScoreData, ScoreSchema)
    ScoreTable.createOrReplaceTempView("Score")
    val TeacherTable = spark.createDataFrame(TeacherData, TeacherSchema)
    TeacherTable.createOrReplaceTempView("Teacher")


    /** ***************************************************************************************************************
      * 走sql节奏
      * 表名，字段名，区分大小写
      */
    ////1、 查询Student表中的所有记录的Sname、Ssex和Class列。
    spark.sql("SELECT sname, ssex, sclass FROM Student").show()

    ////2、 查询教师所有的单位即不重复的Depart列。
    spark.sql("SELECT DISTINCT depart FROM Teacher").show()

    ////3、 查询Student表的所有记录
    spark.sql("SELECT * FROM Student").show()

    ////4、 查询Score表中成绩在60到80之间的所有记录。
    //spark.sql("SELECT * FROM Score WHERE degree BETWEEN 60 and 80").show()
    spark.sql("SELECT * FROM Score WHERE degree >= 60 and degree <= 80").show()

    ////5、 查询Score表中成绩为85，86或88的记录。
    spark.sql("SELECT * FROM Score WHERE degree = '85' OR degree = '86' OR degree = '88'").show()

    ////6、 查询Student表中“95031”班或性别为“女”的同学记录。
    spark.sql("SELECT * FROM Student WHERE sclass = '95031' OR ssex = 'female'").show()

    ////7、 以Class降序,升序查询Student表的所有记录。
    spark.sql("SELECT * FROM Student ORDER BY sclass DESC").show()
    spark.sql("SELECT * FROM Student ORDER BY sclass").show()

    ////8、 以Cno升序、Degree降序查询Score表的所有记录。
    spark.sql("SELECT * FROM Score t ORDER BY t.sno ASC, t.degree DESC").show()

    ////9、 查询“95031”班的学生人数。
    spark.sql("SELECT t.sclass totalnum FROM Student t WHERE sclass = '95031'").show()
    spark.sql("SELECT t.sclass AS totalnum FROM Student t WHERE sclass = '95031'").show()

    ////10、 查询Score表中的最高分的学生学号和课程号。（子查询或者排序）
    //// oracle    =>  WHERE rownum = 1
    //// spark sql =>  LIMIT 1
    spark.sql("SELECT * FROM (SELECT * FROM Score ORDER BY degree DESC LIMIT 1)").show()
    spark.sql("SELECT t.sno, t.cno FROM Score t ORDER BY degree DESC").show()
    spark.sql("SELECT * FROM Score WHERE degree IN(SELECT MAX(degree) FROM Score t)").show()

    ////11、 查询每门课的平均成绩。
    spark.sql("SELECT AVG(degree) average FROM Score t WHERE cno = '3-245'").show()
    spark.sql("SELECT AVG(degree) average FROM Score WHERE cno = '3-105'").show()
    spark.sql("SELECT AVG(degree) average FROM Score WHERE cno = '6-166'").show()
    spark.sql("SELECT cno, AVG(degree) FROM Score t GROUP BY cno").show()

    ////12、查询Score表中至少有5名学生选修的并以3开头的课程的平均分数。
    spark.sql("SELECT cno, AVG(degree) FROM Score WHERE cno LIKE '3%' GROUP BY cno HAVING COUNT(1) >= 5").show()

    ////13、查询分数大于70，小于90的Sno列。
    spark.sql("SELECT sno FROM Score WHERE degree BETWEEN 70 AND 90").show()

    ////14、查询所有学生的Sname、Cno和Degree列。
    spark.sql("SELECT s.sname, t.cno, t.degree FROM Score t, Student s WHERE t.sno = s.sno").show()
    spark.sql("SELECT s.sname, t.cno, t.degree FROM Score t JOIN Student s ON t.sno = s.sno").show()

    ////15、查询所有学生的Sno、Cname和Degree列。
    spark.sql("SELECT s.sname, t.cno, t.degree FROM Score t JOIN Student s ON t.sno = s.sno").show()

    ////16、查询所有学生的Sname、Cname和Degree列。
    spark.sql("SELECT s.sname, t.degree, c.cname FROM Score t, Student s, Course c WHERE t.sno = s.sno AND t.cno = c.cno").show()
    spark.sql("SELECT s.sname, t.degree, c.cname FROM Score t " +
      "JOIN Student s on t.sno = s.sno " +
      "JOIN Course c on c.cno = t.cno").show()

    ////17、 查询“95033”班学生的平均分。
    spark.sql("SELECT AVG(degree) average FROM Score WHERE sno IN (SELECT sno FROM Student WHERE sclass = '95033')").show()

    ////19、  查询选修“3-105”课程的成绩高于“109”号同学成绩的所有同学的记录。
    spark.sql("SELECT * FROM Score WHERE cno = '3-105' AND degree > (SELECT degree FROM score WHERE sno = '109' AND cno = '3-105')").show()

    ////20、查询score中选学多门课程的同学中分数为非最高分成绩的记录。
    spark.sql("SELECT * FROM Score WHERE sno IN " +
      "(SELECT sno FROM Score t GROUP BY t.sno HAVING COUNT(1) > 1) AND degree != (SELECT MAX(degree) FROM Score)").show()
    spark.sql("SELECT * FROM Score WHERE degree != (SELECT MAX(degree) FROM Score)").show()

    ////21、 查询成绩高于学号为“109”、课程号为“3-105”的成绩的所有记录。
    spark.sql("SELECT * FROM Score t WHERE t.degree > (SELECT degree FROM Score WHERE sno = '109' AND cno = '3-105')").show()

    ////22、查询和学号为108的同学同年出生的所有学生的Sno、Sname和Sbirthday列。
    //// oracle    =>  to_char(t.sbirthday,'yyyy')
    //// spark sql =>  substring(t.sbirthday, 0, 4)
    spark.sql("SELECT sno, sname , sbirthday " +
      "FROM Student " +
      "WHERE substring(sbirthday, 0, 4) = ( " +
      "SELECT substring(t.sbirthday, 0, 4) " +
      "FROM Student t " +
      "WHERE sno = '108')").show()

    ////23、查询“张旭“教师任课的学生成绩。
    spark.sql("SELECT t.tno, c.cno, c.cname, s.degree FROM Teacher t " +
      "JOIN Course c ON t.tno = c.tno " +
      "JOIN Score s ON c.cno = s.cno WHERE t.tname = 'Zhang xu'").show()

    ////24、查询选修某课程的同学人数多于5人的教师姓名。
    spark.sql("SELECT tname FROM Teacher e " +
      "JOIN Course c ON e.tno = c.tno " +
      "JOIN(SELECT cno FROM Score GROUP BY cno HAVING COUNT(cno) > 5) t ON c.cno = t.cno").show()

    ////25、查询95033班和95031班全体学生的记录。
    spark.sql("SELECT * FROM Student WHERE sclass IN('95031', '95033')").show()
    spark.sql("SELECT * FROM Student WHERE sclass LIKE '9503%'").show()

    ////26、  查询存在有85分以上成绩的课程Cno.
    spark.sql("SELECT cno FROM Score WHERE degree > 85 GROUP BY cno").show()

    ////27、查询出“计算机系“教师所教课程的成绩表。
    spark.sql("SELECT t.sno, t.cno, t.degree FROM Score t " +
      "JOIN Course c ON t.cno = c.cno " +
      "JOIN Teacher e ON c.tno = e.tno WHERE e.depart = 'department of computer'").show()

    ////28、查询“计算机系”与“电子工程系“不同职称的教师的Tname和Prof。
    spark.sql("SELECT tname, prof " +
      "FROM Teacher " +
      "WHERE prof NOT IN (SELECT a.prof " +
      "FROM (SELECT prof " +
      "FROM Teacher " +
      "WHERE depart = 'department of computer' " +
      ") a " +
      "JOIN (SELECT prof " +
      "FROM Teacher " +
      "WHERE depart = 'department of electronic engineering' " +
      ") b ON a.prof = b.prof) ").show()
    spark.sql("SELECT tname, prof " +
      "FROM Teacher " +
      "WHERE depart = 'department of electronic engineering' " +
      "AND prof NOT IN (SELECT prof " +
      "FROM Teacher " +
      "WHERE depart = 'department of computer') " +
      "OR depart = 'department of computer' " +
      "AND prof NOT IN (SELECT prof " +
      "FROM Teacher " +
      "WHERE depart = 'department of electronic engineering')").show()

    ////29、查询选修编号为“3-105“课程且成绩至少高于选修编号为“3-245”的同学的Cno、Sno和Degree,并按Degree从高到低次序排序。
    spark.sql("SELECT t.sno, t.cno, degree " +
      "FROM SCORE t " +
      "WHERE degree > ( " +
      "SELECT MIN(degree) " +
      "FROM score " +
      "WHERE cno = '3-245' " +
      ") " +
      "AND t.cno = '3-105' " +
      "ORDER BY degree DESC").show()

    ////30、查询选修编号为“3-105”且成绩高于选修编号为“3-245”课程的同学的Cno、Sno和Degree.
    // oracle方式 spark.sql("select t.sno, t.cno, t.degree from SCORE t where t.degree > select degree from score where cno='3-245' or cno='3-105'").show()
    spark.sql("SELECT t.sno, t.cno, t.degree FROM Score t WHERE t.degree > (SELECT MAX(degree) FROM Score WHERE cno = '3-245' ) AND t.cno = '3-105'").show()

    ////31、 查询所有教师和同学的name、sex和birthday.
    spark.sql("SELECT sname, ssex, sbirthday FROM Student " +
      "UNION SELECT tname, tsex, tbirthday FROM Teacher").show()

    //// 32、查询所有“女”教师和“女”同学的name、sex和birthday. union
    spark.sql("SELECT sname, ssex, sbirthday " +
      "FROM Student " +
      "WHERE ssex = 'female' " +
      "UNION " +
      "SELECT tname, tsex, tbirthday " +
      "FROM Teacher " +
      "WHERE tsex = 'female'").show()

    ////33、 查询成绩比该课程平均成绩低的同学的成绩表。
    spark.sql("SELECT s.* " +
      "FROM score s " +
      "WHERE s.degree < ( " +
      "SELECT AVG(degree) " +
      "FROM score c " +
      "WHERE s.cno = c.cno)").show()

    ////34、 查询所有任课教师的Tname和Depart. in
    spark.sql("SELECT tname, depart " +
      "FROM teacher t " +
      "WHERE t.tno IN ( " +
      "SELECT tno " +
      "FROM course c " +
      "WHERE c.cno IN (" +
      "SELECT cno " +
      "FROM score))").show()

    ////35 、 查询所有未讲课的教师的Tname和Depart. not in
    spark.sql("SELECT tname, depart " +
      "FROM teacher t " +
      "WHERE t.tno NOT IN ( " +
      "SELECT tno " +
      "FROM course c " +
      "WHERE c.cno IN ( " +
      "SELECT cno " +
      "FROM score))").show()

    ////36、查询至少有2名男生的班号。  group by, having count
    spark.sql("SELECT SClass " +
      "FROM Student t " +
      "WHERE Ssex = 'male' " +
      "GROUP BY SClass " +
      "HAVING COUNT(Ssex) >= 2").show()

    ////37、查询Student表中不姓“王”的同学记录。 not like
    spark.sql("SELECT * FROM Student t WHERE Sname NOT LIKE('Wang%')").show()

    ////38、查询Student表中每个学生的姓名和年龄。
    ////将函数运用到spark sql中去计算，可以直接拿String的类型计算不需要再转换成数值型 默认是会转换成Double类型计算
    spark.sql("SELECT Sname, ("+ getDate("yyyy") +" - substring(sbirthday, 0, 4)) AS age FROM STUDENT t").show()
    ////浮点型转整型
    spark.sql("SELECT Sname, (CAST("+ getDate("yyyy") +" AS INT) - CAST(substring(sbirthday, 0, 4) AS INT)) AS age " +
      "FROM Student t").show()

    ////39、查询Student表中最大和最小的Sbirthday日期值。 时间格式最大值,最小值
    spark.sql("SELECT MAX(t.sbirthday) AS maximum FROM Student t").show()
    spark.sql("SELECT MIN(t.sbirthday) AS minimum FROM Student t").show()

    ////40、以班号和年龄从大到小的顺序查询Student表中的全部记录。 查询结果排序
    spark.sql("SELECT * " +
      "FROM Student " +
      "ORDER BY SClass DESC, CAST("+ getDate("yyyy") +" AS INT) - CAST(substring(Sbirthday, 0, 4) AS INT) DESC").show()

    ////41、查询“男”教师及其所上的课程。 select join
    spark.sql("SELECT TSex, CName " +
      "FROM Teacher t " +
      "JOIN course c ON t.tno = c.tno " +
      "WHERE TSex = 'male'").show()

    ////42、查询最高分同学的Sno、Cno和Degree列。 子查询
    spark.sql("SELECT * " +
      "FROM Score " +
      "WHERE degree = ( " +
      "SELECT MAX(degree) " +
      "FROM SCORE t)").show()

    ////43、查询和“李军”同性别的所有同学的Sname.
    spark.sql("SELECT sname " +
      "FROM STUDENT t " +
      "WHERE ssex IN ( " +
      "SELECT ssex " +
      "FROM student " +
      "WHERE sname = 'Liu Jun')").show()

    ////44、查询和“李军”同性别并同班的同学Sname.
    spark.sql("SELECT sname " +
      "FROM Student t " +
      "WHERE ssex IN (  " +
      "SELECT ssex " +
      "FROM student " +
      "WHERE sname = 'Liu Jun') " +
      "AND sclass IN (SELECT sclass " +
      "FROM student " +
      "WHERE sname = 'Liu Jun')").show()

    ////45、查询所有选修“计算机导论”课程的“男”同学的成绩表。
    spark.sql("SELECT t.sno, t.cno, t.degree " +
      "FROM Score t " +
      "JOIN Course c ON t.cno = c.cno " +
      "JOIN Student s ON s.sno = t.sno " +
      "WHERE s.SSex = 'male' " +
      "AND c.CName = 'Introduction to computer'").show()
  }


}
