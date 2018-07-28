package com.lmt.basic;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

//RDD复用
public class Basic2 {
	public static void main(String args[]) {
		SparkConf conf = new SparkConf().setAppName("basic1").setMaster("local[*]");
		// SparkConf conf = new SparkConf().setAppName("sparkzip");
		Configuration config = new Configuration();
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> rdd1 = sc.textFile("/Users/iyx/Documents/workspace/SparkLearning/TestFile/test.txt");
		JavaRDD<String> rdd2 = rdd1.flatMap(new FlatMapFunction<String, String>() {
			public Iterator<String> call(String t) throws Exception {
				return Arrays.asList(t.split(" ")).iterator();
			}
		});
		JavaPairRDD<String, Integer> rdd3 = rdd2.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String t) throws Exception {
				return new Tuple2<String, Integer>(t, 1);
			}
		});
		// 错误做法
		//根据map方法创建了一个 JavaRDD<String> finalRdd 根据相同的 key 相加 value 
		JavaRDD<String> finalRdd = rdd3.map(new Function<Tuple2<String, Integer>, String>() {
			public String call(Tuple2<String, Integer> v1) throws Exception {
				return v1._1;
			}
		});
		finalRdd.foreach(f -> {
			System.out.println(f);
		});
		//根据reduceBykey 方法创建了一个 JavaPairRdd<String, Integer>
		JavaPairRDD<String,Integer> rdd4 = rdd3.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1+v2;
			}
		});
		
		rdd4.foreach(f -> {
			System.out.println(f._1+"--"+f._2);
		});
		//
		
		//正确做法   finalRdd 可以从 rdd3 直接操作无须新的rdd生成
		rdd3.foreach( f -> {System.out.println(f._1);} );
		JavaPairRDD<String,Integer> rdd5 = rdd3.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1+v2;
			}
		});
		
		rdd5.foreach(f -> {
			System.out.println(f._1+"--"+f._2);
		});
	}
}
