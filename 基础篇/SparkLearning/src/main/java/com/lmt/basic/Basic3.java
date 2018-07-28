package com.lmt.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.storage.StorageLevel;

public class Basic3 {
	public static void main(String args[]) {
		SparkConf conf = new SparkConf().setAppName("basic1").setMaster("local[*]");
		Configuration config = new Configuration();
		JavaSparkContext sc = new JavaSparkContext(conf);
		//缓存到内存
//		JavaRDD<String> rdd1 = sc.textFile("/Users/iyx/Documents/workspace/SparkLearning/TestFile/test.txt").cache();
//		String a = rdd1.reduce(new Function2<String,String,String>(){
//			public String call(String v1, String v2) throws Exception {
//				return v1 + "==="+ v2;
//			}			
//		});
//		System.out.println(a);
		
		//序列化 并且 缓存到磁盘
		JavaRDD<String> rdd1 = sc.textFile("/Users/iyx/Documents/workspace/SparkLearning/TestFile/test.txt").persist(StorageLevel.MEMORY_AND_DISK_SER());
		String a = rdd1.reduce(new Function2<String,String,String>(){
			public String call(String v1, String v2) throws Exception {
				return v1 + "==="+ v2;
			}			
		});
		System.out.println(a);
	}
}
