package bigdata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import scala.Tuple2;

import org.apache.hadoop.io.NullWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.util.StatCounter;

public class ProjetSpark {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("Projet Spark");
		JavaSparkContext context = new JavaSparkContext(conf);

		JavaRDD<PhaseWritable> rdd = context.sequenceFile("/user/ptomas/PhaseToSeqFile/part*", NullWritable.class, PhaseWritable.class).values();

		JavaRDD<PhaseWritable> rdd_non_idle = rdd.filter(element -> (!element.getPatterns().equals("-1")));
		JavaRDD<PhaseWritable> rdd_idle = rdd.filter(element -> ((element.getNPatterns() == 0) && (element.getPatterns().equals("-1"))));

		Question1 q1 = new Question1(rdd_non_idle, rdd_idle, rdd);
		Question2 q2 = new Question2(rdd_non_idle);
		Question3 q3 = new Question3(rdd_non_idle);
		Question4 q4 = new Question4(rdd_non_idle);
		Question5 q5 = new Question5(rdd_idle);
		Question6 q6 = new Question6(rdd_non_idle);

		List<String> output = new ArrayList<String>();
		output.add(q1.toString());
		output.add(q2.toString());
		output.add(q3.toString());
		output.add(q4.toString());
		output.add(q5.toString());
		output.add(q6.toString());

		context.parallelize(output).saveAsTextFile("/user/ptomas/ProjetSpark");

		context.close();
	}
}