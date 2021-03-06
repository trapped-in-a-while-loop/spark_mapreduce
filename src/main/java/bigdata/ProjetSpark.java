package bigdata;

import java.util.ArrayList;
import java.util.List;

import scala.Tuple2;

import org.apache.hadoop.io.NullWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class ProjetSpark {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("Projet Spark");
		JavaSparkContext context = new JavaSparkContext(conf);

		JavaRDD<PhaseWritable> rdd = context.sequenceFile(args[0], NullWritable.class, PhaseWritable.class).values();

		JavaRDD<PhaseWritable> rdd_non_idle = rdd.filter(element -> (!element.getPatterns().equals("-1")));
		JavaRDD<PhaseWritable> rdd_idle = rdd.filter(element -> ((element.getNPatterns() == 0) && (element.getPatterns().equals("-1"))));

		Question1 q1 = new Question1(rdd_non_idle, rdd_idle, rdd);
		Question2 q2 = new Question2(rdd_non_idle);
		Question3 q3 = new Question3(rdd_non_idle);
		Question4 q4 = new Question4(rdd_non_idle);
		Question5 q5 = new Question5(rdd_idle);
		Question6 q6 = new Question6(rdd_non_idle);
		Integer[] patterns = {2, 5, 8, 9};
		Question7 q7 = new Question7(rdd, patterns);

		List<String> output = new ArrayList<String>();
		output.add(q1.toString());
		output.add(q2.toString());
		output.add(q3.toString());
		output.add(q4.toString());
		output.add(q5.toString());
		output.add(q6.toString());
		output.add(q7.toString());

		context.parallelize(output).saveAsTextFile(args[1]);

		context.close();
	}
}