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

	public static Distribution get_distribution_duration(JavaRDD<PhaseWritable> rdd, String name){

		JavaDoubleRDD rdd_duration = rdd.mapToDouble(element -> element.getDuration());
		JavaPairRDD<Long, Long> rdd_duration_index = rdd.map(element -> element.getDuration())
			.sortBy((Long element) -> element, true, rdd.getNumPartitions()).zipWithIndex().mapToPair(element -> new Tuple2<>(element._2, element._1));

		Distribution distribution = new Distribution(name);

		StatCounter stat = rdd_duration.stats();

		distribution.setMin(stat.min());
		distribution.setMax(stat.max());
		distribution.setAvg(stat.mean());
		distribution.setCount(stat.count());

		long size = rdd_duration_index.count();

		if (size % 2 == 1)
			distribution.setMed(rdd_duration_index.lookup((size - 1) / 2).get(0));
		else 
			distribution.setMed((rdd_duration_index.lookup(size / 2).get(0) + rdd_duration_index.lookup((size / 2) - 1).get(0)) / 2);
		distribution.setQuar1(rdd_duration_index.lookup(size / 4).get(0));
		distribution.setQuar3(rdd_duration_index.lookup(size * 3 / 4).get(0));

		return distribution;
	}

	public static Distribution get_distribution_npatterns(JavaRDD<PhaseWritable> rdd, String name){

		JavaDoubleRDD rdd_npatterns = rdd.mapToDouble(element -> element.getNPatterns());
		JavaPairRDD<Long, Integer> rdd_npatterns_index = rdd.map(element -> element.getNPatterns())
			.sortBy((Integer element) -> element, true, rdd.getNumPartitions()).zipWithIndex().mapToPair(element -> new Tuple2<>(element._2, element._1));

		Distribution distribution = new Distribution(name);

		StatCounter stat = rdd_npatterns.stats();

		distribution.setMin(stat.min());
		distribution.setMax(stat.max());
		distribution.setAvg(stat.mean());
		distribution.setCount(stat.count());

		long size = rdd_npatterns_index.count();

		if (size % 2 == 1)
			distribution.setMed(rdd_npatterns_index.lookup((size - 1) / 2).get(0));
		else 
			distribution.setMed((rdd_npatterns_index.lookup(size / 2).get(0) + rdd_npatterns_index.lookup((size / 2) - 1).get(0)) / 2);
		distribution.setQuar1(rdd_npatterns_index.lookup(size / 4).get(0));
		distribution.setQuar3(rdd_npatterns_index.lookup(size * 3 / 4).get(0));

		return distribution;
	}

	public static Distribution get_distribution_njobs(JavaRDD<PhaseWritable> rdd, String name){

		JavaDoubleRDD rdd_njobs = rdd.mapToDouble(element -> element.getNJobs());
		JavaPairRDD<Long, Integer> rdd_njobs_index = rdd.map(element -> element.getNJobs())
			.sortBy((Integer element) -> element, true, rdd.getNumPartitions()).zipWithIndex().mapToPair(element -> new Tuple2<>(element._2, element._1));

		Distribution distribution = new Distribution(name);

		StatCounter stat = rdd_njobs.stats();

		distribution.setMin(stat.min());
		distribution.setMax(stat.max());
		distribution.setAvg(stat.mean());
		distribution.setCount(stat.count());

		long size = rdd_njobs_index.count();

		if (size % 2 == 1)
			distribution.setMed(rdd_njobs_index.lookup((size - 1) / 2).get(0));
		else 
			distribution.setMed((rdd_njobs_index.lookup(size / 2).get(0) + rdd_njobs_index.lookup((size / 2) - 1).get(0)) / 2);
		distribution.setQuar1(rdd_njobs_index.lookup(size / 4).get(0));
		distribution.setQuar3(rdd_njobs_index.lookup(size * 3 / 4).get(0));

		return distribution;
	}

	public static TotalTime get_total_time(JavaRDD<PhaseWritable> rdd, String name){
		TotalTime init = new TotalTime(name);
		TotalTime result = rdd.aggregate(init,
		(TotalTime t, PhaseWritable p) -> {
			t.addTime(p.getDuration());
			return t;
		}, (TotalTime t1, TotalTime t2) -> {
			t1.addTime(t2.getTime());
			return t1;
		});
		return result;
	}

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("Projet Spark");
		JavaSparkContext context = new JavaSparkContext(conf);

		JavaRDD<PhaseWritable> rdd = context.sequenceFile("/user/ptomas/PhaseToSeqFile/part*", NullWritable.class, PhaseWritable.class).values();
		System.out.println("nb partitions = " + rdd.getNumPartitions());

		JavaRDD<PhaseWritable> rdd_non_idle = rdd.filter(element -> (!element.getPatterns().equals("-1")));
		Distribution dist_non_idle_duration = get_distribution_duration(rdd_non_idle, "NON IDLE PHASES DURATION");
		Distribution dist_non_idle_npatterns = get_distribution_npatterns(rdd_non_idle, "NON IDLE PHASES NPATTERNS");
		Distribution dist_non_idle_njobs = get_distribution_njobs(rdd_non_idle, "NON IDLE PHASES NJOBS");
		TotalTime total_time_non_idle = get_total_time(rdd_non_idle, "NON IDLE PHASES TOTAL TIME");

		JavaRDD<PhaseWritable> rdd_idle = rdd.filter(element -> ((element.getNPatterns() == 0) && (element.getPatterns().equals("-1"))));
		Distribution dist_idle_duration = get_distribution_duration(rdd_idle, "IDLE PHASES DURATION");
		TotalTime total_time_idle = get_total_time(rdd_idle, "IDLE PHASES TOTAL TIME");

		JavaRDD<PhaseWritable> rdd_pattern;
		List<Distribution> dist_patterns_duration = new ArrayList<Distribution>();
		List<TotalTime> total_time_patterns = new ArrayList<TotalTime>();
		
		for (AtomicInteger i = new AtomicInteger(0); i.get() < 11; i.incrementAndGet()){
		//for (AtomicInteger i = new AtomicInteger(); i.get() < 22; i.incrementAndGet()){
			rdd_pattern = rdd.filter((PhaseWritable element) -> (element.getPatterns().equals(i.toString())));
			dist_patterns_duration.add(get_distribution_duration(rdd_pattern, "PATTERN " + i.toString() + " DURATION"));
		}

		JavaPairRDD<String, Long> rdd_total_time_patterns = rdd_non_idle.mapToPair(element -> new Tuple2<>(element.getPatterns(), element.getDuration()));
		rdd_total_time_patterns = rdd_total_time_patterns.flatMapToPair(element -> {
			String patterns[] = element._1.split(",");
			List<Tuple2<String, Long>> result = new ArrayList<Tuple2<String, Long>>();
			if (patterns.length == 1) {
				result.add(new Tuple2<String, Long>(patterns[0], element._2));
			}
			else {
				for (String pattern : patterns) {
					result.add(new Tuple2<String, Long>("S" + pattern, element._2));
				}
			}
			return result.iterator();
		});
		rdd_total_time_patterns = rdd_total_time_patterns.reduceByKey((x, y) -> x + y);

		PercentTime percent_time_patterns = new PercentTime("PATTERN ", total_time_non_idle, new ArrayList<Tuple2<String, Long>>(rdd_total_time_patterns.collect()));

		List<String> output = new ArrayList<String>();
		output.add("\n\nQuestion 1\n\n");
		output.add(dist_non_idle_duration.toString());
		output.add(dist_idle_duration.toString());
		for (int n = 0; n < dist_patterns_duration.size(); ++n){
			output.add(dist_patterns_duration.get(n).toString());
		}

		output.add("\nQuestion 2\n\n");
		output.add(dist_non_idle_npatterns.toString());
		output.add("\nQuestion 3\n\n");
		output.add(dist_non_idle_njobs.toString());

		output.add("\nQuestion 5\n\n");
		output.add(total_time_idle.toString());

		output.add("\nQuestion 6\n\n");
		output.add(percent_time_patterns.toString());

		context.parallelize(output).saveAsTextFile("/user/ptomas/ProjetSpark");

		context.close();
	}
	
}
