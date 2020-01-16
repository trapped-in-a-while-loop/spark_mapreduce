package bigdata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

public class Question4 extends QuestionLong implements Serializable {

	Distribution answerA;
	TopK answerB;

	Question4(JavaRDD<PhaseWritable> rdd){
		JavaPairRDD<String, Long> rdd_total_time_jobs = get_total_time_jobs(rdd);
		this.answerA = get_distribution(rdd_total_time_jobs);
		this.answerB = new TopK(get_top_k(rdd_total_time_jobs), "TOTAL TIME JOBS");
	}

	private JavaPairRDD<String, Long> get_total_time_jobs(JavaRDD<PhaseWritable> rdd_non_idle){

		JavaPairRDD<String, Long> rdd_total_time_jobs = rdd_non_idle.mapToPair(element -> new Tuple2<>(element.getJobs(), element.getDuration()));
		rdd_total_time_jobs = rdd_total_time_jobs.flatMapToPair(element -> {
			String jobs[] = element._1.split(",");
            List<Tuple2<String, Long>> result = new ArrayList<Tuple2<String, Long>>();
			for (String job : jobs) {
				result.add(new Tuple2<String, Long>(job, element._2));
			}
			return result.iterator();
		});
		rdd_total_time_jobs = rdd_total_time_jobs.reduceByKey((x, y) -> x + y);
		return rdd_total_time_jobs;
	}

	private Distribution get_distribution(JavaPairRDD<String, Long> rdd_total_time_jobs){
		JavaDoubleRDD rdd_duration = rdd_total_time_jobs.mapToDouble(element -> (double)element._2);
		JavaPairRDD<Long, Long> rdd_duration_index = rdd_total_time_jobs.map(element -> element._2)
			.sortBy((Long element) -> element, true, rdd_total_time_jobs.getNumPartitions()).zipWithIndex()
			.mapToPair(element -> new Tuple2<>(element._2, element._1));

		return make_distribution(rdd_duration, rdd_duration_index, "TOTAL TIME JOBS DURATION");
    }

    @Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("\nQuestion 4\n\n");
		sb.append(this.answerA.toString());
		sb.append(this.answerB.toString());
		return sb.toString();
	}

}