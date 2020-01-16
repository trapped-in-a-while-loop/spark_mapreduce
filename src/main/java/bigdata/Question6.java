package bigdata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

public class Question6 extends QuestionTotalTime implements Serializable {

	PercentTime answerA;
	TopK answerB;

	Question6(JavaRDD<PhaseWritable> rdd){
		JavaPairRDD<String, Long> rdd_total_time_patterns = get_total_time_patterns(rdd);
		TotalTime total_time_non_idle = get_total_time(rdd, "NON IDLE PHASES TOTAL TIME");
		this.answerA = get_percent_time(rdd_total_time_patterns, total_time_non_idle);
		this.answerB = new TopK(get_top_k(rdd_total_time_patterns), "TOTAL TIME PATTERNS");
	}

	private JavaPairRDD<String, Long> get_total_time_patterns(JavaRDD<PhaseWritable> rdd_non_idle) {

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

		return rdd_total_time_patterns;
	}

	private PercentTime get_percent_time(JavaPairRDD<String, Long> rdd_total_time_patterns, TotalTime total_time_non_idle){

		return new PercentTime("PATTERN ", total_time_non_idle,
				new ArrayList<Tuple2<String, Long>>(rdd_total_time_patterns.collect()));
    }

    @Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("\nQuestion 6\n\n");
		sb.append(this.answerA.toString());
		sb.append(this.answerB.toString());
		return sb.toString();
	}
}