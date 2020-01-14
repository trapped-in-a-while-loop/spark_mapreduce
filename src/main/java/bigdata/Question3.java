package bigdata;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

public class Question3 extends QuestionInt {

	Distribution answer;

	Question3(JavaRDD<PhaseWritable> rdd){
		this.answer = get_distribution(rdd, "NON IDLE PHASES NJOBS");
	}

	private Distribution get_distribution(JavaRDD<PhaseWritable> rdd, String name){

		JavaDoubleRDD rdd_njobs = rdd.mapToDouble(element -> element.getNJobs());
		JavaPairRDD<Long, Integer> rdd_njobs_index = rdd.map(element -> element.getNJobs())
			.sortBy((Integer element) -> element, true, rdd.getNumPartitions()).zipWithIndex().mapToPair(element -> new Tuple2<>(element._2, element._1));

		return make_distribution(rdd_njobs, rdd_njobs_index, name);
	}

    @Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("\nQuestion 3\n\n");
		sb.append(this.answer.toString());
		return sb.toString();
	}

}