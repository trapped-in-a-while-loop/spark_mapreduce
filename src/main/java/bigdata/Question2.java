package bigdata;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

public class Question2 extends QuestionInt {

	Distribution answer;

	Question2(JavaRDD<PhaseWritable> rdd) {
        this.answer = get_distribution(rdd, "NON IDLE PHASES NPATTERNS");
	}

	private Distribution get_distribution(JavaRDD<PhaseWritable> rdd, String name){

		JavaDoubleRDD rdd_npatterns = rdd.mapToDouble(element -> element.getNPatterns());
		JavaPairRDD<Long, Integer> rdd_npatterns_index = rdd.map(element -> element.getNPatterns())
			.sortBy((Integer element) -> element, true, rdd.getNumPartitions()).zipWithIndex()
			.mapToPair(element -> new Tuple2<>(element._2, element._1));

		return make_distribution(rdd_npatterns, rdd_npatterns_index, name);
	}
	
    @Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("\nQuestion 2\n\n");
		sb.append(this.answer.toString());
		return sb.toString();
	}
    
}