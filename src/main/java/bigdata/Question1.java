package bigdata;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

public class Question1 extends QuestionLong {

    Distribution answerA;
    Distribution answerB;
    List<Distribution> answerC;

    Question1(JavaRDD<PhaseWritable> rdd_non_idle, JavaRDD<PhaseWritable> rdd_idle, JavaRDD<PhaseWritable> rdd) {
        this.answerA = answerA(rdd_non_idle);
        this.answerB = answerB(rdd_idle);
        this.answerC = new ArrayList<Distribution>(answerC(rdd));
    }

	private Distribution get_distribution(JavaRDD<PhaseWritable> rdd, String name){

		JavaDoubleRDD rdd_duration = rdd.mapToDouble(element -> element.getDuration());
		JavaPairRDD<Long, Long> rdd_duration_index = rdd.map(element -> element.getDuration())
			.sortBy((Long element) -> element, true, rdd.getNumPartitions()).zipWithIndex()
			.mapToPair(element -> new Tuple2<>(element._2, element._1));

		return make_distribution(rdd_duration, rdd_duration_index, name);
	}

    private Distribution answerA(JavaRDD<PhaseWritable> rdd){
        return get_distribution(rdd, "NON IDLE PHASES DURATION");
    }

    private Distribution answerB(JavaRDD<PhaseWritable> rdd){
        return get_distribution(rdd, "IDLE PHASES DURATION");
    }

    private List<Distribution> answerC(JavaRDD<PhaseWritable> rdd) {
        JavaRDD<PhaseWritable> rdd_pattern;
        List<Distribution> dist_patterns_duration = new ArrayList<Distribution>();

        for (AtomicInteger i = new AtomicInteger(); i.get() < 22; i.incrementAndGet()){
            rdd_pattern = rdd.filter((PhaseWritable element) -> (element.getPatterns().equals(i.toString())));
            dist_patterns_duration.add(get_distribution(rdd_pattern, "PATTERN " + i.toString() + " DURATION"));
        }

        return dist_patterns_duration;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n\nQuestion 1\n\n");
        sb.append(this.answerA.toString());
        sb.append(this.answerB.toString());
		for (int n = 0; n < this.answerC.size(); ++n){
			sb.append(this.answerC.get(n).toString());
        }
        return sb.toString();
    }
}