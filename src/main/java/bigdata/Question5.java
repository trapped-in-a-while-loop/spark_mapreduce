package bigdata;

import org.apache.spark.api.java.JavaRDD;

public class Question5 extends QuestionTotalTime {

    TotalTime answer;

    Question5(JavaRDD<PhaseWritable> rdd){
        this.answer = get_total_time(rdd, "IDLE PHASES TOTAL TIME");
    }

    @Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("\nQuestion 5\n\n");
		sb.append(this.answer.toString());
		return sb.toString();
	}

}