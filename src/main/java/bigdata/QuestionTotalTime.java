package bigdata;

import org.apache.spark.api.java.JavaRDD;

public abstract class QuestionTotalTime extends QuestionTopK {

	protected TotalTime get_total_time(JavaRDD<PhaseWritable> rdd, String name){
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

}