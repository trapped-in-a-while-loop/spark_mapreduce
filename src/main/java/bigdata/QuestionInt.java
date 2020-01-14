package bigdata;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.util.StatCounter;

public abstract class QuestionInt {

	protected Distribution make_distribution(JavaDoubleRDD rdd_values, JavaPairRDD<Long, Integer> rdd_indexed, String name) {

		Distribution distribution = new Distribution(name);

		StatCounter stat = rdd_values.stats();

		distribution.setMin(stat.min());
		distribution.setMax(stat.max());
		distribution.setAvg(stat.mean());
		distribution.setCount(stat.count());

		long size = rdd_indexed.count();

		if (size % 2 == 1)
			distribution.setMed(rdd_indexed.lookup((size - 1) / 2).get(0));
		else 
			distribution.setMed((rdd_indexed.lookup(size / 2).get(0) + rdd_indexed.lookup((size / 2) - 1).get(0)) / 2);
		distribution.setQuar1(rdd_indexed.lookup(size / 4).get(0));
		distribution.setQuar3(rdd_indexed.lookup(size * 3 / 4).get(0));

		return distribution;
	}

}