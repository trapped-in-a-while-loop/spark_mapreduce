package bigdata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

public class Question7 {

    Hours answer;

    Question7(JavaRDD<PhaseWritable> rdd, Integer[] patterns) {
        assert patterns.length == 4;
        List<Integer> list_patterns = new ArrayList<Integer>(Arrays.asList(patterns));
        JavaRDD<PhaseWritable> rdd_filtered = rdd.filter((PhaseWritable element) -> 
            element.getPatterns().contains(String.valueOf(list_patterns.get(0))) && element.getPatterns().contains(String.valueOf(list_patterns.get(1))) &&
            element.getPatterns().contains(String.valueOf(list_patterns.get(2))) && element.getPatterns().contains(String.valueOf(list_patterns.get(3))));
        JavaRDD<String> rdd_patterns_hours = rdd_filtered.map(element -> 
            new Date(element.getStart() / 1000).toString() + "-" + new Date(element.getEnd() / 1000).toString());
        rdd_patterns_hours = rdd_patterns_hours.flatMap(element -> {
            String[] dates = element.split("-");

            int hour_start = Integer.parseInt(dates[0].split(" ")[3].split(":")[0]);
            int hour_end = Integer.parseInt(dates[1].split(" ")[3].split(":")[0]) + 1;

            List<String> result = new ArrayList<String>();
            for (; hour_start < hour_end; ++hour_start) {
                result.add(String.valueOf(hour_start) + "h-" + String.valueOf((hour_start + 1) % 24) + "h");
            }
            return result.iterator();
        });
        rdd_patterns_hours = rdd_patterns_hours.distinct();
        this.answer = new Hours(rdd_patterns_hours.collect(), "PATTERNS PER HOURS");
    }

    @Override
    public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("\nQuestion 7\n\n");
		sb.append(this.answer.toString());
		return sb.toString();
    }
}