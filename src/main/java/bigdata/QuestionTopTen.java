package bigdata;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;

import scala.Tuple2;

public abstract class QuestionTopTen {

    protected class InnerComparator implements Comparator<Tuple2<String, Long>>, Serializable {

        @Override
        public int compare(Tuple2<String, Long> t1, Tuple2<String, Long> t2) {
            return Long.compare(t1._2, t2._2);
        }
    }

    protected List<Tuple2<String, Long>> get_top_ten(JavaPairRDD<String, Long> rdd){
        return rdd.top(10, new InnerComparator());
    }
}