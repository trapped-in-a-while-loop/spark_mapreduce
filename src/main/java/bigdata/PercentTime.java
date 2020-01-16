package bigdata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import scala.Tuple2;

public class PercentTime implements Serializable {

    public static final int NPATTERNS = 22;
    private final String name;
    private Map<String, Double> patterns = new HashMap<String, Double>();

    PercentTime(String name, TotalTime total_time_non_idle, ArrayList<Tuple2<String, Long>> patterns_duration) {
        this.name = name;
        for (Tuple2<String, Long> pattern : patterns_duration) {
            this.patterns.put(pattern._1, (((double) pattern._2) / total_time_non_idle.getTime()) * 100.0);
        }
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < NPATTERNS; ++i){
            sb.append("-----------------------------------------------\n");
            sb.append(this.name + String.valueOf(i) + "\n");
            sb.append("-----------------------------------------------\n");
            for (int j = 0; j < 2; ++j){
                if (j == 1){
                    sb.append("simultaneous time percent is = ");
                    if (this.patterns.containsKey("S" + String.valueOf(i))){
                        sb.append(this.patterns.get("S" + String.valueOf(i)) + "%\n\n");
                    }
                    else{
                        sb.append(0.0 + "%\n\n");
                    }
                } 
                else{
                    sb.append("solo time percent is = ");
                    if (this.patterns.containsKey(String.valueOf(i))){
                        sb.append(this.patterns.get(String.valueOf(i)) + "%\n");
                    }
                    else{
                        sb.append(0.0 + "%\n");
                    }
                }
            }
        }
        return sb.toString();
    }
}