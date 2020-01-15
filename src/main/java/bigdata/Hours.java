package bigdata;

import java.util.ArrayList;
import java.util.List;

import scala.Tuple2;

public class Hours {

    List<String> hours;
    String name;

    Hours(List<String> hours, String name) {
        this.name = name;
        this.hours = new ArrayList<String>(); 
        StringBuilder sb;
        for (String hour : hours) {
            sb = new StringBuilder();
            sb.append(hour);
            this.hours.add(sb.toString());
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
		sb.append("-----------------------------------------------\n");
		sb.append(this.name + "\n");
        sb.append("-----------------------------------------------\n");
        for (String hour: this.hours){
            sb.append(hour + " ");
        }
        sb.append("\n");
        return sb.toString();
    }
}