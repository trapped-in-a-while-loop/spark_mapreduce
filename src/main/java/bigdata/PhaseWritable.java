package bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.Writable;

public class PhaseWritable implements Writable, Serializable {

    private static final long serialVersionUID = 1L;
    
    public long start;
    public long end;
    public long duration; 
    public StringBuilder patterns;
    public int npatterns;
    public StringBuilder jobs; 
    public int njobs;
    public StringBuilder days;
    public int ndays;


    public long getStart() {
        return this.start;
    }

    public void setStart(long start) {
        this.start = start;
    }
    
    public long getEnd() {
        return this.end;
    }

    public void setEnd(long end) {
        this.end = end;
    }
    
    public long getDuration() {
        return this.duration;
    }

    public void setDuration(long duration) {
        this.duration = duration;;
    }

    public String getPatterns() {
        return this.patterns.toString();
    }

    public void setPatterns(String patterns) {
        this.patterns = new StringBuilder(patterns);
    }

    public int getNPatterns() {
        return this.npatterns;
    }

    public void setNPatterns(int npatterns) {
        this.npatterns = npatterns;
    }

    public String getJobs() {
        return this.jobs.toString();
    }

    public void setJobs(String jobs) {
        this.jobs = new StringBuilder(jobs);
    }

    public int getNJobs() {
        return this.njobs;
    }

    public void setNJobs(int njobs) {
        this.njobs = njobs;;
    }
    
    public String getDays() {
        return this.days.toString();
    }

    public void setDays(String days) {
        this.days = new StringBuilder(days);
    }

    public int getNDays() {
        return this.ndays;
    }

    public void setNDays(int ndays) {
        this.ndays = ndays;
    }

    public void readFields(DataInput in) throws IOException {
        this.start = in.readLong();
        this.end = in.readLong();
        this.duration = in.readLong();
        this.patterns = new StringBuilder(in.readUTF());
        this.npatterns = in.readInt();
        this.jobs = new StringBuilder(in.readUTF());
        this.njobs = in.readInt();
        this.days = new StringBuilder(in.readUTF());
        this.ndays = in.readInt();
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(this.start);
        out.writeLong(this.end);
        out.writeLong(this.duration);
        out.writeUTF(this.patterns.toString());
        out.writeInt(this.npatterns);
        out.writeUTF(this.jobs.toString());
        out.writeInt(this.njobs);
        out.writeUTF(this.days.toString());
        out.writeInt(this.ndays);        
    }

    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append(String.valueOf(this.start));
        sb.append(";");
        sb.append(String.valueOf(this.end));
        sb.append(";");
        sb.append(String.valueOf(this.duration));
        sb.append(";");
        sb.append(this.patterns.toString());
        sb.append(";");
        sb.append(String.valueOf(this.npatterns));
        sb.append(";");
        sb.append(this.jobs.toString());
        sb.append(";");
        sb.append(String.valueOf(this.njobs));
        sb.append(";");
        sb.append(this.days.toString());
        sb.append(";");
        sb.append(String.valueOf(this.ndays));
        sb.append(";");
        return sb.toString();
    }
}