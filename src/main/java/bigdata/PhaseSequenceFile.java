package bigdata;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PhaseSequenceFile extends Configured implements Tool {

    public static class PhaseMapper extends Mapper<Object, Text, NullWritable, PhaseWritable> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String tokens[] = value.toString().split(";");

            if (!tokens[0].equals("start")) {
                PhaseWritable phase = new PhaseWritable();
                phase.setStart(Long.parseLong(tokens[0]));
                phase.setEnd(Long.parseLong(tokens[1]));
                phase.setDuration(Long.parseLong(tokens[2]));
                phase.setPatterns(tokens[3]);
                phase.setNPatterns(Integer.parseInt(tokens[4]));
                phase.setJobs(tokens[5]);
                phase.setNJobs(Integer.parseInt(tokens[6]));
                phase.setDays(tokens[7]);
                phase.setNDays(Integer.parseInt(tokens[8]));

                context.write(NullWritable.get(), phase);
            }
        }
    }

    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        //Path input = new Path("/raw_data/ALCF_repo/phases.csv");
        Path input = new Path("/user/ptomas/phases.csv");
        Path output = new Path("/user/ptomas/PhaseToSeqFile");

        Job job = Job.getInstance(conf, "PhaseSequenceFile");
        job.setJarByClass(PhaseSequenceFile.class);

        job.setMapperClass(PhaseMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(PhaseWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, input);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job, output);

        job.waitForCompletion(true);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new PhaseSequenceFile(), args));
    }
}