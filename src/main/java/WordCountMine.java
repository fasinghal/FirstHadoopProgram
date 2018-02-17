import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

public class WordCountMine {

    public static class MyMapper extends Mapper<Object, Text, Text, IntWritable>{

        final static IntWritable one = new IntWritable(1);
       // String word;

        private HashMap<String, Integer> map ;
        int newValue;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            map=new HashMap<String, Integer>();
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer st = new StringTokenizer(value.toString());
            while(st.hasMoreTokens()){
            String word = st.nextToken();
                newValue=0;

                if(map.containsKey(word)){
                    newValue = map.get(word)+1;
                    map.put(word, newValue);
                }
                else{
                    newValue=1;
                    map.put(word, newValue);
                }
            }

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            System.out.println(map);
            for (String key: map.keySet()) {
                context.write(new Text(key), new IntWritable(map.get(key)));
            }
        }
    }

    public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable >{

        public IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum+=val.get();

            }
            result.set(sum);
            context.write(key,result);

        }
    }

    public static class KeyValueMappingSwapper extends Mapper<Text, IntWritable, IntWritable, Text>{
        @Override
        protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
                context.write(value, key);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        Path out = new Path(args[1]);
        Job job = Job.getInstance(configuration, "my Word Count");

        job.setJarByClass(WordCountMine.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setCombinerClass(MyReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(out, "out1"));

        if (!job.waitForCompletion(true)) {
            System.exit(1);
        }

        //------------------------------------------------------
        Job job2 = Job.getInstance(configuration, "Frequency Sort");
        job2.setJarByClass(WordCountMine.class);

        job2.setMapperClass(KeyValueMappingSwapper.class);
        job2.setNumReduceTasks(1);
        job2.setSortComparatorClass(IntWritable.Comparator.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job2, new Path(out, "out1"));
        FileOutputFormat.setOutputPath(job2, new Path(out, "out2"));
        if (!job2.waitForCompletion(true)) {
            System.exit(1);
        }
    }
}
