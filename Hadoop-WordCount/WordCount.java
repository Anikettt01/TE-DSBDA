import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCount {
   public static class IntSumReducer extends ReducerText, IntWritable, Text, IntWritable {
   private IntWritable result = new IntWritable();


   public void reduce(Text key, IterableIntWritable values, ReducerText, IntWritable, Text, IntWritable.Context context) throws IOException, InterruptedException {
      int sum = 0;

      IntWritable val;
      for(Iterator var5 = values.iterator(); var5.hasNext(); sum += val.get()) {
         val = (IntWritable)var5.next();
      }

      result.set(sum);
      context.write(key,result);
   }
}
   public class WordCount$TokenizerMapper extends MapperObject, Text, Text, IntWritable {
      private static final IntWritable one = new IntWritable(1);
      private Text word = new Text();


      public void map(Object key, Text value, MapperObject, Text, Text, IntWritable.Context context) throws IOException, InterruptedException {
         StringTokenizer itr = new StringTokenizer(value.toString());

         while(itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, one);
         }

      }
   }

   public WordCount() {
   }

   public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();
      String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
      if (otherArgs.length  2) {
         System.err.println(Usage wordcount in [in...] out);
         System.exit(2);
      }

      Job job = Job.getInstance(conf, word count);
      job.setJarByClass(WordCount.class);
      job.setMapperClass(TokenizerMapper.class);
      job.setCombinerClass(IntSumReducer.class);
      job.setReducerClass(IntSumReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);

      for(int i = 0; i  otherArgs.length - 1; ++i) {
         FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
      }

      FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
      System.exit(job.waitForCompletion(true)  0  1);
   }
}
