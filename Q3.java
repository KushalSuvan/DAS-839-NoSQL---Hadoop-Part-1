
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import javax.naming.Context;
import org.apache.hadoop.io.Writable;


import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.Mapper; 
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.io.LongWritable;


public class Q3 {
    
    public static class InputForMapperReducerTuple implements Writable {
        private Text docID;
        private Text word;

        public InputForMapperReducerTuple() {
            this.docID = new Text();
            this.word = new Text();
        }

        public InputForMapperReducerTuple(String docID, String word) {
            this.docID = new Text(docID);
            this.word = new Text(word);
        }

        public Text getDocID() {
            return this.docID;
        }

        public Text getWord() {
            return this.word;
        }

        public void setDocID(String docID) {
            this.docID.set(docID);
        } 

        public void setWord(String word) {
            this.word.set(word);
        } 

        @Override
        public void write(DataOutput out) throws IOException {
            docID.write(out);
            word.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            docID.readFields(in);
            word.readFields(in);
        }

        @Override 
        public String toString() {
            return this.docID + "\t" + this.word;
        }
    }

    public static class MapperTuple implements Writable {
        private Text timestamp;
        private Text word;

        public MapperTuple() {
            this.timestamp = new Text();
            this.word = new Text();
        }

        public MapperTuple(String timestamp, String word) {
            this.timestamp = new Text(timestamp);
            this.word = new Text(word);
        }

        public Text getTimeStamp() {
            return this.timestamp;
        }

        public Text getWord() {
            return this.word;
        }

        public void setTimestamp(String timestamp) {
            this.timestamp.set(timestamp);
        } 

        public void setWord(String word) {
            this.word.set(word);
        } 

        @Override
        public void write(DataOutput out) throws IOException {
            timestamp.write(out);
            word.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            timestamp.readFields(in);
            word.readFields(in);
        }



        @Override 
        public String toString() {
            return this.timestamp + "\t" + this.word;
        }
    }

    public static class Q3Mapper extends Mapper<LongWritable, Text, LongWritable, Text> {

        private LongWritable indexKey = new LongWritable();
        private Text valueOut = new Text();
        
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] parts = value.toString().split("\\s+");
            if (parts.length < 3) {
                System.err.println("Skipping malformed input: " + value.toString());
                return; // Skip this line if it's malformed
            }
            
            int index = Integer.parseInt(parts[0]);
            int docID = Integer.parseInt(parts[1].replaceAll("[^0-9]", "")); 
            String word = parts[2];

            indexKey.set(index);
            valueOut.set(docID + "," + word);
            context.write(indexKey, valueOut);

        }
    }

    public static class Q3Reducer extends Reducer<LongWritable, Text, LongWritable, Text> {

        private Text result = new Text();
        
        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int maxDocID = Integer.MIN_VALUE;
            String latestWord = "";
            for (Text val : values) {
                String[] parts = val.toString().split(",");
                int docID = Integer.parseInt(parts[0]);
                String word = parts[1];
    
                if (docID > maxDocID) {
                    maxDocID = docID;
                    latestWord = word;
                }
            }
            
            result.set(latestWord);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Index Selection");

        job.setJarByClass(Q3.class);
        job.setMapperClass(Q3Mapper.class);
        job.setReducerClass(Q3Reducer.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
	
	    job.setNumReduceTasks(3);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


