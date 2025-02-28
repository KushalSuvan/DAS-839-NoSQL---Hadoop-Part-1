import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import javax.naming.Context;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.Mapper; 
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


class MapperTuple implements Writable {
    private IntWritable index;
    private Text word;

    public MapperTuple() {
        this.index = new IntWritable();
        this.word = new Text();
    }

    public MapperTuple(int index, String word) {
        this.index = new IntWritable(index);
        this.word = new Text(word);
    }

    public IntWritable getIndex() {
        return this.index;
    }

    public Text getWord() {
        return this.word;
    }

    public void setIndex(int index) {
        this.index.set(index);
    } 

    public void setWord(String word) {
        this.word.set(word);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        index.write(out);
        word.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        index.readFields(in);
        word.readFields(in);
    }

    @Override 
    public String toString() {
        return this.index + "\t" + this.word;
    }
}


class ReducerTuple implements Writable {
    private Text docID;
    private Text word;

    public ReducerTuple() {
        this.docID = new Text();
        this.word = new Text();
    }

    public ReducerTuple(String docID, String word) {
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

public class Q2 {
    
    public static class Q2Mapper extends Mapper<Object, Text, Text, MapperTuple> {

        Text docID = new Text();
        MapperTuple value = new MapperTuple();

        @Override
        public void map(Object key, Text str, Context context) throws IOException, InterruptedException {
            String line = str.toString();
            String[] tokens = line.split("[^\\w']+");
            
            //to get the docID
           FileSplit fileSplit = (FileSplit) ((org.apache.hadoop.mapreduce.lib.input.FileSplit) context.getInputSplit());
            docID.set(fileSplit.getPath().getName());


            for(int i=0; i<tokens.length; i++) {
                value.setIndex(i);
                value.setWord(tokens[i]);
                context.write(docID, value);
            }
        }
    }

    public static class Q2Reducer extends Reducer<Text, MapperTuple, IntWritable, ReducerTuple> {

        ReducerTuple finalValue = new ReducerTuple();
        Text docID = new Text();

        @Override
        public void reduce(Text key, Iterable<MapperTuple> values, Context context) throws IOException, InterruptedException {
            docID.set(key);
            for (MapperTuple val : values) {
                IntWritable newIndex = new IntWritable(val.getIndex().get());
                Text newWord = new Text(val.getWord().toString());
                context.write(newIndex, new ReducerTuple(key.toString(), newWord.toString()));
            }            
            
        }
    }

    public static void main(String[] args) throws Exception {
        
        Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "Q2");

        job.setJarByClass(Q2.class);
	    job.setMapperClass(Q2Mapper.class);
	    job.setReducerClass(Q2Reducer.class); 

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapperTuple.class);

	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(ReducerTuple.class);

	    job.setNumReduceTasks(10);

	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));

	    System.exit(job.waitForCompletion(true) ? 0 : 1); 
    }
}
