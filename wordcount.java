import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.input.FileSplit; 
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.*;
import org.xml.sax.HandlerBase;

public class wordcount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    private Text word = new Text();
    private Text info = new Text();
    String filename;
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      int count=0;
      StringTokenizer itr = new StringTokenizer(value.toString());
      String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
      while (itr.hasMoreTokens()) {
    	  count++;
    	  word.set(itr.nextToken());
    	  info.set(fileName.concat("%").concat(String.valueOf(count)));
    	  context.write(word, info);
      }
    }
  }
  public static class TokenizerMapper1
  extends Mapper<Object, Text, Text, Text>{

private Text word = new Text();
private Text info1 = new Text();
String filename;
private HashMap<String,HashMap<String,ArrayList<Integer>>> wordfreq = new HashMap<String,HashMap<String,ArrayList<Integer>>>();


public void map(Object key, Text value, Context context
               ) throws IOException, InterruptedException {
	
	StringBuilder info = new StringBuilder();
	String[] mainarray = value.toString().split("\\##");
	for(int x=0;x<mainarray.length;x++){
	
	StringBuilder keyval = new StringBuilder();
   // String[] keys[] = value.toString().a
	String[] mainarray2 = mainarray[x].split("\\#\\$");
	HashMap<String,ArrayList<Integer>> temp = new HashMap<String,ArrayList<Integer>>();
	 String[] info1array1 = mainarray2[0].split("\\$");
	 ArrayList<Integer> positions2 = new ArrayList<Integer>();
	  String[] wordkey1 = info1array1[0].split("\\s+");
	  System.out.println(wordkey1[0]);
	  keyval.append(wordkey1[0]);
	  if(!wordfreq.containsKey(wordkey1[0]))
	  	  wordfreq.put(wordkey1[0], temp);
	  temp = wordfreq.get(wordkey1[0]);
	  System.out.println("=================");
	  System.out.println(wordkey1[1]);
	  String[] temp1s = wordkey1[1].split("\\%");
	  if(temp.containsKey(temp1s[0])){
		  System.out.println(temp1s[0]);
		 System.out.println(temp1s[1]);
		 System.out.println("=================");
		  positions2 = temp.get(temp1s[0]);
		  positions2.add(Integer.parseInt(temp1s[1]));
		  temp.put(temp1s[0],positions2);
	  }
	  else{
		  positions2.add(Integer.parseInt(temp1s[1]));
		  temp.put(temp1s[0],positions2);
	  }
	  for(int i=1;i<info1array1.length-1;i++){
    	  ArrayList<Integer> positions1 = new ArrayList<Integer>();
    	  String[] temp1 = info1array1[i].split("\\%");
    	 	  if(temp.containsKey(temp1[0])){
        		  positions1 = temp.get(temp1[0]);
        		  positions1.add(Integer.parseInt(temp1[1]));
        		  temp.put(temp1[0],positions1);
        	  }
        	  else{
        		  positions1.add(Integer.parseInt(temp1[1]));
        		  temp.put(temp1[0],positions1);
        	  }
       }
	
	  for(int y=1;y<mainarray2.length;y++){
		
	
	  String[] info1array = mainarray2[y].split("\\$");
//	  ArrayList<Integer> positions = new ArrayList<Integer>();
//	  String[] wordkey = info1array[0].split("\\s+");
//	  System.out.println(wordkey[0]);
//	  keyval.append(wordkey[0]);
//	  if(!wordfreq.containsKey(wordkey[0]))
//	  	  wordfreq.put(wordkey[0], temp);
//	  temp = wordfreq.get(wordkey[0]);
//	  System.out.println("=================");
//	  System.out.println(wordkey[1]);
//	  String[] temps = wordkey[1].split("\\%");
//	  if(temp.containsKey(temps[0])){
//		  System.out.println(temps[0]);
//		 System.out.println(temps[1]);
//		 System.out.println("=================");
//		  positions = temp.get(temps[0]);
//		  positions.add(Integer.parseInt(temps[1]));
//		  temp.put(temps[0],positions);
//	  }
//	  else{
//		  positions.add(Integer.parseInt(temps[1]));
//		  temp.put(temps[0],positions);
//	  }
	  for(int i=0;i<info1array.length-1;i++){
    	  ArrayList<Integer> positions1 = new ArrayList<Integer>();
    	  String[] temp1 = info1array[i].split("\\%");
    	 	  if(temp.containsKey(temp1[0])){
        		  positions1 = temp.get(temp1[0]);
        		  positions1.add(Integer.parseInt(temp1[1]));
        		  temp.put(temp1[0],positions1);
        	  }
        	  else{
        		  positions1.add(Integer.parseInt(temp1[1]));
        		  temp.put(temp1[0],positions1);
        	  }
    	  }
	}
	  wordfreq.remove(keyval);
      wordfreq.put(keyval.toString(), temp);
      Iterator<Entry<String, ArrayList<Integer>>> it = temp.entrySet().iterator();
      while (it.hasNext()) {
          Map.Entry<String,ArrayList<Integer>> pair = (Map.Entry<String,ArrayList<Integer>>)it.next();
          info.append(pair.getKey()).append("%").append(String.valueOf(pair.getValue().size())).append("$").append(pair.getValue().toString());
          //info.append("|");
      }
	  word.set(keyval.toString());
	  info1.set(info.toString());
	  context.write(word, info1);
	}
 }
}

  
  public static class IntSumReducer
  extends Reducer<Text,Text,Text,Text> {
private Text result = new Text();
public void reduce(Text key, Iterable<Text> values,
                  Context context
                  ) throws IOException, InterruptedException {
	
 StringBuilder info = new StringBuilder();
 for (Text vals : values) {
	  info.append(vals).append("$");
 }
 info.deleteCharAt(info.length()-1);
 info.append("#");
 result.set(info.toString());
 context.write(key, result);
}
}

  public static class IntSumReducer1
       extends Reducer<Text,Text,Text,Text> {
	  private Text result = new Text();
	  public void reduce(Text key, Iterable<Text> values,
	                    Context context
	                    ) throws IOException, InterruptedException {
	  	
	   StringBuilder info = new StringBuilder();
	   for (Text vals : values) {
	  	  info.append(vals).append("$");
	   }
	   info.deleteCharAt(info.length()-1);
	   result.set(info.toString());
	   context.write(key, result);
	  }
//    private Text result = new Text();
//    private HashMap<String,HashMap<String,ArrayList<Integer>>> wordfreq = new HashMap<String,HashMap<String,ArrayList<Integer>>>();
//    private HashMap<String,ArrayList<Integer>> temp = new HashMap<String,ArrayList<Integer>>();
//    
//    public void reduce(Text key, Iterable<Text> values,
//                       Context context
//                       ) throws IOException, InterruptedException {
//    	
//      StringBuilder info = new StringBuilder();
//      StringBuilder info1 = new StringBuilder();
//      if(!wordfreq.containsKey(key.toString()))
//    	  wordfreq.put(key.toString(), temp);
//      temp = wordfreq.get(key.toString());
//      for (Text vals : values) {
//    	  info1.append(vals).append("$");
//      }
//      info1.deleteCharAt(info1.length()-1);
//      String[] info1array = info1.toString().split("$");
//      for(int i=0;i<info1array.length;i++){
//    	  System.out.println(key.toString());
//    	  String[] temps = info1array[i].split("%");
//    	  System.out.println("=============");
//    	  System.out.println(info1array[0]);
//    	  ArrayList<Integer> positions = new ArrayList<Integer>();
//      
//    	  if(temp.containsKey(temps[0])){
//    		  positions = temp.get(temps[0]);
//    		  positions.add(Integer.parseInt(temps[1]));
//    		  temp.put(temps[0],positions);
//    	  }
//    	  else{
//    		  positions.add(Integer.parseInt(temps[1]));
//    		  temp.put(temps[0],positions);
//    	  }
//      }
//      wordfreq.put(key.toString(), temp);
//      Iterator<Entry<String, ArrayList<Integer>>> it = temp.entrySet().iterator();
//      while (it.hasNext()) {
//          Map.Entry<String,ArrayList<Integer>> pair = (Map.Entry<String,ArrayList<Integer>>)it.next();
//          info.append(pair.getKey()).append(String.valueOf(pair.getValue().size())).append(",").append(pair.getKey().toString());
//          info.append("|");
//      }
//      result.set(info.toString());
//      context.write(key, result);
//    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = new Job(conf, "word count");
    job.setJarByClass(wordcount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0])); //these are the arguments given when the hadoop execute commad
    FileOutputFormat.setOutputPath(job, new Path("/user/embed/output"));
    job.waitForCompletion(true);
    
    Configuration conf1 = new Configuration();
    Job job1 = new Job(conf1, "inverted index");
    job1.setJarByClass(wordcount.class);
    job1.setMapperClass(TokenizerMapper1.class);
    job1.setCombinerClass(IntSumReducer1.class);
    job1.setReducerClass(IntSumReducer1.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job1, new Path("/user/embed/output/part-r-00000")); //these are the arguments given when the hadoop execute commad
    FileOutputFormat.setOutputPath(job1, new Path("/user/embed/output2"));    
    System.exit(job1.waitForCompletion(true) ? 0 : 1);
  }
}