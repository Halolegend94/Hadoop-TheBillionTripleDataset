import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Homework
 * 
 * This is the implementation 
 * @author cristian
 *
 */

public class Homework {

	/**
     * tokenize_line
     * returns an ArrayList of strings each of them representing a RDF token parsed from 
     * the input string.
     * 
     * Description: tokenize an RDF line, splitting it in its constituent parts.
     */
	public static ArrayList<String> tokenize_line(String line){
            ArrayList<String> tokens = new ArrayList<String>();
            StringBuffer buff = new StringBuffer();
      
            for (int i = 0; i < line.length(); i++){
            Character ch = line.charAt(i);
            if (ch == ' ' || ch == '\n' || ch == '\t'){
                  //token parsed
                  String result = buff.toString();
                  if (!result.equals(".") && result.length() > 0)
                        tokens.add(result);
                  buff = new StringBuffer();
            }else if (ch == '"'){
                  int escape = 0;
                  boolean escaped = false;
                  do{
                        buff.append(ch);
                        i+=1;
                        ch = line.charAt(i);
                        if(ch == '\\'){
                              escape++;
                        }else if(escape > 0){
                              escaped = escape % 2 == 1;
                              escape = 0;
                        }else{
                              escaped = false;
                        }
                  }while(!(!escaped && ch == '"'));
                        buff.append('"');
            }else if(ch == '<'){
                  do{
                        buff.append(ch);
                        i+=1;
                        ch = line.charAt(i);
                  }while(ch != '>');
                  buff.append('>');
            }else{
                  buff.append(ch);
            }
      }
      return tokens;
  }
    
    // ================ JOB 1 - Unique triples ========================================
    
    /**
     * UniqueTripleMapper
     * 
     * Description: This mapper associate to each triple its contexts.
     * @author cristian
     *
     */
    public static class UniqueTriplesMapper extends Mapper<Object, Text, Text, Text>{
    	public void map(Object key, Text value, Context context)
	        throws IOException, InterruptedException {
	        
	        ArrayList<String> tokens = tokenize_line(value.toString());
	
	        // The map key is the triple itself, the value is the context
	        // We are guaranteed that there are at least 3 tokens
	        StringBuffer newKey = new StringBuffer();
	        for(int i = 0; i < 3; i++) newKey.append(tokens.get(i) + " ");
	        // Now get the value
	        String newValue = null;
	        if(tokens.size() == 4){
	              //there is the context too
	              newValue = tokens.get(3);
	        }else{
	              newValue = ""; //no context
	        }
	        context.write(new Text(newKey.toString()), new Text(newValue)); 
    	}
    }
    
    /**
     * UniqueTripleReducer
     * 
     * Description: takes a triple and its associated list of contexts and computes the
     * total number of contexts, the number of distinct contexts, and the number of empty
     * contexts.
     * @author cristian
     *
     */
    public static class UniqueTripleReducer extends Reducer<Text, Text, Text, Text>{
    	
    	public void reduce(Text key, Iterable<Text> values, Context context)
    			throws IOException, InterruptedException{
    		
    		StringBuffer buff = new StringBuffer();
    		HashSet<Text> set = new HashSet<Text>();
    		int counter = 0;
    		int empty_context = 0;
    		for (Text t : values){
    			set.add(t);
    			counter++;
    			if (t.toString().equals("")) empty_context++;
    		}
    		buff.append(counter + " " + set.size() + " " + empty_context + " ");
    		context.write(key, new Text(buff.toString()));
    	}
    }
    
    // =============== JOB 2: triples with the largest number of different contexts ============
    
    /**
     * Map each triple to its count of distinct contexts, so we can see which triple has the highest
     * number of distinct contexts.
     * Plus, we map to special (negative) integers the number of blank subject and object, and the
     * number of empty context.
     * @author cristian
     *
     */
    public static class TriplesToContextCountMapper extends Mapper<Object, Text, IntWritable, Text>{
    	public void map(Object key, Text value, Context context)
	        throws IOException, InterruptedException {
	        
	        ArrayList<String> tokens = tokenize_line(value.toString());
	        
	        String total_count = tokens.get(3);
	        int distinct_count = Integer.valueOf(tokens.get(4));
	        int empty_context_count = Integer.valueOf(tokens.get(5)); //SPECIAL KEY: -1
	        
	        boolean blank_subj = tokens.get(0).startsWith("_:");  //SPECIAL KEY: -3
	        boolean blank_obj = tokens.get(2).startsWith("_:");   // SPECIAL KEY: -2
	        
	        StringBuffer newValue = new StringBuffer();
	        for(int i = 0; i < 3; i++) newValue.append(tokens.get(i) + " ");
	        
	        //Associate the each triple to the count (Question 6)
	        context.write(new IntWritable(distinct_count), new Text(newValue.toString()));
	        
	        //Question 5
	        context.write(new IntWritable(-1), new Text(String.valueOf(empty_context_count)));
	        if(blank_obj)
	        	context.write(new IntWritable(-2), new Text(total_count));
	        if(blank_subj)
	        	context.write(new IntWritable(-3), new Text(total_count));
    	}
    }
    
    /**
     * takes a special integer (-1, -2, -3) representing blanks or empty context, or a positive integer
     * representing the count of distinct context, as key, a list of associated values, and
     *  - for the positive integers, write the associated triples along the distinct count.
     *  - for the three negatve integers, sum each value in the list of values.
     * @author cristian
     *
     */
    public static class TriplesToContextCountReducer extends Reducer<IntWritable, Text, IntWritable, Text>{
    	
    	public void reduce(IntWritable key, Iterable<Text> values, Context context)
    			throws IOException, InterruptedException{
    		
    		
    		if(key.get() >= 0){ // question 6
	    		for (Text t : values){
	    			context.write(key, t);
	    		}
    		}else{
    			long sum = 0;
    			for (Text v : values){
    				sum += Integer.valueOf(v.toString().trim());
    			}
    			context.write(key, new Text(String.valueOf(sum)));
    		}
    	}
    }
    
    // ===================================JOB 3 PART 1 =============================================
    /**
     * count for each node the number of incoming and outcoming edges
     * @author cristian
     *
     */
    public static class NodeToDegreeMapper extends Mapper<Object, Text, Text, Text>{
    	public void map(Object key, Text value, Context context)
	        throws IOException, InterruptedException {
	        
	        ArrayList<String> tokens = tokenize_line(value.toString());
	        context.write(new Text(tokens.get(0)), new Text("o"));
	        context.write(new Text(tokens.get(2)), new Text("i"));
    	}
    }
    
    public static class NodeToDegreeReducer extends Reducer<Text, Text, Text, Text>{
    	
    	public void reduce(Text key, Iterable<Text> values, Context context)
    			throws IOException, InterruptedException{
    		
    		int indegree = 0;
    		int outdegree = 0;
    		
    		for(Text v : values){
    			if(v.toString().equals("o")){
    				outdegree++;
    			}else{
    				indegree++;
    			}
    		}
    		
    		context.write(key, new Text(indegree + " " + outdegree + " "));
    	}
    }
    
    
    // =================================== JOB 4  =============================================
    
    public static int hashNode(String node){
    	int hash = 0;
    	for(Character ch : node.toCharArray()){
    		hash += (int) ch;
    	}
    	return hash % 100;
    }
    
    public static class DegreeDistributionMapper extends Mapper<Object, Text, Text, IntWritable>{
    	public void map(Object key, Text value, Context context)
	        throws IOException, InterruptedException {
	        
	        ArrayList<String> tokens = tokenize_line(value.toString());
	        
	        int nh = hashNode(tokens.get(0));
	        
	        StringBuffer buff1 = new StringBuffer();
	        buff1.append("i"+tokens.get(1) + "_" + nh);
	        Text newKey1 = new Text(buff1.toString());
	        
	        StringBuffer buff2 = new StringBuffer();
	        buff2.append("o"+tokens.get(2) + "_" + nh);
	        Text newKey2 = new Text(buff2.toString());
	        	        
	        context.write(newKey1, new IntWritable(1));
	        context.write(newKey2, new IntWritable(1));
    	}
    }
    
    public static class DegreeDistributionReducer extends Reducer<Text, IntWritable, Text,IntWritable>{
    	
    	public void reduce(Text key, Iterable<IntWritable> values, Context context)
    			throws IOException, InterruptedException{
    		
    		int sum = 0;
    		
    		for(IntWritable v : values){
    			sum += v.get();
    		}
    		
    		context.write(key, new IntWritable(sum));
    	}
    }
    
// =================================== JOB 5 =============================================
    public static String[] parsePartialDistr(String s){
    	String vals[] = new String[2];
    	StringBuffer buff = new StringBuffer();
    	boolean append = true;
    	for (Character ch : s.toCharArray()){
    		if(ch == '_'){
    			vals[0] = buff.toString();
    			buff = new StringBuffer();
    			append = false;
    		}else if(ch == '\t'){
    			append = true;
    		}else if(append){
    			buff.append(ch);
    		}
    	}
    	vals[1] = buff.toString();
    	return vals;
    }
    
    public static class FinalDegreeDistributionMapper extends Mapper<Object, Text, Text, IntWritable>{
    	public void map(Object key, Text value, Context context)
	        throws IOException, InterruptedException {
	        
    		String vals[] = parsePartialDistr(value.toString());
    		int partial_count = Integer.valueOf(vals[1]);
	        context.write(new Text(vals[0]), new IntWritable(partial_count));
    	}
    }
    
    // ============================ JOB 6 ========================================================
    
    public static class outdegreeToNodeMapper extends Mapper<Object, Text, IntWritable, Text>{
    	public void map(Object key, Text value, Context context)
	        throws IOException, InterruptedException {        
	        ArrayList<String> tokens = tokenize_line(value.toString());        	        
	        context.write(new IntWritable(Integer.valueOf(tokens.get(2))), new Text(tokens.get(0)));
	
    	}
    }
    
    public static class outdegreeToNodeReducer extends Reducer<IntWritable, Text, IntWritable,Text>{
    	
    	public void reduce(IntWritable key, Iterable<Text> values, Context context)
    			throws IOException, InterruptedException{
    		
    		for(Text v : values){
    	 		context.write(key, v);
    		}
    	}
    }
    
    public static void main(String[] args) throws Exception{
    	
    	// Question 7
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Questions7");
        job.setJarByClass(Homework.class);
        job.setMapperClass(UniqueTriplesMapper.class);
        job.setReducerClass(UniqueTripleReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("/input"));
        FileOutputFormat.setOutputPath(job, new Path("/output1"));
        job.waitForCompletion(true);
        
        // Question 6 - 5
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Questions6");
        job2.setJarByClass(Homework.class);
        job2.setMapperClass(TriplesToContextCountMapper.class);
        job2.setReducerClass(TriplesToContextCountReducer.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path("/output1/part-r-*")); //CHANGE
        FileOutputFormat.setOutputPath(job2, new Path("/output2"));
        job2.waitForCompletion(true);
        
        // Question 1 2 3 4 - PART 1
        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance(conf3, "Questions1234Part1");
        job3.setJarByClass(Homework.class);
        job3.setMapperClass(NodeToDegreeMapper.class);
        job3.setReducerClass(NodeToDegreeReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, new Path("/output1/part-r-*")); //CHANGE
        FileOutputFormat.setOutputPath(job3, new Path("/output3"));
        job3.waitForCompletion(true);
        
       // Question 1 2 3 4 - PART 2
        Configuration conf4 = new Configuration();
        Job job4 = Job.getInstance(conf4, "Questions1234Part2");
        job4.setJarByClass(Homework.class);
        job4.setMapperClass(DegreeDistributionMapper.class);
        job4.setCombinerClass(DegreeDistributionReducer.class);
        job4.setReducerClass(DegreeDistributionReducer.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job4, new Path("/output3/part-r-*")); //CHANGE
        FileOutputFormat.setOutputPath(job4, new Path("/output4"));
        job4.waitForCompletion(true);
        
        // Question 1 2 3 4 - PART 3
        Configuration conf5 = new Configuration();
        Job job5 = Job.getInstance(conf5, "Questions1234Part3");
        job5.setJarByClass(Homework.class);
        job5.setMapperClass(FinalDegreeDistributionMapper.class);
        job5.setCombinerClass(DegreeDistributionReducer.class);
        job5.setReducerClass(DegreeDistributionReducer.class);
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job5, new Path("/output4/part-r-*")); //CHANGE
        FileOutputFormat.setOutputPath(job5, new Path("/output5"));
        job5.waitForCompletion(true);
        
        //Question 4
        Configuration conf6 = new Configuration();
        Job job6 = Job.getInstance(conf6, "Questions4");
        job6.setJarByClass(Homework.class);
        job6.setMapperClass(outdegreeToNodeMapper.class);
        job6.setReducerClass(outdegreeToNodeReducer.class);
        job6.setOutputKeyClass(IntWritable.class);
        job6.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job6, new Path("/output3/part-r-*")); //CHANGE
        FileOutputFormat.setOutputPath(job6, new Path("/output6"));
        job6.waitForCompletion(true);
        
     }
      
}
