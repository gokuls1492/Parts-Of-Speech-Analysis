package WordCount.WordCount;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class POSCount {

  public static class TokenizerMapper
       extends Mapper<LongWritable, Text, IntWritable, Text >{
    //private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private  Map<String, String> wordList = null;
    
    //public Map<String, String> loadWords() throws Exception{    	
    @Override
    public void setup(Context context) {
    	Configuration conf = context.getConfiguration();
    	//Path pt = new Path("/user/gokul/hw1b/mobyposi.i");
    	Path pt = new Path("/user/gxs161530/mobyposi.i");
    	BufferedReader br;
    	try {
    	//FileSystem fs = FileSystem.get(new Configuration());
    	FileSystem fs = FileSystem.get(conf);
    	br=new BufferedReader(new InputStreamReader(fs.open(pt)));
    	wordList = new HashMap<String, String>();
    	String line, word, type;
    	char ch;
    	  while ((line=br.readLine())!= null){
    		  word = line.substring(0,line.indexOf("*"));
    		  type = line.substring(line.indexOf("*")+1);
    		  for(int i=0;i<type.length();i++){
    			  ch = type.charAt(i);
    			  switch (ch){
    			  	case 'N' :  wordList.put(word, "noun");
    			  				break;
    			  	case 'p' :  wordList.put(word, "plural");
	  							break;
    			  	case 'V' :  wordList.put(word, "verb");
    			  				break;
    			  	case 't' :  wordList.put(word, "verb");
    			  				break;
    			  	case 'i' :	wordList.put(word, "verb");
    			  				break;
    			  	case 'A' :  wordList.put(word, "adjective");
    			  				break;
    			  	case 'v' :  wordList.put(word, "adverb");
	  							break;
    			  	case 'C' :  wordList.put(word, "conjunction");
	  							break;
    			  	case 'P' :  wordList.put(word, "preposition");
	  							break;
    			  	case 'r' :  wordList.put(word, "pronoun");
	  							break;
    			  	case 'D' :  wordList.put(word, "definite article");
	  							break;
    			  	case 'I' :  wordList.put(word, "indefinite article");
								break;
    			  	case 'o' :  wordList.put(word, "nominative");
    			  				break;
    			  }
    		 }
    	   }
    	} catch(Exception e) {
    	  e.printStackTrace();
    	}
    //	return map;
    }
	  public static boolean isPalindrome(String str) {
	        return str.equals(new StringBuilder(str).reverse().toString());
	    }

    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      String token;
      int len=0,pol;
      try {
		 while (itr.hasMoreTokens()) {
			 pol=0;
			 token = itr.nextToken().trim().toLowerCase();   
			 len = token.length();
			 if(wordList.containsKey(token) && len>=5){	
				if (isPalindrome(token))
					pol=1;
				 word.set(Integer.toString(pol)+wordList.get(token));
				 context.write(new IntWritable(len), word);
				}
		  }
	} catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
		}     
    }
  }

  public static class IntSumReducer
       extends Reducer<IntWritable,Text, Text, Text> {
 //   private IntWritable result = new IntWritable();
    public void reduce(IntWritable key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      //int k = Integer.parseInt(key.toString());
      Iterator<Text> valuesIterator = values.iterator();
      int count=0,polin,npol=0;
      int noun=0,plural=0,verb=0,adj=0,adv=0,conj=0,prep=0,inter=0,pron=0,da=0,ida=0,nom=0;
      String str,type1;
      while(valuesIterator.hasNext())               
      {
    	  str= valuesIterator.next().toString();
    	  polin = Character.getNumericValue(str.toCharArray()[0]);
    	  type1 = str.substring(1);
    	  if(type1.equals("noun"))
			  noun++;
		  else if(type1.equals("plural"))
			  plural++;  
		  else if(type1.equals("verb"))
			  verb++;
		  else if(type1.equals("adjective"))
			  adj++;
		  else if(type1.equals("adverb"))
			  adv++;
		  else if(type1.equals("conjunction"))
			  conj++;
		  else if(type1.equals("preposition"))
			  prep++;
		  else if(type1.equals("pronoun"))
			  pron++;
		  else if(type1.equals("definite article"))
			  da++;
		  else if(type1.equals("indefinite article"))
			  ida++;
		  else if(type1.equals("nominative"))
			  nom++; 
    		  
		  count++;
		  if(polin==1)
			  npol++;
      }
      context.write(new Text("Length :"),new Text(Integer.toString(key.get())));
      context.write(new Text("Count of Words :"), new Text(Integer.toString(count)));      
      context.write(new Text("Distribution of POS :"), new Text("{ noun: "+ noun+";"+"adjective: "+ adj+";"+" adverb: "+ adv+";"+" conjunction: "+ conj+";"+" preposition: "+ prep+";"+" pronoun: "+ pron+";"+" plural: "+ plural+";"+" definite article: "+ da+";"+" indefinite article: "+ ida+";"+"nominative: "+ nom+";"));      
      context.write(new Text("Number of Palindromes :"), new Text(Integer.toString(npol)));
      context.write(new Text("---------------------------------"), new Text("--------------------------------------"));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapred.job.tracker", "hdfs://cshadoop1:61120");
    conf.set("yarn.resourcemanager.address", "cshadoop1.utdallas.edu:8032");
    conf.set("mapreduce.framework.name", "yarn");
    Job job = Job.getInstance(conf, "POSCount");
    job.setJarByClass(POSCount.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setNumReduceTasks(1);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}