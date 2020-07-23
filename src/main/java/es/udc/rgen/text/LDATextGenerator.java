package es.udc.rgen.text;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.distribution.GammaDistribution;
import org.apache.commons.math3.distribution.PoissonDistribution;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import es.udc.rgen.*;
import es.udc.rgen.misc.Utils;

public class LDATextGenerator extends Configured {

	private static final Log log = LogFactory.getLog(LDATextGenerator.class.getName());
	
	public static final String LINES = "mapreduce.ldatext.lines";
	public static final String WORDS_PER_LINE = "mapreduce.ldatext.wordsline";
	public static final String BYTES_PER_MAP = "mapreduce.ldatext.bytesmap";
	public static final String CONTROL_BYTES = "mapreduce.ldatext.controlbytes";
	public static final String NUM_TOPICS = "mapreduce.ldatext.numtopics";
	public static final String NUM_TERMS = "mapreduce.ldatext.numterms";
	public static final String ALPHA = "mapreduce.ldatext.alpha";
	public static final String DELIMETER = "mapreduce.output.textoutputformat.separator";
	public static final String NUM_MAPS = "mapreduce.ldatext.nummaps";
	
	public static final String BETA = "mapreduce.ldatext.beta";
	public static final String VOCA = "mapreduce.ldatext.voca";
	
	private static final String ALPHAFILE = "final.other";
	private static final String BETAFILE = "final.beta";
	private static final String VOCAFILE = ".voca";
	
	private DataOptions options;
	private Configuration conf;
	private Class<? extends OutputFormat> outputFormatClass = TextOutputFormat.class;

	private long lines = Long.MAX_VALUE;
	private int	words_per_line = 100;
	private Path input_path = null, alphafile = null, betafile = null, vocafile = null;
	
	private int num_topics = 0, num_terms = 0;
	private Double alpha = 0.0;

	private static final Random random_seed = new Random(System.currentTimeMillis());

	public LDATextGenerator (Configuration conf, DataOptions options) throws IOException {
		this.conf=conf;
		this.options = options;
		parseArgs(options.getRemainArgs());
	}

	private void parseArgs(String[] args) throws IOException {
		
		for (int i=0; i<args.length; i++) {

			if ("-l".equals(args[i])) {
				lines = Long.parseLong(args[++i]);
			} else if ("-wl".equals(args[i])) {
				words_per_line = Integer.parseInt(args[++i]);
			} else if ("-i".equals(args[i])) {
				String input_path_string = args[++i];
				input_path = new Path(input_path_string);
				alphafile = new Path(input_path_string,ALPHAFILE);
				betafile = new Path(input_path_string,BETAFILE);
				String[] vocafile_name = input_path_string.trim().split("/");
				String voca_path=vocafile_name[vocafile_name.length-1].concat(VOCAFILE);
				vocafile = new Path(input_path_string,voca_path);
				if (!Utils.existsPath(input_path) ||
					!Utils.existsPath(alphafile) ||
					!Utils.existsPath(betafile) ||
					!Utils.existsPath(vocafile)) {
					DataOptions.printUsage("Error with input data path --> " + input_path_string + " <--");
				}
			} else {
				DataOptions.printUsage("Unknown LDA-text data arguments --> " + args[i] + " <--");
			}
		}
	}
	
	public void init() throws IOException {
		
		log.info("Initializing LDA-text data generator...");
		
		Utils.checkHdfsPath(options.getResultPath(), true);
		//Utils.checkHdfsPath(options.getWorkPath(), true);

		//dummy = new Dummy(options.getWorkPath(), options.getNumMaps());
	}
	
	@SuppressWarnings("deprecation")
	private void setOptions(Configuration job) throws URISyntaxException, IOException {
		job.set(DELIMETER, " ");
		job.setLong(LINES, lines);
		job.setInt(WORDS_PER_LINE, words_per_line);
		job.setInt(NUM_MAPS, options.getNumMaps());
		job.setLong(BYTES_PER_MAP, options.getNumSlotPages());
		if (options.getNumPages() <= 0) {
			job.setBoolean(CONTROL_BYTES, false);
		} else {
			job.setBoolean(CONTROL_BYTES, true);
		}
		
		FileSystem fs = FileSystem.get(job);
		FSDataInputStream inalpha = fs.open(alphafile);
		
		String[] num_topics_s = inalpha.readLine().trim().split(" ");
		num_topics=Integer.parseInt(num_topics_s[num_topics_s.length-1]);
		job.setInt(NUM_TOPICS, num_topics);
		
		String[] num_terms_s = inalpha.readLine().trim().split(" ");
		num_terms=Integer.parseInt(num_terms_s[num_terms_s.length-1]);
		job.setInt(NUM_TERMS, num_terms);
		
		String[] alpha_s = inalpha.readLine().trim().split(" ");
		alpha=Double.parseDouble(alpha_s[alpha_s.length-1]);
		job.setDouble(ALPHA, alpha);
		
		inalpha.close();
		
		job.setStrings(BETA,betafile.toString());
		
		job.setStrings(VOCA,vocafile.toString());
		
		fs.close();
	}
	
	static class LDAInputFormat extends InputFormat<Text, Text> {

	    /** 
	     * Generate the requested number of file splits, with the filename
	     * set to the filename of the output file.
	     */
	    public List<InputSplit> getSplits(JobContext job) throws IOException {
	      List<InputSplit> result = new ArrayList<InputSplit>();
	      Path outDir = FileOutputFormat.getOutputPath(job);
	      int numSplits = job.getConfiguration().getInt(NUM_MAPS, 1);
	      for(int i=0; i < numSplits; ++i) {
	        result.add(new FileSplit(new Path(outDir, "dummy-split-" + i), 0, 1, 
	                                  (String[])null));
	      }
	      return result;
	    }

	    /**
	     * Return a single record (filename, "") where the filename is taken from
	     * the file split.
	     */
	    static class RandomRecordReader extends RecordReader<Text, Text> {
	      Path name;
	      Text key = null;
	      Text value = new Text();
	      public RandomRecordReader(Path p) {
	        name = p;
	      }
	      
	      public void initialize(InputSplit split,
	                             TaskAttemptContext context)
	      throws IOException, InterruptedException {
	    	  
	      }
	      
	      public boolean nextKeyValue() {
	        if (name != null) {
	          key = new Text();
	          key.set(name.getName());
	          name = null;
	          return true;
	        }
	        return false;
	      }
	      
	      public Text getCurrentKey() {
	        return key;
	      }
	      
	      public Text getCurrentValue() {
	        return value;
	      }
	      
	      public void close() {}

	      public float getProgress() {
	        return 0.0f;
	      }
	    }

	    public RecordReader<Text, Text> createRecordReader(InputSplit split,
	        TaskAttemptContext context) throws IOException, InterruptedException {
	      return new RandomRecordReader(((FileSplit) split).getPath());
	    }
	  }
	
	static class DummyToTextMapper extends Mapper<Text, Text, Text, Text> {

		private int words_line, topics_num, terms_num;
		private long lines, bytes_per_map;
		private double alpha;
		private boolean control_bytes;
		private String beta_path_string, voca_path_string;
		private double[][] beta;
		private String[] voca;
		FileSystem fs;
		
		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			lines = conf.getLong(LINES, Long.MAX_VALUE);
			words_line = conf.getInt(WORDS_PER_LINE, 20);
			topics_num = conf.getInt(NUM_TOPICS, 0);
			terms_num = conf.getInt(NUM_TERMS, 0);
			alpha = conf.getDouble(ALPHA, 1);
			bytes_per_map = conf.getLong(BYTES_PER_MAP, 1024*1024*1024);
			control_bytes = conf.getBoolean(CONTROL_BYTES, false);
			
			beta_path_string = conf.get(BETA);
			voca_path_string = conf.get(VOCA);
			
			fs = FileSystem.get(conf);
			
			beta = new double[topics_num][terms_num];
			FSDataInputStream inbeta = fs.open(new Path(beta_path_string));
			for (int j=0; j<topics_num; j++) {
				String[] beta_topic = inbeta.readLine().trim().split(" ");
				for (int i=0; i<terms_num; i++) {
					beta[j][i] = Math.exp(Double.parseDouble(beta_topic[i]));
				}
			}
			inbeta.close();

			voca = new String[terms_num];
			FSDataInputStream invoca = fs.open(new Path(voca_path_string));
			for (int i=0; i<terms_num; i++) {
				voca[i] = invoca.readLine().trim();
			}
			invoca.close();
			
			//log.info("------------------> BETA FILE IN " + beta_path_string);
			//log.info("------------------> VOCA FILE IN " + voca_path_string);
		}
		
		public void map(Text key, Text value, Context context) throws IOException,InterruptedException {
			
			int lenght, topic, word;
			PoissonDistribution poisson;
			Multinomial multinomial1;
			Multinomial[] multinomiali = new Multinomial[topics_num];
			
			double[] theta = new double[topics_num];
			GammaDistribution gamma = new GammaDistribution(alpha,1);
			for(int i=0; i<topics_num; i++){
		        theta[i]=gamma.sample();
		    }
			
			boolean cont = true;
			long bytes_written = 0;
			StringBuffer line, key_s;
			Text key_t, line_t;
			
			poisson = new PoissonDistribution(words_line);
			multinomial1 = new Multinomial(random_seed,theta);
			for (int i=0; i<topics_num; i++) {
				multinomiali[i] = new Multinomial(random_seed,beta[i]);
			}
			
			for (long size_i=0; cont && size_i<lines ; size_i++) {
				lenght = poisson.sample();
				
				key_s = new StringBuffer("");
				line = new StringBuffer("");
				
				for (int i=0; i<lenght; i++) {
					topic = multinomial1.sample();
					
					word = multinomiali[topic].sample();
					
					if (i<lenght/2) {
						key_s.append(voca[word]).append(" ");
					} else {
						line.append(voca[word]).append(" ");
					}
				}
				
				key_t = new Text(key_s.toString());
				line_t = new Text(line.toString());
				
				bytes_written += key_t.getLength() + line_t.getLength();
				context.write(key_t, line_t);
				
				if (bytes_written >= bytes_per_map && control_bytes) {
					cont = false;
				}
			}
			
			fs.close();
		}
	}

	private void createLDAText() throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

		log.info("Creating LDA-text data...", null);
		
		Utils.checkHdfsPath(options.getResultPath());
		Path fout = options.getResultPath();
		
		setOptions(conf);
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(LDATextGenerator.class);
	    job.setJobName("Create LDA-text data");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(LDAInputFormat.class);
		job.setMapperClass(DummyToTextMapper.class);
		
		job.setNumReduceTasks(0);
		
		job.setOutputFormatClass(outputFormatClass);
		FileOutputFormat.setOutputPath(job, fout);
		
		log.info("Running Job: Create LDA-text data");
		
//		log.info("LINES: "+conf.getLong(LINES, 0));
//		log.info("WORDS_LINE: "+conf.getInt(WORDS_PER_LINE, 0));
//		log.info("TOPICS_NUM: "+conf.getInt(NUM_TOPICS, 0));
//		log.info("ALPHA: "+conf.getDouble(ALPHA, 1));
//		log.info("BYTES_MAP: "+conf.getLong(BYTES_PER_MAP, 0));
		
		log.info("Data output " + fout + "");
		Date startTime = new Date();
	    log.info("Job started: " + startTime);
	    int ret = job.waitForCompletion(true) ? 0 : 1;
	    Date endTime = new Date();
	    log.info("Job ended: " + endTime);
	    log.info("The job took " + 
	                       (endTime.getTime() - startTime.getTime()) /1000 + 
	                       " seconds.");
		log.info("Finished Running Job: Create LDA-text data");
	}

	public void generate() throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
		
		log.info("Generating LDA-text data files...");
		init();
		createLDAText();
		closeGenerator();
	}

	private void closeGenerator() throws IOException {

		log.info("Closing LDA-text generator...");
		Utils.checkHdfsPath(options.getWorkPath(), true);
	}
}
