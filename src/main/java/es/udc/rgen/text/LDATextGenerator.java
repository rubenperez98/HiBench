package es.udc.rgen.text;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.distribution.GammaDistribution;
import org.apache.commons.math3.distribution.PoissonDistribution;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.NLineInputFormat;

import es.udc.rgen.*;
import es.udc.rgen.misc.Dummy;
import es.udc.rgen.misc.Utils;

public class LDATextGenerator {

	private static final Log log = LogFactory.getLog(LDATextGenerator.class.getName());
	
	public static final String LINES = "mapreduce.ldatext.lines";
	public static final String WORDS_PER_LINE = "mapreduce.ldatext.wordsline";
	public static final String BYTES_PER_MAP = "mapreduce.ldatext.bytesmap";
	public static final String CONTROL_BYTES = "mapreduce.ldatext.controlbytes";
	public static final String NUM_TOPICS = "mapreduce.ldatext.numtopics";
	public static final String NUM_TERMS = "mapreduce.ldatext.numterms";
	public static final String ALPHA = "mapreduce.ldatext.alpha";
	
	private static final String ALPHAFILE = "final.other";
	private static final String BETAFILE = "final.beta";
	private static final String VOCAFILE = ".voca";
	
	private DataOptions options;

	private int lines = Integer.MAX_VALUE, words_per_line = 100;
	private Path input_path = null, alphafile = null, betafile = null, vocafile = null;
	
	private int num_topics = 0, num_terms = 0;
	private Double alpha = 0.0;
	
    private static double[][] beta = null;
    private static String[] voca = null;
	
	private static Random random_seed = new Random(System.currentTimeMillis());

	private Dummy dummy;

	public LDATextGenerator (DataOptions options) throws IOException {
		this.options = options;
		parseArgs(options.getRemainArgs());
	}

	private void parseArgs(String[] args) throws IOException {
		
		for (int i=0; i<args.length; i++) {

			if ("-l".equals(args[i])) {
				lines = Integer.parseInt(args[++i]);
			} else if ("-wl".equals(args[i])) {
				words_per_line = Integer.parseInt(args[++i]);
			} else if ("-s".equals(args[i])) {
				int seed = Integer.parseInt(args[++i]);
				random_seed = new Random(seed);
			} else if ("-i".equals(args[i])) {
				String input_path_string = args[++i];
				input_path = new Path(input_path_string);
				alphafile = new Path(input_path_string,ALPHAFILE);
				betafile = new Path(input_path_string,BETAFILE);
				String[] vocafile_name = input_path_string.trim().split("/");
				vocafile = new Path(input_path_string,vocafile_name[vocafile_name.length-1].concat(VOCAFILE));
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
		Utils.checkHdfsPath(options.getWorkPath(), true);

		dummy = new Dummy(options.getWorkPath(), options.getNumMaps());
	}
	
	@SuppressWarnings("deprecation")
	private void setOptions(JobConf job) throws URISyntaxException, IOException {
		job.setInt(LINES, lines);
		job.setInt(WORDS_PER_LINE, words_per_line);
		if (options.getNumPages() <= 0) {
			job.setInt(BYTES_PER_MAP, (int) options.getNumSlotPages());
			job.setBoolean(CONTROL_BYTES, false);
		} else {
			job.setInt(BYTES_PER_MAP, (int) options.getNumSlotPages());
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
		
		
		beta = new double[num_topics][num_terms];
		FSDataInputStream inbeta = fs.open(betafile);
		for (int j=0; j<num_topics; j++) {
			String[] beta_topic = inbeta.readLine().trim().split(" ");
			for (int i=0; i<num_terms; i++) {
				beta[j][i] = Math.exp(Double.parseDouble(beta_topic[i]));
			}
		}
		inbeta.close();
		
		
		voca = new String[num_terms];
		FSDataInputStream invoca = fs.open(vocafile);
		for (int i=0; i<num_terms; i++) {
			voca[i] = invoca.readLine().trim();
		}
		invoca.close();
		
		fs.close();
	}
	
	public static class DummyToTextMapper extends MapReduceBase implements
	Mapper<LongWritable, Text, Text, Text> {

		private int lines, words_line, topics_num, bytes_per_map;
		private double alpha;
		private boolean control_bytes;
		
		private void getOptions(JobConf job) {
			lines = job.getInt(LINES, 10);
			words_line = job.getInt(WORDS_PER_LINE, 10);
			topics_num = job.getInt(NUM_TOPICS, 0);
			alpha = job.getDouble(ALPHA, 1);
			bytes_per_map = job.getInt(BYTES_PER_MAP, 1024*1024*1024);
			control_bytes = job.getBoolean(CONTROL_BYTES, false);
		}
		
		@Override
		public void configure(JobConf job) {
			getOptions(job);
		}
		
		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output,
				Reporter reporter) throws IOException{
			
			int lenght, topic, word;
			PoissonDistribution poisson;
			Multinomial multinomial1, multinomiali;
			
			double[] theta = new double[topics_num];
			GammaDistribution gamma = new GammaDistribution(alpha,1);
			for(int i=0; i<topics_num; i++){
		        theta[i]=gamma.sample();
		    }
			
			boolean cont = true;
			int bytes_written = 0;
			
			for (int size_i=0; size_i<lines && cont; size_i++) {
				
				poisson = new PoissonDistribution(words_line);
				lenght = poisson.sample();
				
				StringBuffer line = new StringBuffer("");
				
				for (int i=0; i<lenght; i++) {
					multinomial1 = new Multinomial(random_seed,theta);
					topic = multinomial1.sample();
					
					multinomiali = new Multinomial(random_seed,beta[topic]);
					word = multinomiali.sample();
					
					line.append(voca[word]).append(" ");
				}
				
				Text line_t = new Text(line.toString());
				
				bytes_written += line_t.getLength();
				output.collect(new Text(""), line_t);
				
				if (bytes_written >= bytes_per_map && control_bytes) {
					cont = false;
				}
			}
		}
	}

	private void createLDAText() throws IOException, URISyntaxException {

		log.info("Creating LDA-text data...", null);

		JobConf job = new JobConf(LDATextGenerator.class);
		String jobname = "Create LDA-text data";
		
		Utils.checkHdfsPath(options.getResultPath());
		Path fout = options.getResultPath();

		job.setJobName(jobname);
		setOptions(job);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, dummy.getPath());
		job.setInputFormat(NLineInputFormat.class);
		
		job.setMapperClass(DummyToTextMapper.class);
		job.setNumReduceTasks(0);
		
		FileOutputFormat.setOutputPath(job, fout);
		job.setOutputFormat(TextOutputFormat.class);
		
		log.info("Running Job: " +jobname);
		log.info("Dummy file " + dummy.getPath() + " as input");
		log.info("Data output " + fout + "");
		JobClient.runJob(job);
		log.info("Finished Running Job: " + jobname);
	}

	public void generate() throws IOException, URISyntaxException {
		
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
