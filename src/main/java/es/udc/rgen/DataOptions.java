package es.udc.rgen;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;

public class DataOptions {

//	private static final Log log = LogFactory.getLog(DataOptions.class.getName());
	
	public static final double LINK_SIMULATE_SPACE_RATIO = 0.1;
	public static final double WORD_SIMULATE_SPACE_RATIO = 0.1;

	public static final double LINK_ZIPF_EXPONENT = 0.5;
	public static final double WORD_ZIPF_EXPONENT = 0.9;

	private static final String TEMP_DIR = "temp";
	private static final int NUM_LINUX_DICT_WORD = 479623;

	public static enum DataType {
		HIVE, PAGERANK, BAYES, NUTCH, NONE, RANDOMTEXT, TERAGEN, KMEANS, KRONECKER
	}
	private DataType type;

	private String base, dname;
	private Path workPath, resultPath;
	
	private int maps, reds;
	private long pages, slotpages;
	private int words;
	
	private boolean sequenceOut;
	private Class<? extends CompressionCodec> codecClass;
	
	private StringBuffer remainArgs;

	DataOptions(String[] args) throws ClassNotFoundException {
		
		type = DataType.NONE;
		base = "/tmp";
		maps = -1;
		reds = -1;
		pages = -1;
		words = -1;
		sequenceOut = false;
		codecClass = null;
		remainArgs = new StringBuffer("");

		if (args.length < 2) {
			System.exit(printUsage("Error: number of arguments should be no less than 2!!!"));
		}

		if ("-t".equals(args[0])) {
			if ("hive".equalsIgnoreCase(args[1])) {
				type = DataType.HIVE;
				dname = "hive";
			} else if ("randomtext".equalsIgnoreCase(args[1])) {
				type = DataType.RANDOMTEXT;
				dname = "randomtext";
			} else if ("teragen".equalsIgnoreCase(args[1])) {
				type = DataType.TERAGEN;
				dname = "teragen";
			} else if ("kmeans".equalsIgnoreCase(args[1])) {
				type = DataType.KMEANS;
				dname = "kmeans";
			} else if ("pagerank".equalsIgnoreCase(args[1])) {
				type = DataType.PAGERANK;
				dname = "pagerank";
			} else if ("kronecker".equalsIgnoreCase(args[1])) {
				type = DataType.KRONECKER;
				dname = "kronecker";
			} else if ("bayes".equalsIgnoreCase(args[1])) {
				type = DataType.BAYES;
				words = NUM_LINUX_DICT_WORD;
				dname = "bayes";
			} else if ("nutch".equalsIgnoreCase(args[1])) {
				type = DataType.NUTCH;
				words = NUM_LINUX_DICT_WORD;
				dname = "nutch";
			} else {
				System.exit(printUsage("Error: arguments syntax error!!!"));
			}
		}

		for (int i=2; i<args.length; i++) {
			if ("-m".equals(args[i])) {
				maps = Integer.parseInt(args[++i]);
			} else if ("-r".equals(args[i])) {
				reds = Integer.parseInt(args[++i]);
			} else if ("-p".equals(args[i])) {
				pages = parseHumanLong(args[++i]);
			} else if ("-b".equals(args[i])) {
				base = args[++i];
			} else if ("-n".equals(args[i])) {
				dname = args[++i];
			} else if ("-o".equals(args[i])) {
				if ("sequence".equalsIgnoreCase(args[++i])) {
					sequenceOut = true;
				}
			} else if ("-c".equals(args[i])) {
				codecClass =
						Class.forName(args[++i]).asSubclass(CompressionCodec.class);
			} else if (args[i].length()>0) {
				remainArgs.append(args[i]).append(" ");
			}
		}
		
		checkOptions();
		
		slotpages = (long) Math.ceil(pages * 1.0 / maps);

		resultPath = new Path(base, dname);
		workPath = new Path(base, TEMP_DIR);
	}
	
	/**
	   * Parse a number that optionally has a postfix that denotes a base.
	   * @param str an string integer with an option base {k,m,b,t}.
	   * @return the expanded value
	   */
	  private long parseHumanLong(String str) {
	    char tail = str.charAt(str.length() - 1);
	    long base = 1;
	    switch (tail) {
	    case 't':
		  base *= 1000 * 1000 * 1000 * 1000;
		  break;
		case 'b':
		  base *= 1000 * 1000 * 1000;
		  break;
		case 'm':
		  base *= 1000 * 1000;
		  break;
		case 'k':
	      base *= 1000;
	      break;
	    default:
	    }
	    if (base != 1) {
	      str = str.substring(0, str.length() - 1);
	    }
	    return Long.parseLong(str) * base;
	  }
	
	private void checkOptions() {
		
		switch (type) {
		case RANDOMTEXT:
			if (pages<=0) {
				System.exit(printUsage("Error: number of bytes of ramdomtext data should be larger than 0!!!"));
			}
			break;
		case TERAGEN:
			if (pages<=0) {
				System.exit(printUsage("Error: number of rows of teragen data should be larger than 0!!!"));
			}
			break;
		case KMEANS:
			if (remainArgs.length()==0) {
				System.exit(printUsage("Error: number of arguments should be no less than 6 for KMeans!!!"));
			}
			break;
		case HIVE:
			if (pages<=0) {
				System.exit(printUsage("Error: pages of hive data should be larger than 0!!!"));
			}
			break;
		case PAGERANK:
			if (pages<=0) {
				System.exit(printUsage("Error: pages of pagerank data should be larger than 0!!!"));
			}
			break;
		case KRONECKER:
			if (remainArgs.length()==0) {
				System.exit(printUsage("Error: number of arguments should be no less than 1 for Kronecker!!!"));
			}
			break;
		case BAYES:
			if (pages<=0 || words<=0) {
				System.exit(printUsage("Error: pages/words of bayes data should be larger than 0!!!"));
			}
			break;
		case NUTCH:
			if (pages<=0 || words<=0) {
				System.exit(printUsage("Error: pages/words of nutch data should be larger than 0!!!"));
			}
			break;
		default:
			System.exit(printUsage("Error: type of data not defined!!!"));
		}
	}
	
	public Path getWorkPath() {
		return workPath;
	}
	
	public Path getResultPath() {
		return resultPath;
	}
	
	public static final int printUsage(String msg) {
		
		if (null != msg) {
			System.out.println(msg);
			System.out.println();
		}
		
		System.out.println("OPTIONS:");
		
		System.out.println("RANDOM TEXT WRITER:");
		System.out.println("-t randomtext -p <bytes> [-outFormat <class>] "
				+ "[-b <base path>] [-n <data name>] "
				+ "[-m <num maps>]\n");
		
		System.out.println("TERAGEN:");
		System.out.println("-t teragen -p <rows> "
				+ "[-b <base path>] [-n <data name>] "
				+ "[-m <num maps>]\n");
		
		System.out.println("KRONECKER:");
		System.out.println("-t kronecker -k <iterations> "
				+ "[-sm <seed matrix ([value11,value12...;value21,value22...;...])>] [-s <seed>] "
				+ "[-b <base path>] [-n <data name>] "
				+ "[-m <num maps>] [-r <num reduces>] "
				+ "[-o sequence] "
				+ "[-d cdelim]\n");
		
		System.out.println("HIVE:");
		System.out.println("-t hive -p <pages> -v <visits> "
				+ "[-b <base path>] [-n <data name>] "
				+ "[-m <num maps>] [-r <num reduces>] "
				+ "[-o sequence] [-c <codec>] [-d <delimiter>]\n");
		
		System.out.println("PAGERANK:");
		System.out.println("-t pagerank -p <pages> "
				+ "[-b <base path>] [-n <data name>] "
				+ "[-m <num maps>] [-r <num reduces>] "
				+ "[-o sequence] [-c <codec>] "
				+ "[-d cdelim] [-pbalance]\n");
		
		System.out.println("NUTCH:");
		System.out.println("-t nutch -p <pages> [-w <words>] "
				+ "[-b <base path>] [-n <data name>] "
				+ "[-m <num maps>] [-r <num reduces>] "
				+ "[-o sequence] [-c <codec>]\n");
		
		System.out.println("BAYES:");
		System.out.println("-t bayes -p <pages> -g <num classes> [-w <words>] "
				+ "[-b <base path>] [-n <data name>] "
				+ "[-m <num maps>] [-r <num reduces>] "
				+ "[-o sequence] [-c <codec>]\n");
		
        System.out.println("KMEANS:");
        System.out.println("-t kmeans -sampleDir <sampleDirectory> -clusterDir <centroidDirectory> -numClusters <numberofClusters> -numSamples <numberOfSamples> \n"
        		+ "-samplesPerFile <numberOfSamplesPerFile> -sampleDimension <dimensionOfEachSample> [-centroidMin <minValueOfEachDimensionForCenters>] \n"
        		+ "[-centroidMax <maxValueOfEachDimensionForCenters>] [-stdMin <minStandardDeviationOfClusters>] [-stdMax <maxStandardDeviationOfClusters>] \n"
        		+ "[-maxIteration <maxIter> (The samples are generated using Gaussian Distribution around a set of centers which are also generated using UniformDistribution)] \n"
        		+ "[-textOutput (Output text result instead of mahout vector)]");
		
		return -1;
	}
	
	public String[] getRemainArgs() {
		if (remainArgs.length()>0) {
			return remainArgs.toString().trim().split(" ");
		} else {
			return new String[0];
		}
	}
	
	public DataType getType() {
		return type;
	}
	
	public int getNumMaps() {
		return maps;
	}
	
	public int getNumReds() {
		return reds;
	}
	
	public long getNumPages() {
		return pages;
	}
	
	public long getNumSlotPages() {
		return slotpages;
	}
	
	public int getNumWords() {
		return words;
	}
	
	public void setNumWords(int words) {
		this.words = words;
	}
	
	public boolean isSequenceOut() {
		return sequenceOut;
	}
	
	public Class<? extends CompressionCodec> getCodecClass() {
		return codecClass;
	}
}
