package es.udc.rgen;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import es.udc.rgen.graph.KroneckerGraph;
import es.udc.rgen.graph.PagerankData;
import es.udc.rgen.table.HiveData;
import es.udc.rgen.table.KMeans;
import es.udc.rgen.table.NutchData;
import es.udc.rgen.text.BayesData;
import es.udc.rgen.text.RandomTextWriter;
import es.udc.rgen.text.tera.TeraGen;

public class RGen extends Configured implements Tool {

	public static final boolean DEBUG_MODE = true;
	
	@Override
	public int run(String[] args) throws Exception {

		DataOptions options = new DataOptions(args);
		switch (options.getType()) {
			case RANDOMTEXT: {
				RandomTextWriter data = new RandomTextWriter(getConf(),options);
				data.generate();
				break;
			}
			case TERAGEN: {
				TeraGen data = new TeraGen(getConf(),options);
				data.generate();
				break;
			}
			case KMEANS: {
				KMeans data = new KMeans(getConf(),options);
				data.generate();
				break;
			}
			case HIVE: {
				HiveData data = new HiveData (options);
				data.generate();
				break;
			}
			case PAGERANK: {
				PagerankData data = new PagerankData(options);
				data.generate();
				break;
			}
			case KRONECKER: {
				KroneckerGraph data = new KroneckerGraph(options);
				data.generate();
				break;
			}
			case BAYES: {
				BayesData data = new BayesData(options);
				data.generate();
				break;
			}
			case NUTCH: {
				NutchData data = new NutchData(options);
				data.generate();
				break;
			}
			default:
				break;
		}
		return 0;
	}

	public static void main(String[] args) throws Exception
	{
		int result = ToolRunner.run(new Configuration(), new RGen(), args);
		System.exit(result);
	}
}
