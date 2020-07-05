package es.udc.rgen.text;

import java.util.Random;
	
public class Multinomial {
	static Random generator;
	double[] distribution;
	int range;

	//Constructor
	Multinomial(Random gen, double[] probabilities){
		generator = gen;
		range = probabilities.length;
		distribution = new double[range];
		double sumProb = 0;
		for (double value : probabilities){
			sumProb += value;
		}
		double position = 0;
		for (int i = 0; i < range; ++i){
			position += probabilities[i] / sumProb;
			distribution[i] = position;
		}
		distribution[range -1] = 1.0;
	}

	int sample() {
		double uniform = generator.nextDouble();
		for (int i = 0; i < range; ++i){
			if (uniform < distribution[i]){
				return i;
			}
		}
		return range - 1;
	}

}