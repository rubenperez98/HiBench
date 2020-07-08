package es.udc.rgen.misc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class Cell implements WritableComparable<Cell>{

	private double prob;
	private long row;
	private long col;
	
	Cell(){
		this.prob=0;
		this.row=0;
		this.col=0;
	}
	
	public Cell(double prob, long row, long col) {
		this.prob = prob;
		this.row = row;
		this.col = col;
	}

	public double getProb() {
		return prob;
	}

	public void setProb(double prob) {
		this.prob = prob;
	}

	public long getRow() {
		return row;
	}

	public void setRow(long row) {
		this.row = row;
	}

	public long getCol() {
		return col;
	}

	public void setCol(long col) {
		this.col = col;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(this.row);
		out.writeLong(this.col);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.row=in.readLong();
		this.col=in.readLong();
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Cell other = (Cell) obj;
		if (row == other.col && col == other.row)
			return true;
		if (col != other.col)
			return false;
		if (row != other.row)
			return false;
		return true;
	}

	@Override
	public int compareTo(Cell other) {
		if (row != other.row) {
			return (row < other.row ? -1 : 1);
		} else if (col != other.col) {
			return (col < other.col ? -1 : 1);
		}
		return 0;
	}
	
	@Override
	public int hashCode() {
		final long prime = 31;
		long result = 1;
		result = prime * result + col;
		result = prime * result + row;
		return (int) result;
	}

}
