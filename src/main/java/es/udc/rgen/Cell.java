package es.udc.rgen;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class Cell implements WritableComparable<Cell>{

	private double prob;
	private int row;
	private int col;
	
	Cell(){
		this.prob=0;
		this.row=0;
		this.col=0;
	}
	
	Cell(double prob, int row, int col) {
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

	public int getRow() {
		return row;
	}

	public void setRow(int row) {
		this.row = row;
	}

	public int getCol() {
		return col;
	}

	public void setCol(int col) {
		this.col = col;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.row);
		out.writeInt(this.col);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.row=in.readInt();
		this.col=in.readInt();
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
		final int prime = 31;
		int result = 1;
		result = prime * result + col;
		result = prime * result + row;
		return result;
	}

}
