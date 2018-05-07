package bigdata.project1.job2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class Avarage implements WritableComparable<Avarage> {
	
	private DoubleWritable value;
	private IntWritable count;
	
	public Avarage() {}
	
	public Avarage(double value) {
		this.value = new DoubleWritable(value);
		this.count = new IntWritable(1);
	}
	
	public Avarage(double value, int count) {
		this.value = new DoubleWritable(value);
		this.count = new IntWritable(count);
	}
	
	public void readFields(DataInput in) throws IOException {
        value = new DoubleWritable(Double.parseDouble(in.readUTF()));
        count = new IntWritable(Integer.parseInt(in.readUTF()));
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(value.toString());
        out.writeUTF(count.toString());
    }


    public DoubleWritable getValue() {
		return value;
	}
    
    public void setValue(double val) {
    	this.value = new DoubleWritable(val);
    }
    
    public DoubleWritable getValueAndIncrement(Avarage avg) {
    	avg.incrementCount();
		return value;
	}

	public IntWritable getCount() {
		return count;
	}
	
	public void incrementCount() {
		this.count = new IntWritable(this.count.get() + 1); 
	}

	@Override
    public String toString() {
        return value.toString() + " -- " + count.toString();
    }

    @Override
    public int hashCode() {
        return value.hashCode() + count.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof Avarage) {
            Avarage avg = (Avarage) o;
            return value.equals(avg.value)
                    && count.equals(avg.count);
        }
        return false;
    }

    public int compareTo(Avarage tp) {
        int cmp = count.compareTo(tp.count);
        if (cmp != 0) {
            return cmp;
        }
        return value.compareTo(tp.value);
    }

}
