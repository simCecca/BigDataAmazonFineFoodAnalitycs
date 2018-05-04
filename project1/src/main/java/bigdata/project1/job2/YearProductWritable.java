package bigdata.project1.job2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class YearProductWritable implements WritableComparable<YearProductWritable> {
	
	private IntWritable year;
	private Text product;
	
	public YearProductWritable() {}
	
	public YearProductWritable(int year, String product) {
		this.year = new IntWritable(year);
		this.product = new Text(product);
	}
	
	public void readFields(DataInput in) throws IOException {
        year = new IntWritable(Integer.parseInt(in.readUTF()));
        product = new Text(in.readUTF());
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(year.toString());
        out.writeUTF(product.toString());
    }


    public IntWritable getYear() {
		return year;
	}

	public Text getProduct() {
		return product;
	}

	@Override
    public String toString() {
        return year.toString() + " -- " + product.toString();
    }

    @Override
    public int hashCode() {
        return year.hashCode() + product.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof YearProductWritable) {
            YearProductWritable yp = (YearProductWritable) o;
            return year.equals(yp.year)
                    && product.equals(yp.product);
        }
        return false;
    }

    public int compareTo(YearProductWritable tp) {
        int cmp = year.compareTo(tp.year);
        if (cmp != 0) {
            return cmp;
        }
        return product.compareTo(tp.product);
    }

}
