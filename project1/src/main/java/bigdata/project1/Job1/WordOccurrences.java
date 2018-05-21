package bigdata.project1.Job1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class WordOccurrences implements WritableComparable<WordOccurrences> {
	
	private Text word;
	private IntWritable occurrences;
	
	public WordOccurrences(String word, int occurrences) {
		this.word = new Text(word);
		this.occurrences = new IntWritable(occurrences);
	}

	public String getWordString() {
		return word.toString();
	}
	
	public int getOccurrencesInt() {
		return occurrences.get();
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		occurrences = new IntWritable(Integer.parseInt(in.readUTF()));
        word = new Text(in.readUTF());
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		 out.writeUTF(occurrences.toString());
	     out.writeUTF(word.toString());
		
	}

	@Override
	public int compareTo(WordOccurrences o) {
		return this.word.compareTo(o.word);
	}

}
