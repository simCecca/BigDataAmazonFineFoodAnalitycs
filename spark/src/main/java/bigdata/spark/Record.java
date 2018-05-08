package bigdata.spark;

import java.io.Serializable;
import java.util.List;

public class Record implements Serializable {
	
	private String product;
	
	private String user;
	
	private int score;
	
	private int year;
	
	private List<String> summaryWords;

	public String getProduct() {
		return product;
	}

	public void setProduct(String product) {
		this.product = product;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public int getScore() {
		return score;
	}

	public void setScore(int score) {
		this.score = score;
	}

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public List<String> getSummaryWords() {
		return summaryWords;
	}

	public void setSummaryWords(List<String> summaryWords) {
		this.summaryWords = summaryWords;
	}
	
	

}
