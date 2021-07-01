package edu.rmit.cosc2367.s3779009.Assignment2_Task1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class WordPair implements Writable, WritableComparable<WordPair> {
	
	private Text word;
	private Text neighbour;
	
	public WordPair(Text word, Text neighbor) {
        this.word = word;
        this.neighbour = neighbor;
    }

    public WordPair(String word, String neighbour) {
        this(new Text(word),new Text(neighbour));
    }
    
    public WordPair() {
        this.word = new Text();
        this.neighbour = new Text();
    }
    
	public void setWord(String word) {
		this.word.set(word);
	}

	public void setNeighbour(String tokens) {
		this.neighbour.set(tokens);
	}
	
	public Text getWord() {
        return word;
    }

    public Text getNeighbour() {
        return neighbour;
    }

	public int compareTo(WordPair o) {
		// TODO Auto-generated method stub
		int returnVal = this.word.compareTo(o.getWord());
        if(returnVal != 0){
            return returnVal;
        }
        if(this.neighbour.toString().equals("*")){
            return -1;
        }else if(o.getNeighbour().toString().equals("*")){
            return 1;
        }
        return this.neighbour.compareTo(o.getNeighbour());
	}

	
	public static WordPair read(DataInput in) throws IOException 
	{ 
		WordPair wordPair = new WordPair();
		wordPair.readFields(in); 
		return wordPair; 
	}
	
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		word.readFields(in);
        neighbour.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		word.write(out);
        neighbour.write(out);
	}
	
	public String toString() {
        return "{word=["+word+"]"+
               " neighbor=["+neighbour+"]}";
    }
	
	public boolean equals(Object o) 
	{ 
		if (this == o) return true; 
		if (o == null|| getClass() != o.getClass()) return false;
		WordPair wordPair = (WordPair) o;
		if (neighbour != null ? !neighbour.equals(wordPair.neighbour) : wordPair.neighbour != null) return false; 
		if (word != null ?!word.equals(wordPair.word) : wordPair.word != null) return false;
		return true; 
	  }
	 

	// @Override
	public int hashCode() {
	        int result = word != null ? word.hashCode() : 0;
	        result = 163 * result + (neighbour != null ? neighbour.hashCode() : 0);
	        return result;
	  }

}
