package Trec2016.dd_trec;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import jgibblda.Estimator;
import jgibblda.Inferencer;
import jgibblda.LDACmdOption;
/**
 * apply LDA Topic model to achieve the classification of the articles
 * @author Wenzao Wang
 * 2016-08-15
 */
public class TopicModelBuildAndApply {
	private static double alpha = 0.5;
	private static double beta = 0.1;
	private static int niters = 1000;
	private static String OUTPUT = "LDAResult.txt";
	private static BufferedReader br;
	
	/**
	 * Build the Topic model called "model-final" by the data
	 * @param directory  
	 * @param fileName   
	 * @param classNumber 
	 */
	public static void newModule(String directory, String fileName,int classNumber) {	
		LDACmdOption ldaOption = new LDACmdOption();   
	    ldaOption.est = true;  
	    ldaOption.estc = false;  
	    ldaOption.modelName = "model-final";  
	    ldaOption.dir = directory; 
	    ldaOption.dfile = fileName;  
	    ldaOption.alpha = alpha;  
	    ldaOption.beta = beta;  
	    ldaOption.K =  classNumber;  
	    ldaOption.niters = niters;   
	    Estimator estimator = new Estimator();  
	    estimator.init(ldaOption);  
	    estimator.estimate(); 
	    Inferencer inferencer = new Inferencer(); 
        inferencer.init(ldaOption);	
	}
	/**
	 * set the number of key you want
	 * @param number
	 */
	public static void getKeyword(int number,String directory) {
		File f = new File(directory+"/model-final.twords");
		String result = new String();
		try{
			BufferedReader br = new BufferedReader(new FileReader(f));
			String line;
		    boolean isResult = false;
		    int count = -1;
			while((line = br.readLine())!= null){
				//System.out.println("read line ok");
				String[] word = line.split(" ");
				if(word[0].equals("Topic")) {//A new topic
					count = 0;
					isResult = true;
					//System.out.println("find topic");
					continue;
				}
				if(isResult && count < number) {
					String[] keyword = line.split(" ");
					count++;
					if(count < number)
					    result += keyword[0].trim()+" ";
					else {
						result += keyword[0].trim() +"\n";
						isResult = false;
					}
					//System.out.println(keyword[0].trim());						
				}			
			}		
		}catch(IOException ex){
			ex.printStackTrace();
		}
		write(result);
	}
	//write the result into a special file which path is OUTPUT
	private static void write(String str) {
		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter(
					OUTPUT,false));
			writer.write(str);
			writer.close();
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}
	/**
	 * the complete process of LDA classification
	 * @param directory  Catalogue of dataset,like"models"
	 * @param fileName   the file's name ,like "text.txt"
	 * @param classNumber  the number of classes,if you wanna divide 20 docs into 5 groups,
	 * the classNumber will be 5
	 * @param keyNumber    the number of keyword for each class , 
	 * if you want the first 5 keyword to represent the Topic 0th ,the keyNumber will be 5
	 * @return the relative path of the file which contains the result of LDA
	 * ps:the format of the data file is very import
	 * the first line must be the number of docs.For example ,if you have 20 docs,
	 * the data file's first line is 20.
	 * Then,the rest lines are docs,each line is the context of a docs in 20 docs.
	 * The "models/text.txt" is a correct example.
	 */
	public static String classify(String directory, String fileName,int classNumber,int keyNumber){
		filter(directory, fileName);
		newModule(directory, fileName,classNumber);
		getKeyword(keyNumber,directory);
		return OUTPUT;
	}
	
	public static void main(String[] args) {
		classify("models","text.txt",5,5);
	}
	private static void filter(String directory,String filename) {
		File f = new File(directory+"/"+filename);
		Filter filter = new Filter();
		ArrayList<String> wordList = new ArrayList<String>();
		try {
			br = new BufferedReader(new FileReader(f));
			String line;
			int linecount = 0;
			while((line = br.readLine()) != null) {
				//wordList.clear();
				if(linecount != 0){				
				String[] word = line.split(" ");
				for(int i = 0;i<word.length;i++) {
					if(filter.filter(word[i]))	
						wordList.add(word[i]+" ");
					if(i==word.length -1)
						wordList.add("\n");
				} 
			 }else {
				 wordList.add(line);
				 wordList.add("\n");
				 linecount+=1;
				 continue;
			 }
			}
			String str = new String();
			for(int j = 0;j<wordList.size();j++){
				str+=wordList.get(j);		
			}
			BufferedWriter writer = new BufferedWriter(new FileWriter(
					directory+"/"+filename));
			writer.write(str);
			writer.close();
	
		}catch (IOException ex) {
			ex.printStackTrace();
		}
	}

}
