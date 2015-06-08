package assignment3;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.logging.*;

import assignment2.MyCrawler;
import assignment2.ReturnObject;

public class ReadSerialFile {

	public static void main(String[] args) {
		
		InputStream file;
		try {
			file = new FileInputStream("D:/CrawlerData/indexTest.ser");
			InputStream buffer = new BufferedInputStream(file);
			ObjectInput input = new ObjectInputStream (buffer);
			//deserialize the List
			@SuppressWarnings("unchecked")
			HashMap<String,HashMap<String,Object>> table =
			(HashMap<String, HashMap<String, Object>>) input.readObject();
			//display its data
			System.out.println(table.size());
			Scanner scan = new Scanner(System.in);
			
			String folderLocation = "D:/CrawlerData/WebData/ParseData";
			String TempLine;
			
			while(true){
				System.out.println("Enter the token.");
				String temp = scan.nextLine();
				if (table.containsKey(temp)){
					
					HashMap<String, Object> entry = table.get(temp);
					int k = 0;
					for(Map.Entry<String, Object> value : entry.entrySet()){
						if(k<=5){
							System.out.println(value.getKey());
							System.out.println();
							String hash = Integer.toString(value.getKey().hashCode());
							String fileLocation = folderLocation.concat(hash);
							BufferedReader TextFile = new BufferedReader(new FileReader(fileLocation));
							int j = 0;
							while((TempLine = TextFile.readLine()) != null){
								if (!TempLine.isEmpty() && j<4){
									System.out.println(TempLine);
									j++;
								}
							}
							TextFile.close();
							k++;
							System.out.println();
							System.out.println();
						}
						else{
							break;
						}						
					}
				}
				else{
					System.out.println("Not Present!!!");
				}
			}
			
//			String filename = "D:/CrawlerData/Table1.txt";
//    		File urls = new File(filename);
//    		if (!urls.exists()){
//    			urls.createNewFile();
//    		}
//    		FileWriter fw1 = new FileWriter(urls,true);
//    		BufferedWriter write1 = new BufferedWriter(fw1);
//    		for (Map.Entry<String, HashMap<String, Object>> entry : table.entrySet()){
//    			write1.write(entry.getKey());
//    			HashMap<String, Object> tableNew = entry.getValue();
//				for (Map.Entry<String, Object> entry1 : tableNew.entrySet()){
//					write1.write(" --> ");
//					write1.write(entry1.getKey());
//					write1.write(" " + entry1.getValue().Locations + " " + entry1.getValue().TF + " " +
//							entry1.getValue().IDF + " " + entry1.getValue().Score);
//					write1.newLine();
//				}
//    			write1.newLine();
//    		}    		
//    		
//    		write1.close();
//    		
//    		System.out.println(table.size());
			

		} 
		catch (FileNotFoundException e) {
			e.printStackTrace();
		}		
		catch (IOException e) {
			e.printStackTrace();
		}
		catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
			
		}
		
		
		
		
	}

	




