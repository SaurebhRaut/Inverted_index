package assignment3;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java.lang.*;

import javax.swing.text.html.HTMLDocument.Iterator;

import assignment1.Token;
import assignment2.ReturnObject;
import assignment2.WordFrequency;


public class Index {

	public static void main(String[] args) {
		Long start = System.currentTimeMillis();
//		float tmp = (float)2/18;
//		System.out.println(tmp);
		// Putting Stop Words in List.
				Hashtable<String,Integer> stopWordsList = new Hashtable<String,Integer>();
				String path = "D:/CrawlerData/StopWords.txt";		
				String TempLine;
				try {
					BufferedReader TextFile = new BufferedReader(new FileReader(path));
					while((TempLine = TextFile.readLine()) != null){
						stopWordsList.put(TempLine, 1);
					}
					
					TextFile.close();
				} 
				catch (IOException e) {
					e.printStackTrace();
				}
		
		HashMap<String,HashMap<String,Object>> table = 
				new HashMap<String,HashMap<String,Object>>();
		
		//Object obj = new Object();
		
		List<String> TempList = new ArrayList<String>();
		List<String> List = new ArrayList<String>();
		
		
		String folderLocation = "D:/CrawlerData/WebData/ParseData";
		//String folderLocation = "D:/CrawlerData/WebData1/";
		String readUrl = "D:/CrawlerData/Test2.txt";
		//String readUrl = "D:/CrawlerData/urlList1.txt";
		String TempLine1;
		String TempLine2;
		int count = 0;
//		int initialWords = 0;		
//		String pageUrl = "";
		
		try {
		BufferedReader TextFile1 = new BufferedReader(new FileReader(readUrl));
		while((TempLine1 = TextFile1.readLine()) != null){
			int finalWords = 0;
			String hash = Integer.toString(TempLine1.hashCode());
			//String fileLocation = folderLocation.concat(TempLine1);
			String fileLocation = folderLocation.concat(hash);
			BufferedReader TextFile = new BufferedReader(new FileReader(fileLocation));
			//BufferedReader TextFile = new BufferedReader(new FileReader("Input"));
			while((TempLine2 = TextFile.readLine()) != null){
				if (!TempLine2.isEmpty()){
					ReturnObject ReturnObj = TokenizeFile(TempLine2, TempList, stopWordsList, finalWords);
					TempList = ReturnObj.List;
				}
			}
			for (String a : TempList){
				if (!table.containsKey(a)){
					HashMap<String,Object> table2 = new HashMap<String,Object>();
					//table2.clear();
					ArrayList<Integer> Locations = new ArrayList<Integer>();
					Locations.add(TempList.indexOf(a));
					float tf = (float)Locations.size()/TempList.size();
					table2.put(TempLine1, new Object(tf,1,1,Locations));
					table.put(a, table2);
					//Locations.clear();
				}
				else{
					HashMap<String,Object> table2 = table.get(a);
					if (table2.containsKey(TempLine1)){
						ArrayList<Integer> Locations = table2.get(TempLine1).Locations;
						List<String> TempList1 = new ArrayList<String>();
						TempList1 = TempList.subList(Locations.get(Locations.size()-1)+1, TempList.size());
						int index = TempList1.indexOf(a);
						index = index + Locations.get(Locations.size()-1) + 1;
						Locations.add(index);
						float tf = (float)Locations.size()/TempList.size();
						table2.put(TempLine1, new Object(tf,1,1,Locations));
						table.put(a, table2);
					}
					else{
						ArrayList<Integer> Locations = new ArrayList<Integer>();
						Locations.add(TempList.indexOf(a));
						float tf = (float)Locations.size()/TempList.size();
						table2.put(TempLine1, new Object(tf,1,1,Locations));
						table.put(a, table2);
					}					
				}
			}
			TextFile.close();
			count += 1;
			System.out.println(count);
			TempList.clear();
			
			//Print(table);
		//}
		//TextFile1.close();
		
		}
		//table = ;
		table = ComputeIDF(table, count);
		//Print(ComputeIDF(table, count));
		//System.out.println("Table Returned!!!");
		
		//=====
		try
        {
//               FileOutputStream indexFilePath =
//                  new FileOutputStream("D:/CrawlerData/indexTest.ser");
			OutputStream file = new FileOutputStream("D:/CrawlerData/temp2.ser");
			OutputStream buffer = new BufferedOutputStream(file);
			ObjectOutput output = new ObjectOutputStream(buffer);
               //ObjectOutputStream oos = new ObjectOutputStream(indexFilePath);
			output.writeObject(table);
			output.close();
			file.close();
               System.out.printf("Serialized HashMap data is saved in hashmap.ser");
        }catch(IOException ioe)
         {
        	System.out.println("I/O Exception!!!");
               //ioe.printStackTrace();
         }
		//======
		System.out.println();
		System.out.println("Number of files: " + count);
		System.out.println("Number of unique tokens: " + table.size());
		Long end = System.currentTimeMillis();
		System.out.println("Time: " + (end-start)/60000 + " minutes");
		}
		catch (FileNotFoundException e) {
			System.out.println("File not found!!!");
		} 
		catch (IOException e) {
			System.out.println("Error!!!");
		}
	}

	private static HashMap<String, HashMap<String, Object>> ComputeIDF(
			HashMap<String, HashMap<String, Object>> table, int count) {
		System.out.println("Size of table: " + table.size());
		int size=0;
		for (Map.Entry<String, HashMap<String, Object>> entry : table.entrySet()){
			HashMap<String, Object> tableNew = entry.getValue();
			//System.out.println(entry.getKey() + " --> " + entry.getValue().size());
			float compute = (float) count/tableNew.size();
			float idf = (float) Math.log(compute);
			for (Map.Entry<String, Object> entry1 : tableNew.entrySet()){
				String temp = entry1.getKey();
				
				float tf = entry1.getValue().TF;
				ArrayList<Integer> Locations = entry1.getValue().Locations;
				float score = (float)tf*idf;
				tableNew.put(temp, new Object(tf,idf,score,Locations));
			}
			   List<String> mapKeys = new ArrayList<String>(tableNew.keySet());
			   List<Float> mapValues = new ArrayList<Float>();
			   for(String key : tableNew.keySet()){
				 mapValues.add(tableNew.get(key).Score);  
			   }
			   //List<float> mapValues = new ArrayList<float>(tableNew.values());
			   Collections.sort(mapValues);
			   Collections.reverse(mapValues);
			   Collections.sort(mapKeys);
			   Collections.reverse(mapKeys);

			   LinkedHashMap<String, Object> sortedMap = new LinkedHashMap<String, Object>();

			   java.util.Iterator<Float> valueIt = mapValues.iterator();
			   while (valueIt.hasNext()) {
			       Float val = valueIt.next();
			       java.util.Iterator<String> keyIt = mapKeys.iterator();

			       while (keyIt.hasNext()) {
			           String key = keyIt.next();
			           String comp1 = Float.toString(tableNew.get(key).Score);
			           Object obj = tableNew.get(key);
			           String comp2 = val.toString();

			           if (comp1.equals(comp2)){
			               tableNew.remove(key);
			               mapKeys.remove(key);
			               sortedMap.put(key,obj);
			               break;
			           }

			       }

			   }
			
			//==========
			   table.put(entry.getKey(), sortedMap);
			   size += 1;
			   System.out.println("Token number: " + size);
		}
		return table;
	}
	
	public static ReturnObject TokenizeFile(String TempLine, List<String> TempList, 
			Hashtable<String, Integer> stopWordsList, int numberOfWords){
		
		TempLine = TempLine.replaceAll("[^a-zA-Z0-9']", " ");
		
		StringTokenizer fileIn = new StringTokenizer(TempLine);	
		
		while (fileIn.hasMoreTokens()){
			String a = fileIn.nextToken().toLowerCase();
			a = a.replaceAll("[']", "");
			if (!stopWordsList.containsKey(a) && a.length()>1){
				TempList.add(a);
			}
			numberOfWords += 1;
		}
		return new ReturnObject(TempList,numberOfWords);
		
	}

	private static void Print(HashMap<String, HashMap<String, Object>> table) {
		for (Map.Entry<String, HashMap<String, Object>> entry : table.entrySet()){	
			System.out.printf("%s\t" ,entry.getKey());
			HashMap<String, Object> tableNew = entry.getValue();
			//for (HashMap<String, Object>> entry1 : tableNew.entrySet()){
			for (Map.Entry<String, Object> entry1 : tableNew.entrySet()){
				System.out.printf("%s\t" ,"--->");
				System.out.printf("%s\t" ,entry1.getKey());
				System.out.println(" " + entry1.getValue().Locations + " " + entry1.getValue().TF + " " +
						entry1.getValue().IDF + " " + entry1.getValue().Score);
			}
			
			//for (Map.Entry<HashMap<String, Object>> entry1 : table.entrySet()){
			
				
			}
		}
		
	}



