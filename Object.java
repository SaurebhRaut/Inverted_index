package assignment3;

import java.io.Serializable;
import java.util.ArrayList;

public class Object implements Serializable{
	float TF;
	float IDF;
	float Score;
	ArrayList<Integer> Locations = new ArrayList<Integer>();
	
	public Object(float TF, float IDF,	float Score,	ArrayList<Integer> Locations){
		this.TF = TF;
		this.IDF = IDF;
		this.Score = Score;
		this.Locations = Locations;
	}
	
	public Object TempFunction(float TF, float IDF,	float Score,	ArrayList<Integer> Locations){
		return new Object(TF, IDF,	Score, Locations);
	}
	
}
