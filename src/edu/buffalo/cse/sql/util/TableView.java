package edu.buffalo.cse.sql.util;

import java.util.ArrayList;

import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.Datum.CastError;

public class TableView {
	
	public void show(ArrayList<Datum[]> data, ArrayList<Schema.Var> var) throws CastError{
		
		ArrayList<Integer> field=new ArrayList<Integer>();
		for(int i=0;i<var.size();i++){
			int max=0;
			if(data.get(0)[i].getType()==Schema.Type.STRING){
				for(int d=0;d<data.size();d++){
					if(data.get(d)[i].toString().length()>max){
						if(var.get(i).name.length()>max){
							max=var.get(i).name.length();
						}
						else{
							max=data.get(d)[i].toString().length();
						}
					}
				}
				field.add(max+4);
				continue;
			}
			if(data.get(data.size()-1)[i].toString().length()>=var.get(i).name.toString().length()){
				field.add(data.get(data.size()-1)[i].toString().length()+4);
			}
			else{
				field.add(var.get(i).name.toString().length());
			}
	
		}
		
		for(int i=0;i<var.size();i++){
			int n;
			for(n=0;n<(field.get(i)-var.get(i).name.length())/2;n++){
				System.out.print(" ");
			}
			System.out.print(var.get(i).name);
			for(int j=0;j<field.get(i)-var.get(i).name.length()-n;j++){
				System.out.print(" ");
			}
			System.out.print("|");
		}
		System.out.println();
		for(int i=0;i<=var.size();i++){
			System.out.print("--------------------");
		}
		System.out.println();
		for(int i=0;i<data.size();i++){
			for(int j=0;j<var.size();j++){
				int n;
				for(n=0;n<(field.get(j)-data.get(i)[j].toString().length())/2;n++){
					System.out.print(" ");
				}
				System.out.print(data.get(i)[j].toString());
				for(int k=0;k<(field.get(j)-data.get(i)[j].toString().length())-n;k++){
					System.out.print(" ");
				}
				System.out.print("|");
			}
			System.out.println();
		}
	}
	
	
public void show(ArrayList<Datum[]> data, ArrayList<Schema.Var> var,int num) throws CastError{
		
		ArrayList<Integer> field=new ArrayList<Integer>();
		for(int i=0;i<var.size();i++){
			int max=0;
			if(data.get(0)[i].getType()==Schema.Type.STRING){
				for(int d=0;d<num;d++){
					if(data.get(d)[i].toString().length()>max){
						if(var.get(i).name.length()>max){
							max=var.get(i).name.length();
						}
						else{
							max=data.get(d)[i].toString().length();
						}
					}
				}
				field.add(max+4);
				continue;
			}
			if(data.get(data.size()-1)[i].toString().length()>=var.get(i).name.toString().length()){
				field.add(data.get(data.size()-1)[i].toString().length()+4);
			}
			else{
				field.add(var.get(i).name.toString().length());
			}
	
		}
		
		for(int i=0;i<var.size();i++){
			int n;
			for(n=0;n<(field.get(i)-var.get(i).name.length())/2;n++){
				System.out.print(" ");
			}
			System.out.print(var.get(i).name);
			for(int j=0;j<field.get(i)-var.get(i).name.length()-n;j++){
				System.out.print(" ");
			}
			System.out.print("|");
		}
		System.out.println();
		for(int i=0;i<=var.size();i++){
			System.out.print("--------------------");
		}
		System.out.println();
		for(int i=0;i<num;i++){
			for(int j=0;j<var.size();j++){
				int n;
				for(n=0;n<(field.get(j)-data.get(i)[j].toString().length())/2;n++){
					System.out.print(" ");
				}
				System.out.print(data.get(i)[j].toString());
				for(int k=0;k<(field.get(j)-data.get(i)[j].toString().length())-n;k++){
					System.out.print(" ");
				}
				System.out.print("|");
			}
			System.out.println();
		}
	}


}
