package edu.buffalo.cse.sql;

import java.util.ArrayList;
import java.util.Iterator;

import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.Datum.CastError;
import edu.buffalo.cse.sql.plan.PlanNode;

public class UnionIter implements Iterator {
	ArrayList<Schema.Var> map=new ArrayList<Schema.Var>();
	ArrayList<Datum[]> dat=new ArrayList<Datum[]>();
	public ArrayList<Datum[]> union(PlanNode node,ArrayList<Datum[]> data1,
			ArrayList<Datum[]> data2,ArrayList<Schema.Var> var) throws CastError{
		int size1=data1.size();
		int size2=data2.size();
		
		//System.out.println(data1.get(0)[0].toInt()+"      data 1         "+data1.get(0)[1].toInt());
		//System.out.println(data2.get(0)[0].toInt()+"      data 1");
		
		int arr[]=new int[size1];
		
		for(int i=0;i<size1;i++){
			for(int j=0;j<size2;j++){
					if(data1.get(i).equals(data2.get(j))){
						arr[i]=0;
						break;
					}
					else{
						arr[i]=1;
					}
				
			}
		}
		
		for(int i=0;i<size1;i++){
			if(arr[i]==1){
				data2.add(data1.get(i));
			}
		}
		dat=data2;
		map=var;
		return dat;
	}
	
	public ArrayList<Schema.Var> getMap(){
		return map;
	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Object next() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void remove() {
		// TODO Auto-generated method stub
		
	}
}

