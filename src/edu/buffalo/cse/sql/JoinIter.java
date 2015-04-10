package edu.buffalo.cse.sql;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Stack;

import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.Datum.CastError;
import edu.buffalo.cse.sql.plan.PlanNode;

public class JoinIter implements Iterator {
	
	Stack<ArrayList<Schema.Var>> map=new Stack<ArrayList<Schema.Var>>();
	ArrayList<Datum[]> materialize=new ArrayList<Datum[]>();
	Iterator ob1;
	Stack<ArrayList<Schema.Var>> varmap=new Stack<ArrayList<Schema.Var>>();
	int matcount=0;
	int sizemat;
	Datum[] hold;
	int size1;
	int size2;
	//int ctr=0;
	
	public JoinIter(PlanNode node, Iterator ob1,Iterator ob2,Stack<ArrayList<Schema.Var>> varmap){
		this.ob1=ob1;
		this.varmap=varmap;
		ArrayList<Schema.Var> var1=new ArrayList<Schema.Var>();
		ArrayList<Schema.Var> var2=new ArrayList<Schema.Var>();
		ArrayList<Schema.Var> tempmap=new ArrayList<Schema.Var>();
		var1=(ArrayList<Schema.Var>) varmap.pop();
		//view.show(dat1, var1);
		var2=(ArrayList<Schema.Var>) varmap.pop();
		//view.show(dat2, var2);
		int size1=var1.size();
		for(int i=0;i<size1;i++){
			tempmap.add(var1.get(i));
		}

		int size2=var2.size();
		for(int i=0;i<size2;i++){
			tempmap.add(var2.get(i));
		}

		varmap.push(tempmap);
		map=varmap;
		while(ob2.hasNext()){
			materialize.add((Datum[]) ob2.next());
		}
		this.sizemat=materialize.size();
		this.size1=var1.size();
		this.size2=var2.size();
	}
	
	public JoinIter(){
		
	}

	
	public ArrayList<Datum[]> join(PlanNode node,ArrayList<Datum[]> dat1,ArrayList<Datum[]> dat2,
			Stack<ArrayList<Schema.Var>> varmap) throws CastError{

		//TableView view=new TableView();
		
		
		ArrayList<Datum[]> temptab=new ArrayList<Datum[]>();
		ArrayList<Schema.Var> var1=new ArrayList<Schema.Var>();
		ArrayList<Schema.Var> var2=new ArrayList<Schema.Var>();
		ArrayList<Schema.Var> tempmap=new ArrayList<Schema.Var>();


		var1=(ArrayList<Schema.Var>) varmap.pop();
		//view.show(dat1, var1);
		var2=(ArrayList<Schema.Var>) varmap.pop();
		//view.show(dat2, var2);
		int size1=var1.size();
		for(int i=0;i<size1;i++){
			tempmap.add(var1.get(i));
		}

		int size2=var2.size();
		for(int i=0;i<size2;i++){
			tempmap.add(var2.get(i));
		}

		varmap.push(tempmap);
		map=varmap;
		int sizetab1=dat1.size();
		int sizetab2=dat2.size();
		ArrayList<Datum[]> tab1;
		ArrayList<Datum[]> tab2;
		
		if(dat1.size()>=dat2.size()){
			sizetab1=dat1.size();
			sizetab2=dat2.size();
			tab1=dat1;
			tab2=dat2;
			size1=var1.size();
			size2=var2.size();
		}
		else{
			sizetab1=dat2.size();
			sizetab2=dat1.size();
			tab1=dat2;
			tab2=dat1;
			size1=var2.size();
			size2=var1.size();
		}
		
		var1.clear();
		var2.clear();
		//int i=0;
		//Datum arr[]=new Datum[size1+size2];

		for(int j=0;j<sizetab1;j++){
			if(j>sizetab2){
				tab2.clear();
			}
			for(int k=0;k<sizetab2;k++){
				Datum[] arr=new Datum[size1+size2];
				for(int l=0;l<size1+size2;l++){
					if(l<size1){
						arr[l]=tab1.get(0)[l];

					}
					else{
						if(j>sizetab2){
							arr[l]=temptab.get(k)[l];
						}
						else{
							arr[l]=tab2.get(k)[l-size1];
						}
						

					}

				}
				temptab.add(arr);
				//System.out.println("iterated "+i+" times");
				//i++;
			}
			tab1.remove(0);

		}

		//System.out.println("table after join");
		//view.show(temptab, map.peek());

		return temptab;
	}
	public Stack<ArrayList<Schema.Var>> getMap(){
		return map;
	}
	@Override
	public boolean hasNext() {
		if(matcount==0){
			if(ob1.hasNext()){
				return true;
			}
			else{
				return false;
			}
		}
		else{
			if(matcount<sizemat&&matcount>0){
				return true;
			}
			else{
				return false;
			}
		}
	}
	@Override
	public Datum[] next() {
		Datum[] tempdat=new Datum[size1+size2];
		if((matcount==0)){
			hold=new Datum[size1];
			hold=(Datum[]) ob1.next();
		}
		
		for(int i=0;i<size1+size2;i++){
			if(i<size1){
				tempdat[i]=hold[i];
			}
			else{
				tempdat[i]=materialize.get(matcount)[i-size1];
			}
		}
		//System.out.println("join num: "+(ctr+1)+" matcount: "+matcount);ctr++;
		matcount++;
		if(matcount==sizemat){
			matcount=0;
		}
		//System.out.println(tempdat[0]+" "+tempdat[1]+" "+tempdat[2]+" "+tempdat[3]+" ");
		//System.out.println("@ join: "+Datum.stringOfRow(tempdat));
		//System.out.println("returning from join: "+Datum.stringOfRow(tempdat));
		return tempdat;
	}
	@Override
	public void remove() {
		// TODO Auto-generated method stub
		
	}
}
