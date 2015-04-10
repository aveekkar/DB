package edu.buffalo.cse.sql;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;

import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.plan.ExprTree;
import edu.buffalo.cse.sql.plan.JoinNode;
import edu.buffalo.cse.sql.plan.PlanNode;

public class SMJ implements Iterator {
	
	boolean next=false;
	Sort sort1;
	Sort sort2;
	ExprTree exp;
	ArrayList<Schema.Var> varmap1=new ArrayList<Schema.Var>();
	ArrayList<Schema.Var> varmap2=new ArrayList<Schema.Var>();
	Datum[] toret;
	int idx1;
	int idx2;
	boolean end=false;
	int num=0;
	int size1;
	int size2;
	
	public SMJ(JoinNode node,Iterator ob1,Iterator ob2,ArrayList<Schema.Var> varmap1,
			ArrayList<Schema.Var> varmap2) throws Exception{
		ExprTree exp=node.getExp();
		this.varmap1=varmap1;
		this.varmap2=varmap2;
		//System.out.println(exp);
		//System.out.println("var1: "+varmap1);
		//System.out.println("var2: "+varmap2);
		this.size1=varmap1.size();
		this.size2=varmap2.size();
		this.exp=exp;
		String var1;
		String var2;
		int found=0;
		for(int i=0;i<varmap1.size();i++){
			if(exp.get(0).toString().substring(0, 1).equalsIgnoreCase(varmap1.get(i).toString().substring(0, 1))){
				found=1;
				break;
			}
		}
		if(found==1){
			var1=exp.get(0).toString();
			var2=exp.get(1).toString();
		}
		else{
			var1=exp.get(1).toString();
			var2=exp.get(0).toString();
		}
		int idx1=-1;
		int idx2=-1;
		//System.out.println(exp);
		//System.out.println(varmap1);
		for(int i=0;i<varmap1.size();i++){
			if(varmap1.get(i).toString().equalsIgnoreCase(var1)){
				idx1=i;
				break;
			}
		}
		//System.out.println(varmap2);
		for(int i=0;i<varmap2.size();i++){
			if(varmap2.get(i).toString().equalsIgnoreCase(var2)){
				idx2=i;
				break;
			}
		}
		this.idx1=idx1;
		this.idx2=idx2;
		//System.out.println("idx1: "+idx1);
		//System.out.println("idx2: "+idx2);
		
		File file1 = new File(exp.toString()+var1);
		if (!file1.exists()) {
			file1.createNewFile();
		}
		File file2 = new File(exp.toString()+var2);
		if (!file2.exists()) {
			file2.createNewFile();
		}
		
		Sort sort1=new Sort(ob1,new int[] {idx1},file1);
		sort1.sort();
		this.sort1=sort1;
		Sort sort2=new Sort(ob2,new int[] {idx2},file2);
		sort2.sort();
		System.out.println("completed sort left...");
		this.sort2=sort2;
		/*while(sort1.hasNext()||sort2.hasNext()){
			
			if(sort2.hasNext()){
				System.out.print(Datum.stringOfRow(sort1.next()));
				System.out.print("            ");
				System.out.print(Datum.stringOfRow(sort2.next()));
				System.out.println("  ");
			}
			else{
				System.out.println(Datum.stringOfRow(sort1.next()));
			}
		}
		
		sort1.reset();
		sort2.reset();*/
		System.out.println("completed sort right...");
	}

	@Override
	public boolean hasNext() {
		if(join()){
			return true;
		}
		else{
			return false;
		}
	}

	@Override
	public Object next() {
		//System.out.println(Datum.stringOfRow(toret));
		return toret;
	}

	@Override
	public void remove() {}
	
	private boolean join(){
		Datum[] left;
		Datum[] right;
		
		Datum[] hold=new Datum[size1+size2];
		int comp;
		if(num==0){
			if(sort1.hasNext()){
				left=sort1.next();
			}
			else{
				return false;
			}
			if(sort2.hasNext()){
				right=sort2.next();
			}
			else{
				return false;
			}
			comp=left[idx1].compareTo(right[idx2]);
			if(comp!=0){
				while(sort1.hasNext()&&sort2.hasNext()){
					if(comp==-1){
						left=sort1.next();
						comp=left[idx1].compareTo(right[idx2]);
					}
					else{
						right=sort2.next();
						comp=left[idx1].compareTo(right[idx2]);
					}
					if(comp==0){
						break;
					}
				}
				if(left[idx1].compareTo(right[idx2])!=0){
					return false;
				}
				
				for(int i=0;i<size1;i++){
					hold[i]=left[i];
				}
				for(int i=0;i<size2;i++){
					hold[i+size1]=right[i];
				}
				this.toret=hold;
				++num;
				return true;
			}
			else{
				for(int i=0;i<size1;i++){
					hold[i]=left[i];
				}
				for(int i=0;i<size2;i++){
					hold[i+size1]=right[i];
				}
				this.toret=hold;
				++num;
				return true;
			}
		}
		else{
			if(sort2.hasNext()){
				sort1.moveBack(1);
				left=sort1.next();
				right=sort2.next();
				comp=left[idx1].compareTo(right[idx2]);
				if(comp!=0){
					if(sort1.hasNext()){
						Datum[] tmp=sort1.next();
						if(tmp[idx1].compareTo(left[idx1])==0){
							left=tmp;
							sort2.moveBack(num+1);
							right=sort2.next();
							num=0;
							for(int i=0;i<size1;i++){
								hold[i]=left[i];
							}
							for(int i=0;i<size2;i++){
								hold[i+size1]=right[i];
							}
							this.toret=hold;
							++num;
							return true;
						}
						else{
							num=0;
							left=tmp;
							if(num==0){
								comp=left[idx1].compareTo(right[idx2]);
								if(comp!=0){
									while(sort1.hasNext()&&sort2.hasNext()){
										if(comp==-1){
											left=sort1.next();
											comp=left[idx1].compareTo(right[idx2]);
										}
										else{
											right=sort2.next();
											comp=left[idx1].compareTo(right[idx2]);
										}
										if(comp==0){
											break;
										}
									}
									if(left[idx1].compareTo(right[idx2])!=0){
										return false;
									}
									
									for(int i=0;i<size1;i++){
										hold[i]=left[i];
									}
									for(int i=0;i<size2;i++){
										hold[i+size1]=right[i];
									}
									this.toret=hold;
									++num;
									return true;
								}
								else{
									for(int i=0;i<size1;i++){
										hold[i]=left[i];
									}
									for(int i=0;i<size2;i++){
										hold[i+size1]=right[i];
									}
									this.toret=hold;
									++num;
									return true;
								}
							}
							
						}
					}
					else{
						return false;
					}
				}
				else{
					for(int i=0;i<size1;i++){
						hold[i]=left[i];
					}
					for(int i=0;i<size2;i++){
						hold[i+size1]=right[i];
					}
					this.toret=hold;
					++num;
					return true;
				}
			}
			else{
				sort1.moveBack(1);
				Datum[] kk=sort1.next();
				if(sort1.hasNext()){
					left=sort1.next();
					if(left[idx1].compareTo(kk[idx1])==0){
						sort2.moveBack(num);
						right=sort2.next();
						num=0;
						for(int i=0;i<size1;i++){
							hold[i]=left[i];
						}
						for(int i=0;i<size2;i++){
							hold[i+size1]=right[i];
						}
						this.toret=hold;
						++num;
						return true;
					}
					else{
						return false;
					}
				}
				else{
					return false;
				}
			}
		}
		return false;
	}
	
	public ArrayList<Schema.Var> getSchemaVars(){
		ArrayList<Schema.Var> varmap=new ArrayList<Schema.Var>();
		for(Schema.Var var:varmap1){
			varmap.add(var);
		}
		for(Schema.Var var:varmap2){
			varmap.add(var);
		}
		return varmap;
	}

	

}
