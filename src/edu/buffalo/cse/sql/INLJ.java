package edu.buffalo.cse.sql;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.index.ISAMIndex;
import edu.buffalo.cse.sql.index.IndexFile;
import edu.buffalo.cse.sql.index.Iter;
import edu.buffalo.cse.sql.optimizer.FinalRewrite;
import edu.buffalo.cse.sql.plan.JoinNode;

public class INLJ implements Iterator {
	
	ArrayList<Schema.Var> varmap1;
	ArrayList<Schema.Var> varmap2;
	int size1;
	int size2;
	Datum[] ret;
	ISAMIndex left;
	ISAMIndex right;
	Iterator ob1;
	Iterator ob2;
	int iidx1=-1;
	int iidx2=-1;
	Datum[] hold;
	boolean check=false;
	Iter iter;
	ArrayList<Schema.Var> varfinal=new ArrayList<Schema.Var>();
	
	public INLJ(JoinNode j,Iterator ob1,Iterator ob2,ArrayList<Schema.Var> varmap1
			,ArrayList<Schema.Var> varmap2,HashMap<String,ISAMIndex> map) throws SqlException, IOException{
		boolean l=false;
		boolean r=false;
		int iidx1=-1;
		int iidx2=-1;
		this.size1=varmap1.size();
		this.size2=varmap2.size();
		ret=new Datum[size1+size2];
		String l1=varmap1.get(0).toString().substring(0, 1).toLowerCase();
		String l2=varmap2.get(0).toString().substring(0, 1).toLowerCase();
		int tmp=0;
		for(int i=0;i<size1;i++){
			if(l1.equalsIgnoreCase(varmap1.get(i).toString().substring(0, 1).toLowerCase())){
				tmp++;
			}
		}
		if(tmp==size1){
			l=true;
		}
		tmp=0;
		for(int i=0;i<size2;i++){
			if(l2.equalsIgnoreCase(varmap2.get(i).toString().substring(0, 1).toLowerCase())){
				tmp++;
			}
		}
		if(tmp==size2){
			r=true;
		}
		
		if(l&&r){
			this.left=map.get(l1);
			this.right=map.get(l2);
			this.ob1=left.scan();
			iidx1=FinalRewrite.map.get(l1);
			iidx2=FinalRewrite.map.get(l2);
			this.iidx1=iidx1;
			this.iidx2=iidx2;
			for(int i=0;i<size1;i++){
				varfinal.add(varmap1.get(i));
			}
			for(int i=0;i<size2;i++){
				varfinal.add(varmap2.get(i));
			}
		}
		else if(l){
			this.right=map.get(l1);
			this.ob1=ob2;
			this.size1=varmap2.size();
			this.size2=varmap1.size();
			iidx2=FinalRewrite.map.get(l1);
			for(int i=0;i<size1;i++){
				try {
					if(j.getExp().get(0).toString().substring(1).equalsIgnoreCase(varmap2.get(i).toString().substring(1))){
						iidx1=i;
						break;
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			this.iidx1=iidx1;
			this.iidx2=iidx2;
			for(int i=0;i<size1;i++){
				varfinal.add(varmap2.get(i));
			}
			for(int i=0;i<size2;i++){
				varfinal.add(varmap1.get(i));
			}
		}
		else{
			this.right=map.get(l2);
			this.ob1=ob1;
			this.size1=varmap1.size();
			this.size2=varmap2.size();
			iidx2=FinalRewrite.map.get(l2);
			for(int i=0;i<size1;i++){
				try {
					if(j.getExp().get(0).toString().substring(1).equalsIgnoreCase(varmap1.get(i).toString().substring(1))){
						iidx1=i;
						break;
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			this.iidx1=iidx1;
			this.iidx2=iidx2;
			for(int i=0;i<size1;i++){
				varfinal.add(varmap1.get(i));
			}
			for(int i=0;i<size2;i++){
				varfinal.add(varmap2.get(i));
			}
		}
		
		hold=new Datum[size1];
		
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
		System.out.println(Datum.stringOfRow(ret));
		return ret;
	}
	
	
	private boolean join(){
		Datum[] lft=null;
		Datum[] rgt=null;
		if(check==false){
			if(ob1.hasNext()){
				lft=(Datum[]) ob1.next();
				Datum[] key={lft[iidx1]};
				try {
					ob2=right.rangeScan(key, key);
					boolean found=false;
					while(found==false){
						if(ob2.hasNext()){
							rgt=(Datum[]) ob2.next();
							for(int i=0;i<size1;i++){
								ret[i]=lft[i];
							}
							for(int i=0;i<size2;i++){
								ret[i+size1]=rgt[i];
							}
							check=true;
							hold=lft;
							return true;
						}
						else{
							if(ob1.hasNext()){
								lft=(Datum[]) ob1.next();
								key=new Datum[] {lft[iidx1]};
								ob2=right.rangeScan(key, key);
							}
							else{
								return false;
							}
						}
					}
				} catch (SqlException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			else{
				return false;
			}
		}
		else{
			if(ob2.hasNext()){
				lft=hold;
				rgt=(Datum[]) ob2.next();
				for(int i=0;i<size1;i++){
					ret[i]=lft[i];
				}
				for(int i=0;i<size2;i++){
					ret[i+size1]=rgt[i];
				}
				check=true;
				hold=lft;
				return true;
			}
			else{
				check=false;
				if(ob1.hasNext()){
					lft=(Datum[]) ob1.next();
					Datum[] key={lft[iidx1]};
					try {
						ob2=right.rangeScan(key, key);
						boolean found=false;
						while(found==false){
							if(ob2.hasNext()){
								rgt=(Datum[]) ob2.next();
								for(int i=0;i<size1;i++){
									ret[i]=lft[i];
								}
								for(int i=0;i<size2;i++){
									ret[i+size1]=rgt[i];
								}
								check=true;
								hold=lft;
								return true;
							}
							else{
								if(ob1.hasNext()){
									lft=(Datum[]) ob1.next();
									key=new Datum[] {lft[iidx1]};
									ob2=right.rangeScan(key, key);
								}
								else{
									return false;
								}
							}
						}
					} catch (SqlException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}
		return false;
	}
	

	@Override
	public void remove() {
		
	}
	public ArrayList<Schema.Var> getSchemavars(){
		
		return varfinal;
	}
}
