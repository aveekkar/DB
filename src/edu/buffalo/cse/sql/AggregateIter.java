package edu.buffalo.cse.sql;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Stack;

import javax.swing.text.AttributeSet;
import javax.swing.text.html.HTML.Tag;

import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.Datum.CastError;
import edu.buffalo.cse.sql.plan.AggregateNode;
import edu.buffalo.cse.sql.plan.ExprTree.VarLeaf;
import edu.buffalo.cse.sql.plan.ExprTree;
import edu.buffalo.cse.sql.plan.PlanNode;
import edu.buffalo.cse.sql.plan.ProjectionNode;

public class AggregateIter implements Iterator {
	
	ArrayList<Schema.Var> map=new ArrayList<Schema.Var>();
	ArrayList<Datum[]> dat=new ArrayList<Datum[]>();
	int size;
	
	
	
	
	public ArrayList<Datum[]> aggregate(PlanNode node, Iterator ob,ArrayList<Schema.Var> var)
			throws CastError{
		ArrayList<Datum[]> data=new ArrayList<Datum[]>();
		while(ob.hasNext()){
			Datum rec[]=(Datum[]) ob.next(); 
			//System.out.println("recd at agg: "+Datum.stringOfRow(rec));
			data.add(rec);
		}
		if(data.size()==0){
			return null;
		}
		//System.out.println("data size at agg: "+data.size());
		//System.out.println("table recd at aggregate");
		//TableView view=new TableView();
		//view.show(data, var,100);
		map.addAll(((AggregateNode)node).getSchemaVars());
		ArrayList<ProjectionNode.Column> gbcol=new ArrayList<ProjectionNode.Column>();
		gbcol=(ArrayList)(((AggregateNode)node).getGroupByVars());
		ArrayList<AggregateNode.AggColumn> aggcol=new ArrayList<AggregateNode.AggColumn>();
		aggcol=(ArrayList)(((AggregateNode)node).getAggregates());
		//view.show(data, var);
		//System.out.println("group by"+gbcol.get(0).expr);
		//System.out.println("aggregates"+aggcol);
		//System.out.println("Schema received: "+var.get(0).name+"  "+var.get(0).rangeVariable);
		
		int arr[]=new int[gbcol.size()];
		int z=0;
		for(int i=0;i<gbcol.size();i++){
			for(int j=0;j<var.size();j++){
				if(gbcol.get(i).expr.toString().split("\\.")[0].length()==gbcol.get(i).expr.toString().length()){
					if(gbcol.get(i).expr.toString().equalsIgnoreCase(var.get(j).name)){
						arr[z]=j;
						z++;
					}
				}
				else{
					if(gbcol.get(i).expr.toString().equalsIgnoreCase(var.get(j).rangeVariable+"."+var.get(j).name)){
						arr[z]=j;
						z++;
					}
				}
			}
		}
		
		
		
		Comp comp=new Comp(arr,gbcol.size());
		if(!gbcol.isEmpty()){
			Collections.sort(data, comp);
		}
		
		//System.out.println("after sorting");
		//view.show(data, var,10);
		
		Datum[] temp=new Datum[gbcol.size()+aggcol.size()];
		boolean notend=true;
		int size=data.size();
		int ctr=0;
		int count=1;
		int sum=0;
		int max=-10000000;
		int min=10000000;
		Datum[] hold=new Datum[aggcol.size()];
		for(int i=0;i<aggcol.size();i++){
			if(aggcol.get(i).aggType==AggregateNode.AType.SUM){
				hold[i]=new Datum.Int(sum);
			}
			else if(aggcol.get(i).aggType==AggregateNode.AType.AVG){
				hold[i]=new Datum.Int(sum);
			}
			else if(aggcol.get(i).aggType==AggregateNode.AType.MIN){
				hold[i]=new Datum.Int(min);
			}
			else if(aggcol.get(i).aggType==AggregateNode.AType.MAX){
				hold[i]=new Datum.Int(max);
			}
		}
		ArrayList<Datum[]> retfinal=new ArrayList<Datum[]>();
		while(notend){
			if(gbcol.size()!=0){
				for(int i=0;i<gbcol.size();i++){
					temp[i]=data.get(0)[arr[i]];
				}
				
				for(int i=0;i<aggcol.size();i++){
					temp[gbcol.size()+i]=evalAggs(data.get(0), var, aggcol.get(i));
					if(aggcol.get(i).aggType==AggregateNode.AType.SUM){
						if(temp[gbcol.size()+i].getType()==Schema.Type.INT){
							hold[i]=new Datum.Int(temp[gbcol.size()+i].toInt()+hold[i].toInt());
						}
						else{
							hold[i]=new Datum.Flt(temp[gbcol.size()+i].toFloat()+hold[i].toFloat());
						}
						
					}
					else if(aggcol.get(i).aggType==AggregateNode.AType.MIN){
						if(temp[gbcol.size()+i].toInt()<=hold[i].toInt()){
							if(temp[gbcol.size()+i].getType()==Schema.Type.INT){
								hold[i]=new Datum.Int(temp[gbcol.size()+i].toInt());
							}
							else{
								hold[i]=new Datum.Flt(temp[gbcol.size()+i].toFloat());
							}
							
						}
					}
					else if(aggcol.get(i).aggType==AggregateNode.AType.MAX){
						if(temp[gbcol.size()+i].toInt()>=hold[i].toInt()){
							if(temp[gbcol.size()+i].getType()==Schema.Type.INT){
								hold[i]=new Datum.Int(temp[gbcol.size()+i].toInt());
							}
							else{
								hold[i]=new Datum.Flt(temp[gbcol.size()+i].toFloat());
							}
						}
					}
					else if(aggcol.get(i).aggType==AggregateNode.AType.AVG){
						if(temp[gbcol.size()+i].getType()==Schema.Type.INT){
							hold[i]=new Datum.Int(temp[gbcol.size()+i].toInt()+hold[i].toInt());
						}
						else{
							hold[i]=new Datum.Flt(temp[gbcol.size()+i].toFloat()+hold[i].toFloat());
						}
					}
				}
				for(int i=0;i<aggcol.size();i++){
					if(aggcol.get(i).aggType==AggregateNode.AType.SUM){
						temp[gbcol.size()+i]=hold[i];
					}
					else if(aggcol.get(i).aggType==AggregateNode.AType.MIN){
						temp[gbcol.size()+i]=hold[i];
					}
					else if(aggcol.get(i).aggType==AggregateNode.AType.MAX){
						temp[gbcol.size()+i]=hold[i];
					}
					else if(aggcol.get(i).aggType==AggregateNode.AType.AVG){
						temp[gbcol.size()+i]=new Datum.Flt((float)hold[i].toFloat()/count);
					}
					else{
						temp[gbcol.size()+i]=new Datum.Int(count);
					}
				}
				
				
				if(ctr==size-1){
					retfinal.add(temp);
					//System.out.println("sum: "+sum+" count: "+count+" min: "+min+" max: "+max);
					break;
				}
				
				count++;
				
				if(comp.compare(data.get(0), data.get(1))!=0){
					retfinal.add(temp);
					temp=new Datum[gbcol.size()+aggcol.size()];
					//System.out.println("sum: "+sum+" count: "+count+" min: "+min+" max: "+max);
					for(int i=0;i<aggcol.size();i++){
						if(aggcol.get(i).aggType==AggregateNode.AType.SUM){
							hold[i]=new Datum.Int(sum);
						}
						else if(aggcol.get(i).aggType==AggregateNode.AType.AVG){
							hold[i]=new Datum.Int(sum);
						}
						else if(aggcol.get(i).aggType==AggregateNode.AType.MIN){
							hold[i]=new Datum.Int(min);
						}
						else if(aggcol.get(i).aggType==AggregateNode.AType.MAX){
							hold[i]=new Datum.Int(max);
						}
					}
					count=1;
				}
				
				data.remove(0);
				ctr++;
				
			}
			else{
				
				for(int i=0;i<aggcol.size();i++){
					temp[gbcol.size()+i]=evalAggs(data.get(0), var, aggcol.get(i));
					if(aggcol.get(i).aggType==AggregateNode.AType.SUM){
						if(temp[gbcol.size()+i].getType()==Schema.Type.INT){
							hold[i]=new Datum.Int(temp[gbcol.size()+i].toInt()+hold[i].toInt());
						}
						else{
							hold[i]=new Datum.Flt(temp[gbcol.size()+i].toFloat()+hold[i].toFloat());
						}
						
					}
					else if(aggcol.get(i).aggType==AggregateNode.AType.MIN){
						if(temp[gbcol.size()+i].toInt()<=hold[i].toInt()){
							if(temp[gbcol.size()+i].getType()==Schema.Type.INT){
								hold[i]=new Datum.Int(temp[gbcol.size()+i].toInt());
							}
							else{
								hold[i]=new Datum.Flt(temp[gbcol.size()+i].toFloat());
							}
							
						}
					}
					else if(aggcol.get(i).aggType==AggregateNode.AType.MAX){
						if(temp[gbcol.size()+i].toInt()>=hold[i].toInt()){
							if(temp[gbcol.size()+i].getType()==Schema.Type.INT){
								hold[i]=new Datum.Int(temp[gbcol.size()+i].toInt());
							}
							else{
								hold[i]=new Datum.Flt(temp[gbcol.size()+i].toFloat());
							}
						}
					}
					else if(aggcol.get(i).aggType==AggregateNode.AType.AVG){
						if(temp[gbcol.size()+i].getType()==Schema.Type.INT){
							hold[i]=new Datum.Int(temp[gbcol.size()+i].toInt()+hold[i].toInt());
						}
						else{
							hold[i]=new Datum.Flt(temp[gbcol.size()+i].toFloat()+hold[i].toFloat());
						}
					}
				}
				for(int i=0;i<aggcol.size();i++){
					if(aggcol.get(i).aggType==AggregateNode.AType.SUM){
						temp[gbcol.size()+i]=hold[i];
					}
					else if(aggcol.get(i).aggType==AggregateNode.AType.MIN){
						temp[gbcol.size()+i]=hold[i];
					}
					else if(aggcol.get(i).aggType==AggregateNode.AType.MAX){
						temp[gbcol.size()+i]=hold[i];
					}
					else if(aggcol.get(i).aggType==AggregateNode.AType.AVG){
						temp[gbcol.size()+i]=new Datum.Flt((float)hold[i].toFloat()/count);
					}
					else{
						temp[gbcol.size()+i]=new Datum.Int(count);
					}
				}
				
				
				if(ctr==size-1){
					retfinal.add(temp);
					//System.out.println("sum: "+sum+" count: "+count+" min: "+min+" max: "+max);
					break;
				}
				
				count++;
				data.remove(0);
				ctr++;
				
				
			}
		}
		
		return retfinal;
	}
	
	

	public ArrayList<Schema.Var> getMap(){
		return map;
	}
	
	
	public void  exprEval(ExprTree exp,Stack<ExprTree> expr){

		expr.push(exp);
		if(exp.op==ExprTree.OpCode.CONST||exp.op==ExprTree.OpCode.VAR){
			return ;
		}
		else{
			exprEval(exp.get(0),expr);
			if(!(exp.size()==1)){
				exprEval(exp.get(1),expr);}


		}
	}
	
	 public class Comp implements Comparator<Datum[]>{
		 	int[] arr;
		 	int size;
		 	public Comp(int arr[],int size){
		 		this.size=size;
		 		this.arr=new int[size];
		 		this.arr=arr;
		 	}

			@Override
			public int compare(Datum[] lhs, Datum[] rhs) {
				int comp=0;
				for(int i=0;i<size;i++){
					comp=comp(lhs[arr[i]],rhs[arr[i]]);
					if(comp!=0){
						break;
					}
				}
				return comp;
	
			}
			
			 public int comp(Datum left,Datum right)
			  {
			    if(left.equals(right)) return 0;
			    try {
			      switch(left.getType()){
			        case INT: 
			          switch(left.getType()){
			            case INT: return left.toInt() > right.toInt() ? 1 : -1;
			            case FLOAT: return left.toFloat() > right.toFloat() ? 1 : -1;
			            case STRING: return -1;
			            case BOOL: return -1;
			          }
			        case FLOAT:
			          switch(left.getType()){
			            case INT: 
			            case FLOAT: return left.toFloat() > right.toFloat() ? 1 : -1;
			            case STRING: return -1;
			            case BOOL: return -1;
			          }
			        case STRING: 
			          switch(left.getType()){
			            case INT: 
			            case FLOAT: 
			            case BOOL: return 1;
			            case STRING: return left.toString().compareTo(right.toString());
			          }
			        case BOOL:
			          switch(left.getType()){
			            case INT: 
			            case FLOAT: return 1;
			            case BOOL: return left.toBool() ? -1 : 1;
			            case STRING: return -1;
			          }        
			      } 
			    } catch (CastError e){}
			    return 0;
			  }
			 
		 }
	 
	 public Datum evalAggs(Datum[] dat,ArrayList<Schema.Var> var,AggregateNode.AggColumn agg) throws CastError{
		 Stack<ExprTree> exprstk=new Stack<ExprTree>();
		 ExprTree root=agg.expr;
		 exprEval(root, exprstk);
		 Stack<Datum> tmp=new Stack<Datum>();
		 while(!exprstk.isEmpty()){
			 
			 int k=0;
				int j=0;
				float a=0;
				float b=0;
				int x=0;
				boolean m,n;
				if(exprstk.peek().op==ExprTree.OpCode.CONST){
					tmp.push(((ExprTree.ConstLeaf)exprstk.pop()).v);
				}
				else if(exprstk.peek().op==ExprTree.OpCode.VAR){
					ExprTree.VarLeaf temp;
					temp=(VarLeaf) exprstk.pop();
					//System.out.println("got var: "+temp.name.toString());
					for(int s=0;s<var.size();s++){	
						//System.out.println("comparing with schema var: "+var.get(s).name);
						if(var.get(s).name.equalsIgnoreCase(temp.name.toString())||(var.get(s).rangeVariable+"."+var.get(s).name).equalsIgnoreCase(temp.name.toString())){
							//System.out.println("pushing value for: "+temp.name.toString());
							tmp.push(dat[s]);
							break;
						}
					}
				}
				else{
					if(exprstk.peek().op==ExprTree.OpCode.ADD){
						exprstk.pop();
						if(((Datum)tmp.peek()).getType()==Schema.Type.INT){
							k=((Datum)tmp.pop()).toInt();}
						else{
							a=((Datum)tmp.pop()).toFloat();
						}
						if(((Datum)tmp.peek()).getType()==Schema.Type.INT){
							j=((Datum)tmp.pop()).toInt();}
						else{
							b=((Datum)tmp.pop()).toFloat();
						}
						if(a!=0&&b!=0){
							tmp.push(new Datum.Flt(a+b));
						}
						else if(k!=0&&j!=0){
							tmp.push(new Datum.Int(k+j));
						}
						else if(k!=0&&b!=0){
							tmp.push(new Datum.Flt((float)k+b));
						}
						else{
							tmp.push(new Datum.Flt(a+(float)j));
						}

						//System.out.println(tmp.size()+exprstk.size());

					}
					else if(exprstk.peek().op==ExprTree.OpCode.DIV){
						exprstk.pop();
						if(((Datum)tmp.peek()).getType()==Schema.Type.INT){
							k=((Datum)tmp.pop()).toInt();}
						else{
							a=((Datum)tmp.pop()).toFloat();
						}
						if(((Datum)tmp.peek()).getType()==Schema.Type.INT){
							j=((Datum)tmp.pop()).toInt();}
						else{
							b=((Datum)tmp.pop()).toFloat();
						}
						if(a!=0&&b!=0){
							tmp.push(new Datum.Flt(a/b));
						}
						else if(k!=0&&j!=0){
							tmp.push(new Datum.Int(k/j));
						}
						else if(k!=0&&b!=0){
							tmp.push(new Datum.Flt((float)k/b));
						}
						else{
							tmp.push(new Datum.Flt(a/(float)j));
						}

						
					}
					else if(exprstk.peek().op==ExprTree.OpCode.MULT){
						exprstk.pop();
						if(((Datum)tmp.peek()).getType()==Schema.Type.INT){
							k=((Datum)tmp.pop()).toInt();}
						else{
							a=((Datum)tmp.pop()).toFloat();
						}
						if(((Datum)tmp.peek()).getType()==Schema.Type.INT){
							j=((Datum)tmp.pop()).toInt();}
						else{
							b=((Datum)tmp.pop()).toFloat();
						}
						if(a!=0&&b!=0){
							tmp.push(new Datum.Flt(a*b));
						}
						else if(k!=0&&j!=0){
							tmp.push(new Datum.Int(k*j));
						}
						else if(k!=0&&b!=0){
							tmp.push(new Datum.Flt((float)k*b));
						}
						else{
							tmp.push(new Datum.Flt(a*(float)j));
						}

				
					}
					else if(exprstk.peek().op==ExprTree.OpCode.SUB){
						exprstk.pop();
						if(((Datum)tmp.peek()).getType()==Schema.Type.INT){
							k=((Datum)tmp.pop()).toInt();}
						else{
							a=((Datum)tmp.pop()).toFloat();
						}
						if(((Datum)tmp.peek()).getType()==Schema.Type.INT){
							j=((Datum)tmp.pop()).toInt();}
						else{
							b=((Datum)tmp.pop()).toFloat();
						}
						if(a!=0&&b!=0){
							tmp.push(new Datum.Flt(a-b));
						}
						else if(k!=0&&j!=0){
							tmp.push(new Datum.Int(k-j));
						}
						else if(k!=0&&b!=0){
							tmp.push(new Datum.Flt((float)k-b));
						}
						else{
							tmp.push(new Datum.Flt(a-(float)j));
						}

				
					}
					else if(exprstk.peek().op==ExprTree.OpCode.EQ){
						exprstk.pop();
						Datum[] t1={((Datum)tmp.pop())};
						Datum[] t2={((Datum)tmp.pop())};
						tmp.push(new Datum.Bool(Datum.compareRows(t1, t2)==0?true:false));
					}
					else if(exprstk.peek().op==ExprTree.OpCode.GT){
						exprstk.pop();
						Datum[] t1={((Datum)tmp.pop())};
						Datum[] t2={((Datum)tmp.pop())};
						tmp.push(new Datum.Bool(Datum.compareRows(t1, t2)==1?true:false));
					}
					else if(exprstk.peek().op==ExprTree.OpCode.GTE){
						exprstk.pop();
						Datum[] t1={((Datum)tmp.pop())};
						Datum[] t2={((Datum)tmp.pop())};
						tmp.push(new Datum.Bool((Datum.compareRows(t1, t2)==0||Datum.compareRows(t1, t2)==1)?true:false));
					}
					else if(exprstk.peek().op==ExprTree.OpCode.LT){
						exprstk.pop();
						Datum[] t1={((Datum)tmp.pop())};
						Datum[] t2={((Datum)tmp.pop())};
						tmp.push(new Datum.Bool((Datum.compareRows(t1, t2)==-1)?true:false));
					}
					else if(exprstk.peek().op==ExprTree.OpCode.LTE){
						exprstk.pop();
						Datum[] t1={((Datum)tmp.pop())};
						Datum[] t2={((Datum)tmp.pop())};
						tmp.push(new Datum.Bool((Datum.compareRows(t1, t2)==0||Datum.compareRows(t1, t2)==-1)?true:false));
					}
					else if(exprstk.peek().op==ExprTree.OpCode.NEQ){
						exprstk.pop();
						Datum[] t1={((Datum)tmp.pop())};
						Datum[] t2={((Datum)tmp.pop())};
						tmp.push(new Datum.Bool((Datum.compareRows(t1, t2)==1||Datum.compareRows(t1, t2)==-1)?true:false));
					}
					else if(exprstk.peek().op==ExprTree.OpCode.NOT){
						exprstk.pop();
						m=((Datum)tmp.pop()).toBool();
						tmp.push(new Datum.Bool(!m));
					}
					else if(exprstk.peek().op==ExprTree.OpCode.AND){
						exprstk.pop();
						m=((Datum)tmp.pop()).toBool();
						n=((Datum)tmp.pop()).toBool();
						tmp.push(new Datum.Bool(m&&n));
					}
					else if(exprstk.peek().op==ExprTree.OpCode.OR){
						exprstk.pop();
						m=((Datum)tmp.pop()).toBool();
						n=((Datum)tmp.pop()).toBool();
						tmp.push(new Datum.Bool(m||n));
					}
					else {
						exprstk.pop();
						String s=((Datum)tmp.pop()).toString();
						tmp.push(new Datum.Str(s));
					}

			
				}
			 
		 }
		 return tmp.pop();
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

