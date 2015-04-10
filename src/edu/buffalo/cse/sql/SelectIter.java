package edu.buffalo.cse.sql;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Stack;

import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.Datum.CastError;
import edu.buffalo.cse.sql.plan.ExprTree;
import edu.buffalo.cse.sql.plan.ExprTree.VarLeaf;
import edu.buffalo.cse.sql.plan.PlanNode;
import edu.buffalo.cse.sql.plan.SelectionNode;

public class SelectIter implements Iterator {
	ArrayList<Schema.Var> map=new ArrayList<Schema.Var>();
	ArrayList<Schema.Var> var=new ArrayList<Schema.Var>();
	ArrayList<Datum[]> dat=new ArrayList<Datum[]>();
	Iterator obiter;
	ArrayList<Datum[]> dat1=new ArrayList<Datum[]>();
	Stack<Schema.Var> varleaf=new Stack<Schema.Var>();
	ExprTree exprroot;
	Stack<ExprTree> expr=new Stack<ExprTree>();
	Datum[] outp;
	int damn=0;
	SelectIter(){
		
	}
	SelectIter(PlanNode node,Iterator ob,ArrayList<Schema.Var> var){
	map=var;
	this.var=var;
	outp= new Datum[var.size()];
	obiter=ob;
	exprroot=((SelectionNode)node).getCondition();
	}

	static int r=0;
	public ArrayList<Datum[]> select(PlanNode node,ArrayList<Datum[]> data,ArrayList<Schema.Var> var)
			throws CastError{
		dat=data;
		map=var;
		int size=data.size();
		ExprTree exprroot=((SelectionNode)node).getCondition();
		int index[]=new int[size];

		for(int q=0;q<size;q++){
			Stack<Datum> tmp=new Stack<Datum>();
			Stack<ExprTree> exprstk=new Stack<ExprTree>();
			Datum arr[]=new Datum[var.size()];
			arr=data.get(q);
			expr.clear();
			exprEval(exprroot);
			exprstk=expr;
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
					for(int z=0;z<var.size();z++){
						if(var.get(z).name.equalsIgnoreCase(temp.name.toString())||(var.get(z).rangeVariable+"."+var.get(z).name).equalsIgnoreCase(temp.name.toString())){
							x=z;
						}	
					}
					tmp.push(arr[x]);
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
						k=((Datum)tmp.pop()).toInt();
						j=((Datum)tmp.pop()).toInt();
						tmp.push(new Datum.Bool(k==j));
					}
					else if(exprstk.peek().op==ExprTree.OpCode.GT){
						exprstk.pop();
						k=((Datum)tmp.pop()).toInt();
						j=((Datum)tmp.pop()).toInt();
						tmp.push(new Datum.Bool(k>j));
					}
					else if(exprstk.peek().op==ExprTree.OpCode.GTE){
						exprstk.pop();
						k=((Datum)tmp.pop()).toInt();
						j=((Datum)tmp.pop()).toInt();
						tmp.push(new Datum.Bool(k>=j));
					}
					else if(exprstk.peek().op==ExprTree.OpCode.LT){
						exprstk.pop();
						k=((Datum)tmp.pop()).toInt();
						j=((Datum)tmp.pop()).toInt();
						tmp.push(new Datum.Bool(k<j));
					}
					else if(exprstk.peek().op==ExprTree.OpCode.LTE){
						exprstk.pop();
						k=((Datum)tmp.pop()).toInt();
						j=((Datum)tmp.pop()).toInt();
						tmp.push(new Datum.Bool(k<=j));
					}
					else if(exprstk.peek().op==ExprTree.OpCode.NEQ){
						exprstk.pop();
						k=((Datum)tmp.pop()).toInt();
						j=((Datum)tmp.pop()).toInt();
						tmp.push(new Datum.Bool(k!=j));
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
			if(!(tmp.pop().toBool())){
				tmp.clear();
				index[q]=1;
			}
			else{
				tmp.clear();
				index[q]=0;
			}
		}

		for(int k=0;k<size;k++){
			if(index[k]==0){
				dat1.add(dat.get(k));
			}
		}
		return dat1;
	}




	public void  exprEval(ExprTree exp){

		expr.push(exp);
		if(exp.op==ExprTree.OpCode.CONST||exp.op==ExprTree.OpCode.VAR){
			return ;
		}
		else{
			exprEval(exp.get(0));
			if(!(exp.size()==1)){
				exprEval(exp.get(1));}


		}
	}




	public ArrayList<Schema.Var> getMap(){
		return map;
	}




	@Override
	public boolean hasNext() {
		while(obiter.hasNext()){
			try {
				if(nextHelper((Datum[]) obiter.next())){
					//System.out.println("returning true from select");
					return true;
				}
			} catch (CastError e) {
				e.printStackTrace();
				System.exit(-1);
			}
		}
		return false;
	}

	private boolean nextHelper(Datum[] rows) throws CastError{
		//System.out.println("selecting for: ");
		//System.out.println(exprroot.op);
		Stack<Datum> tmp=new Stack<Datum>();
		Datum arr[]=new Datum[map.size()];
		arr=rows;
		outp=rows;
		Stack<ExprTree> exprstk=new Stack<ExprTree>();
		
		
		expr.clear();
		exprEval(exprroot);
		exprstk=expr;
		//System.out.println("exprstk: "+exprstk);
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
				for(int z=0;z<map.size();z++){
					if(var.get(z).name.equalsIgnoreCase(temp.name.toString())||(var.get(z).rangeVariable+"."+var.get(z).name).equalsIgnoreCase(temp.name.toString())){
						x=z;
					}	
				}
				tmp.push(arr[x]);
				//System.out.println(tmp);
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
					//System.out.println("comparing: "+t1[0].toString()+"== "+t2[0].toString());
					tmp.push(new Datum.Bool(Datum.compareRows(t1, t2)==0));
				}
				else if(exprstk.peek().op==ExprTree.OpCode.GT){
					exprstk.pop();
					Datum[] t1={((Datum)tmp.pop())};
					Datum[] t2={((Datum)tmp.pop())};
					//System.out.println("comparing: "+t1[0].toString()+" > "+t2[0].toString());
					tmp.push(new Datum.Bool(Datum.compareRows(t1, t2)==1));
					//System.out.println(tmp);
				}
				else if(exprstk.peek().op==ExprTree.OpCode.GTE){
					exprstk.pop();
					Datum[] t1={((Datum)tmp.pop())};
					Datum[] t2={((Datum)tmp.pop())};
					//System.out.println("comparing: "+t1[0].toString()+">= "+t2[0].toString());
					tmp.push(new Datum.Bool((Datum.compareRows(t1, t2)==0||Datum.compareRows(t1, t2)==1)));
				}
				else if(exprstk.peek().op==ExprTree.OpCode.LT){
					exprstk.pop();
					Datum[] t1={((Datum)tmp.pop())};
					Datum[] t2={((Datum)tmp.pop())};
					//System.out.println("comparing: "+t1[0].toString()+"< "+t2[0].toString());
					tmp.push(new Datum.Bool((Datum.compareRows(t1, t2)==-1)));
				}
				else if(exprstk.peek().op==ExprTree.OpCode.LTE){
					exprstk.pop();
					Datum[] t1={((Datum)tmp.pop())};
					Datum[] t2={((Datum)tmp.pop())};
					//System.out.println("comparing: "+t1[0].toString()+"<= "+t2[0].toString()+" "+Datum.compareRows(t1, t2));
					tmp.push(new Datum.Bool((Datum.compareRows(t1, t2)==0||Datum.compareRows(t1, t2)==-1)));
				}
				else if(exprstk.peek().op==ExprTree.OpCode.NEQ){
					exprstk.pop();
					Datum[] t1={((Datum)tmp.pop())};
					Datum[] t2={((Datum)tmp.pop())};
					//System.out.println("comparing: "+t1[0].toString()+" != "+t2[0].toString());
					tmp.push(new Datum.Bool((Datum.compareRows(t1, t2)==1||Datum.compareRows(t1, t2)==-1)));
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
		/*if(tmp.peek().toBool()){
			System.out.println("true for: "+Datum.stringOfRow(rows));
		}*/
		return tmp.pop().toBool();
	}


	@Override
	public Datum[] next() {
		//System.out.println("returning: "+ outp[0].toString()+" "+ outp[1].toString());
		//System.out.println("from select: "+Datum.stringOfRow(outp));
		return outp;
	}




	@Override
	public void remove() {
		// TODO Auto-generated method stub
		
	}


}
