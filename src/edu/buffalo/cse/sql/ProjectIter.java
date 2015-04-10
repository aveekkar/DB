package edu.buffalo.cse.sql;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Stack;

import edu.buffalo.cse.sql.Schema.Var;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.Datum.CastError;
import edu.buffalo.cse.sql.plan.ExprTree;
import edu.buffalo.cse.sql.plan.PlanNode;
import edu.buffalo.cse.sql.plan.ProjectionNode;
import edu.buffalo.cse.sql.plan.ExprTree.VarLeaf;

class ProjectIter implements Iterator{
	Stack<ExprTree> expr=new Stack<ExprTree>();
	ArrayList<Schema.Var> varadd=new ArrayList<Schema.Var>();
	ArrayList<ProjectionNode.Column> cols=new ArrayList<ProjectionNode.Column>();
	int colsize;
	Iterator ob;
	ArrayList<ExprTree> exproot=new ArrayList<ExprTree>();
	ArrayList<Schema.Var> var=new ArrayList<Schema.Var>();
	PlanNode node;
	
	public ProjectIter(){
		
	}
	
	public ProjectIter(PlanNode node,Iterator ob,ArrayList<Schema.Var> var){
		cols=(ArrayList<ProjectionNode.Column>) ((ProjectionNode)node).getColumns();
		colsize=cols.size();
		varadd.addAll(((ProjectionNode)node).getSchemaVars());
		this.ob=ob;
		this.var=var;
		this.node=node;
		for(ProjectionNode.Column c : cols){
			exproot.add(c.expr);
		}
		varadd=(ArrayList<Var>) node.getSchemaVars();
	}
	
	public ArrayList<Datum[]> project(PlanNode node,ArrayList<Datum[]> data,ArrayList<Schema.Var> var) 
			throws CastError{
		ArrayList<ArrayList<Datum[]>> tempdat=new ArrayList<ArrayList<Datum[]>>();
		ArrayList<Datum[]> retdat=new ArrayList<Datum[]>();
		ArrayList<ProjectionNode.Column> cols=new ArrayList<ProjectionNode.Column>();
		cols=(ArrayList<ProjectionNode.Column>) ((ProjectionNode)node).getColumns();
		int colsize=cols.size();
		int size=data.size();
		varadd.addAll(((ProjectionNode)node).getSchemaVars());
		ArrayList<ExprTree> exproot=new ArrayList<ExprTree>();
		for(ProjectionNode.Column c : cols){
			exproot.add(c.expr);
		}
		
		if(((ProjectionNode)node).getChild().type!=PlanNode.Type.NULLSOURCE&&
				((PlanNode.Unary) node).getChild().type!=PlanNode.Type.SCAN){
			//System.out.println("do I ever get called????");
			for(ExprTree e:exproot){
				//ArrayList<Datum[]> datum=new ArrayList<Datum[]>();
				//datum.add(null);
				tempdat.add((eval(e,data,var)));
			}
			for(int j=0;j<size;j++){
			 Datum dat[]=new Datum[colsize];
			 for(int i=0;i<colsize;i++){
				dat[i]=tempdat.get(i).get(j)[0];
			 }
			 retdat.add(dat);
			}
			 
			varadd=(ArrayList<Var>) node.getSchemaVars();
		}
		
		else if( ((PlanNode.Unary) node).getChild().type==PlanNode.Type.SCAN){
			int arr[]=new int[cols.size()];
			int z=0;
			for(int i=0;i<cols.size();i++){
				for(int j=0;j<var.size();j++){
					//System.out.println("to project column: "+cols.get(i).expr.toString());
					//System.out.println("looking in: "+var.get(j).name);
					if(cols.get(i).expr.toString().split("\\.")[0].length()==cols.get(i).expr.toString().length()){
						if(cols.get(i).expr.toString().equalsIgnoreCase(var.get(j).name)){
							//System.out.println("got index: "+j);
							arr[z]=j;
							z++;
						}
					}
					else{
						if(cols.get(i).expr.toString().equalsIgnoreCase(var.get(j).rangeVariable+"."+var.get(j).name)){
							//System.out.println("got index: "+j);
							arr[z]=j;
							z++;
						}
					}
				}
			}
			
			for(int k=0;k<data.size();k++){
				Datum[] dat=new Datum[node.getSchemaVars().size()];
				for(int j=0;j<node.getSchemaVars().size();j++){
					//System.out.println("adding data at: "+arr[j]);
					dat[j]=((data.get(k))[arr[j]]);
					//System.out.println("added data: "+dat[j].toString());
				}
				retdat.add(dat);
			}
			
		}
		else{
			for(ExprTree e:exproot){
				ArrayList<Datum[]> datum=new ArrayList<Datum[]>();
				datum.add(null);
				tempdat.add((eval(e,datum,var)));
			}
			Datum dat[]=new Datum[colsize];
			for(int i=0;i<colsize;i++){
				dat[i]=tempdat.get(i).get(0)[0];
			}
			retdat.add(dat);
			for(ProjectionNode.Column c : cols){
				Schema.Var var1=new Schema.Var(c.name);
				varadd.add(var1);
			}
		}
		
		return retdat;
	}
	
	
	private ArrayList<Datum[]> eval(ExprTree exprroot,ArrayList<Datum[]> data,ArrayList<Schema.Var> var) 
			throws CastError {
		ArrayList<Datum[]> intdat=new ArrayList<Datum[]>();
		int size1=data.size();
		for(int q=0;q<size1;q++){
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

					System.out.println(tmp.size()+exprstk.size());

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
		Datum datum[]=new Datum[1];
		datum[0]=tmp.pop();
		tmp.clear();
		intdat.add(datum);
		}
		return intdat;
	}
	
	private Datum eval(ExprTree exprroot,ArrayList<Schema.Var> var,Datum[] send) 
			throws CastError {
			Stack<Datum> tmp=new Stack<Datum>();
			Stack<ExprTree> exprstk=new Stack<ExprTree>();
			Datum arr[]=new Datum[var.size()];
			//System.out.println("calling join next");
			arr=send;
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
		Datum datum[]=new Datum[1];
		datum[0]=tmp.pop();
		tmp.clear();
		//System.out.println("returning from proj eval: "+datum[0]);
		return datum[0];
	}
	
	
	public void exprEval(ExprTree exp){
		expr.push(exp);
		if(exp.op==ExprTree.OpCode.CONST||exp.op==ExprTree.OpCode.VAR){
			return;
		}
		else{
			exprEval(exp.get(0));
			if(!(exp.size()==1)){
				exprEval(exp.get(1));}


		}
	}


	public ArrayList<Schema.Var> getMap(){
		return varadd;
	}


	@Override
	public boolean hasNext() {
		//System.out.println("project has next");
		if(ob.hasNext()==false){
			return false;
		}
		else{
			return true;
		}
		
	}


	@Override
	public Datum[] next() {
		
		Datum[] ret=new Datum[colsize];
		if(((ProjectionNode)node).getChild().type!=PlanNode.Type.NULLSOURCE&&
				((PlanNode.Unary) node).getChild().type!=PlanNode.Type.SCAN){
			//System.out.println("do I ever get called????");
			Datum[] send=new Datum[var.size()];
			send=(Datum[]) ob.next();
			int i=0;
			for(ExprTree e:exproot){
				try {
					ret[i]=eval(e,var,send);
				} catch (CastError e1) {
					e1.printStackTrace();
				}
				i++;
			}
		}
		
		else if( ((PlanNode.Unary) node).getChild().type==PlanNode.Type.SCAN){
			//System.out.println("inside middle one");
			int arr[]=new int[cols.size()];
			int z=0;
			for(int i=0;i<cols.size();i++){
				for(int j=0;j<var.size();j++){
					//System.out.println("to project column: "+cols.get(i).expr.toString());
					//System.out.println("looking in: "+var.get(j).name);
					if(cols.get(i).expr.toString().split("\\.")[0].length()==cols.get(i).expr.toString().length()){
						if(cols.get(i).expr.toString().equalsIgnoreCase(var.get(j).name)){
							//System.out.println("got index: "+j);
							arr[z]=j;
							z++;
						}
					}
					else{
						if(cols.get(i).expr.toString().equalsIgnoreCase(var.get(j).rangeVariable+"."+var.get(j).name)){
							//System.out.println("got index: "+j);
							arr[z]=j;
							z++;
						}
					}
				}
			}
			
			Datum[] temphold=new Datum[this.var.size()];
			temphold=(Datum[]) ob.next();
			for(int k=0;k<colsize;k++){
				ret[k]=temphold[arr[k]];
			}
			
			
		}
		else{
			//System.out.println("before calling eval");
			int i=0;Datum[] send=new Datum[var.size()];
			send=(Datum[]) ob.next();
			for(ExprTree e:exproot){
				//ArrayList<Datum[]> datum=new ArrayList<Datum[]>();
				//datum.add(null);
				try {
					ret[i]=eval(e,var,send);
				} catch (CastError e1) {
					e1.printStackTrace();
				}
				i++;
			}
			
			for(ProjectionNode.Column c : cols){
				Schema.Var var1=new Schema.Var(c.name);
				varadd.add(var1);
			}
		}
		
		//System.out.println("Returning from Project Iter"+ ret[0]+" "+ret[1]);
		return ret;
	}


	@Override
	public void remove() {
		// TODO Auto-generated method stub
		
	}

}




