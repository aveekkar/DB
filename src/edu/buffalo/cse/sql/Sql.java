
package edu.buffalo.cse.sql;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.List;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.util.Stack;

import edu.buffalo.cse.sql.Schema.TableFromFile;
import edu.buffalo.cse.sql.Schema.Var;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.Datum.CastError;
import edu.buffalo.cse.sql.optimizer.FinalRewrite;
import edu.buffalo.cse.sql.optimizer.MakeJoin;
import edu.buffalo.cse.sql.optimizer.PushDownSelect;
import edu.buffalo.cse.sql.plan.AggregateNode;
import edu.buffalo.cse.sql.plan.ExprTree;
import edu.buffalo.cse.sql.plan.JoinNode;
import edu.buffalo.cse.sql.plan.JoinNode.JType;
import edu.buffalo.cse.sql.plan.PlanNode;
import edu.buffalo.cse.sql.plan.ProjectionNode;
import edu.buffalo.cse.sql.plan.ScanNode;
import edu.buffalo.cse.sql.plan.SelectionNode;
import edu.buffalo.cse.sql.util.TableView;
import edu.buffalo.cse.sql.TestIt;
public class Sql 
{
	public static boolean tpch=false;
	public static boolean index=false;
	public static boolean explain=false;
	static boolean b=false;
	public static void main( String[] args ) throws FileNotFoundException, CastError, ParseException
	{
		String fl=null;
		String norm=null;
		
		for(int i=0;i<args.length;i++){
			if(args[i].equalsIgnoreCase("-explain")){
				explain=true;
			}
			else if(args[i].equalsIgnoreCase("-index")){
				index=true;
			}
			else if(args[i].length()>=9&&args[i].substring(0, 9).equalsIgnoreCase("test/tpch")){
				tpch=true;
				fl=args[i];
			}
			else if(args[i].startsWith("test/")){
				b=true;
				norm=args[i];
			}
		}
		
		try {
			if(tpch){
				System.out.println("executing file: "+fl);
				execFile(new File(fl));
			}
			else if(b){
				execFile(new File(norm));
			}
		} catch (SqlException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	public static List<Datum[]> execQuery(Map<String, Schema.TableFromFile> tables,PlanNode q)throws Exception 
	{  
	if(explain){
		System.out.println("===Before===");
		System.out.println(q.toString());
	}
	CreateIndex create=null;
	PushDownSelect rewrite=new PushDownSelect();
	q=rewrite.rewrite(q);
	MakeJoin jrewrt=new MakeJoin();
	q=jrewrt.smj(q);
	if(index){
		FinalRewrite again=new FinalRewrite();
		again.Join(q);
		System.out.println("===After rewrite using indices===");
		System.out.println(q.toString());
		create=new CreateIndex();
		for(int i=0;i<FinalRewrite.finalkeys.size();i++){
			create.create(FinalRewrite.finalkeys.get(i));
		}
	}
	Stack<ArrayList<Schema.Var>> mapstk=new Stack<ArrayList<Schema.Var>>();
	Stack<ArrayList<Datum[]>> ret=new Stack<ArrayList<Datum[]>>();
	Stack<ArrayList<Datum[]>> projret=new Stack<ArrayList<Datum[]>>();
	ArrayList<Datum[]> tempdat=new ArrayList<Datum[]>();
	Stack<PlanNode> stkhold=new Stack<PlanNode>();
	ParseEngine parse=new ParseEngine();
	Stack<PlanNode> stk=parse.getOps(q);
	Stack<PlanNode> stk1=parse.getLeaf();
	int size=stk1.size();
	int size1=stk.size();
	ParseEngine parse1=new ParseEngine();
	Stack<PlanNode> stktemp1=parse1.getOps(q);
	Stack<PlanNode> stktemp2=parse1.getLeaf();
	Stack<Iterator> itstk=new Stack<Iterator>();
	PlanNode temp;
	
	if(explain){
		System.out.println("===After===");
		System.out.println(q.toString());
		return null;
	}
	
	
	String key[]=new String[size];
	for(int k=0;k<size;k++){
		temp=stk1.pop();
		if(temp.type==PlanNode.Type.SCAN){
			key[k]=((ScanNode)temp).table.toUpperCase();
		}
		else{
			key[k]="[";
		}
	}

	HashMap<String,Schema.TableFromFile> tab=new HashMap<String,Schema.TableFromFile>();
	for(int k=0;k<size;k++){
		tab.put(key[k], tables.get(key[k]));
	}

	
	PlanNode node1;
	while(!stktemp1.isEmpty()){
		node1=stktemp1.pop();
		if(node1.type==PlanNode.Type.PROJECT){
			ArrayList<Datum[]> tmp=new ArrayList<Datum[]>();
			if(itstk.isEmpty()){
				ProjectIter proj1=new ProjectIter();
				ret.push(proj1.project(node1,ret.pop(), mapstk.pop()));
				mapstk.push(proj1.getMap());
			}
			else{
				Iterator onj=itstk.pop();
				ProjectIter proj=new ProjectIter(node1,onj,mapstk.pop());
				//tempdat=proj.project(node1,ret.pop(), mapstk.pop());
				//ret.push(tempdat);
				itstk.push(proj);
				mapstk.push(proj.getMap());
				while(proj.hasNext()){
					tmp.add(((Datum[])proj.next()));
				}
				ret.push(tmp);
			}
		}

		else if(node1.type==PlanNode.Type.SELECT){
			Iterator obj= itstk.pop();
			SelectIter sel=new SelectIter(node1,obj,mapstk.pop());
			itstk.push(sel);
			mapstk.push(sel.getMap());
		}

		else if(node1.type==PlanNode.Type.AGGREGATE){
			Iterator obj=itstk.pop();
			AggregateIter agg=new AggregateIter();
			tempdat=agg.aggregate(node1, obj, mapstk.pop());
			ret.push(tempdat);
			mapstk.push(agg.getMap());
		}

		else if(node1.type==PlanNode.Type.JOIN){
			if(index&&tpch==false){
				SMJ smj=new SMJ(((JoinNode)node1),itstk.pop(),itstk.pop(),mapstk.pop(),mapstk.pop());
				itstk.push(smj);
				mapstk.push(smj.getSchemaVars());
			}
			else if(((JoinNode)node1).getJoinType()==JType.NLJ){
				Iterator obj1=itstk.pop();
				Iterator obj2=itstk.pop();
				JoinIter iter=new JoinIter(node1,obj1,obj2,mapstk);
				itstk.push(iter);
				mapstk=iter.getMap();
			}
			else if(((JoinNode)node1).getJoinType()==JType.MERGE){
				SMJ smj=new SMJ(((JoinNode)node1),itstk.pop(),itstk.pop(),mapstk.pop(),mapstk.pop());
				itstk.push(smj);
				mapstk.push(smj.getSchemaVars());
			}
			else{
				INLJ inlj=new INLJ(((JoinNode)node1),itstk.pop(),itstk.pop(),mapstk.pop(),mapstk.pop(),create.getIndex());
				itstk.push(inlj);
				mapstk.push(inlj.getSchemavars());
			}
		}
		else if(node1.type==PlanNode.Type.SCAN){
			String a=((ScanNode)node1).table.toUpperCase();
			TestIt obj=new TestIt(tab,a);
			itstk.push(obj);
			mapstk.push((ArrayList<Var>) node1.getSchemaVars());
			
		}
		else if(node1.type==PlanNode.Type.NULLSOURCE){
			TestIt obj=new TestIt(tab,"]");
			itstk.push(obj);
			mapstk.push((ArrayList<Var>) node1.getSchemaVars());
		}

		else{
			UnionIter uni=new UnionIter();
			tempdat=(uni.union(node1,ret.pop(),ret.pop(),mapstk.pop()));
			ret.push(tempdat);
			mapstk.push(uni.getMap());
		} 




	}

		if(tpch||b){
			TableView view=new TableView();
			view.show(ret.peek(), mapstk.peek());
		}
	
	
		return ret.pop();
	
	}

	public static List<List<Datum[]>> execFile(
			File program
			)
					throws SqlException, Exception
					{ 
		String arr[]={"test/"+program.toString().substring(5)};
		//System.out.println(arr[0]);
		Program p=SqlParser.main(arr);
		//System.out.println(p.toString());
		Map<String, Schema.TableFromFile> map=p.tables;
		//System.out.println("inside map: "+p.tables.toString());
		ArrayList<PlanNode> query=(ArrayList<PlanNode>)p.queries;
		List<List<Datum[]>> ret = new ArrayList<List<Datum[]>>();
		ret.add((ArrayList<Datum[]>) execQuery(map, query.get(0)));
		return ret;
		
		
					}


}


