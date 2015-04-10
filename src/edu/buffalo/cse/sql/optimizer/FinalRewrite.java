package edu.buffalo.cse.sql.optimizer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Stack;

import edu.buffalo.cse.sql.ParseEngine;
import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.Schema.Var;
import edu.buffalo.cse.sql.plan.ExprTree;
import edu.buffalo.cse.sql.plan.JoinNode;
import edu.buffalo.cse.sql.plan.JoinNode.JType;
import edu.buffalo.cse.sql.plan.PlanNode;
import edu.buffalo.cse.sql.plan.ScanNode;

public class FinalRewrite {
	
	public static HashMap<String,Integer> map=new HashMap<String,Integer>();
	public static ArrayList<String> keys=new ArrayList<String>();
	public static ArrayList<String> finalkeys=new ArrayList<String>();
	
	public PlanNode Join(PlanNode q){
		ParseEngine parse=new ParseEngine();
		Stack<PlanNode> stk=parse.getOps(q);
		Stack<PlanNode> tables=parse.getLeaf();
		//System.out.println(tables);
		int i=0;
		while(!tables.isEmpty()){
			keys.add(((ScanNode)tables.pop()).table.substring(0, 1).toLowerCase());
			//System.out.println(keys.get(i));
			i++;
		}
		
		while(!stk.isEmpty()){
			PlanNode p=stk.pop();
			if(p.type==PlanNode.Type.JOIN&&(((JoinNode)p).getJoinType())!=JType.NLJ
					&&(((JoinNode)p).getRHS().type!=PlanNode.Type.SELECT)){
				whatever(((JoinNode)p));
				JoinNode j=rewrite((((JoinNode)p)));
				j.setLHS(((JoinNode)p).getLHS());
				j.setRHS(((JoinNode)p).getRHS());
				if(stk.peek().struct==PlanNode.Structure.UNARY){
					((PlanNode.Unary)stk.peek()).setChild(j);
				}
				else{
					((PlanNode.Binary)stk.peek()).setLHS(j);
				}
			}
		}
		
		return q;
	}
	
	private void whatever(JoinNode j){
		try {
			if(j.getJoinType()!=JType.NLJ){
				ExprTree exp=j.getExp();
				PlanNode p=j.getRHS();
				PlanNode t=j.getLHS();
				ScanNode scan=null;
				ScanNode scan1=null;
				if(p.type==PlanNode.Type.SCAN){
					scan=(ScanNode) j.getRHS();
				}
				else{
					scan=(ScanNode)((PlanNode.Unary)j.getRHS()).getChild();
				}
				
				if(t.type==PlanNode.Type.SCAN||t.type==PlanNode.Type.SELECT){
					if(t.type==PlanNode.Type.SCAN){
						scan1=(ScanNode) j.getLHS();
					}
					else if(((PlanNode.Unary)j.getLHS()).getChild().type!=PlanNode.Type.JOIN){
						scan1=(ScanNode)((PlanNode.Unary)j.getLHS()).getChild();
					}
				}
				
				String s=scan.table.substring(0, 1).toLowerCase();
				ArrayList<Schema.Var> var1=null;
				String s1=null;
				int iidx1=-1;
				if(scan1!=null){
					s1=scan1.table.substring(0, 1).toLowerCase();
					var1=(ArrayList<Var>) scan1.getSchemaVars();
					if(exp!=null&&exp.get(0).toString().substring(0, 1).equals(s1)){
						for(int i=0;i<var1.size();i++){
							if(exp.get(0).toString().substring(2).equalsIgnoreCase(var1.get(i).name)){
								iidx1=i;
								break;
							}
						}
					}
					else{
						for(int i=0;i<var1.size();i++){
							if(exp!=null&&exp.get(1).toString().substring(2).equalsIgnoreCase(var1.get(i).name)){
								iidx1=i;
								break;
							}
						}
					}
					
					map.put(s1, iidx1);
					finalkeys.add(s1);
					//System.out.println("key: "+s1+" index: "+var1.get(iidx1));
				}
				ArrayList<Schema.Var> var=(ArrayList<Var>) scan.getSchemaVars();
				int iidx=-1;
				if(exp!=null&&exp.get(0).toString().substring(0, 1).equals(s)){
					for(int i=0;i<var.size();i++){
						if(exp.get(0).toString().substring(2).equalsIgnoreCase(var.get(i).name)){
							iidx=i;
							break;
						}
					}
				}
				else{
					for(int i=0;i<var.size();i++){
						if(exp!=null&&exp.get(1).toString().substring(2).equalsIgnoreCase(var.get(i).name)){
							iidx=i;
							break;
						}
					}
				}
				map.put(s, iidx);
				finalkeys.add(s);
				//System.out.println("key: "+s+" index: "+var.get(iidx));
			}
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private JoinNode rewrite(JoinNode j){
		try {
			ExprTree exp=j.getExp();
			JoinNode node=new JoinNode("index",exp);
			node.setJoinType(JType.INDEX);
			return node;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
}
