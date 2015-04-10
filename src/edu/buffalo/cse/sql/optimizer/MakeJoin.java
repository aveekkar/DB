package edu.buffalo.cse.sql.optimizer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Stack;

import edu.buffalo.cse.sql.ParseEngine;
import edu.buffalo.cse.sql.plan.ExprTree;
import edu.buffalo.cse.sql.plan.JoinNode;
import edu.buffalo.cse.sql.plan.JoinNode.JType;
import edu.buffalo.cse.sql.plan.PlanNode;
import edu.buffalo.cse.sql.plan.ScanNode;
import edu.buffalo.cse.sql.plan.SelectionNode;

public class MakeJoin {
	
	HashMap<String[],ExprTree> map;
	ArrayList<String[]> keys=new ArrayList<String[]>();
	static int keyidx=-1;
	ArrayList<String> checklist=new ArrayList<String>();
	Stack<ExprTree.OpCode> opstk=new Stack<ExprTree.OpCode>();
	Stack<ExprTree> varstk=new Stack<ExprTree>();
	
	public PlanNode smj(PlanNode q){
		Stack<PlanNode> stkop=new Stack<PlanNode>();
		ParseEngine parse=new ParseEngine();
		stkop=parse.getOps(q);
		
		while(!stkop.isEmpty()){
			PlanNode p=stkop.pop();
			if(p.type==PlanNode.Type.SELECT){
				if(((SelectionNode)p).getChild().type==PlanNode.Type.JOIN){
					SelectionNode sel=(SelectionNode) sepExpr(((SelectionNode)p).getCondition());
					if(sel.getCondition()==null||sel.getCondition().isEmpty()){
						((PlanNode.Unary)stkop.pop()).setChild(((SelectionNode)p).getChild());
					}
					else{
						sel.setChild(((PlanNode.Unary)p).getChild());
						((PlanNode.Unary)stkop.peek()).setChild(sel);
					}
				}
			}
		
		}
		stkop=parse.getOps(q);
		while(!stkop.isEmpty()){
			PlanNode p=stkop.pop();
			if(p.type==PlanNode.Type.JOIN){
				ArrayList<String> list=new ArrayList<String>();
				getList(p,list);
				JoinNode j=(JoinNode) makeJoin(list);
				list.clear();
				if(j==null){
					continue;
				}
				j.setLHS(((JoinNode)p).getLHS());
				j.setRHS(((JoinNode)p).getRHS());
				if(stkop.peek().struct==PlanNode.Structure.UNARY){
					((PlanNode.Unary)stkop.peek()).setChild(j);
				}
				else{
					if(((PlanNode.Binary)stkop.peek()).getLHS().type==PlanNode.Type.JOIN){
						((PlanNode.Binary)stkop.peek()).setLHS(j);
					}
					else if(((PlanNode.Binary)stkop.peek()).getRHS().type==PlanNode.Type.JOIN){
						((PlanNode.Binary)stkop.peek()).setRHS(j);
					}
				}
			}
			
		}
		
		if(map!=null&&map.size()>0){
			stkop=parse.getOps(q);
			while(!stkop.isEmpty()){
				PlanNode p=stkop.pop();
				if(p.type==PlanNode.Type.JOIN){
					try {
						boolean t=check(((JoinNode)p).getExp());
						if(t){
							if(stkop.peek().struct==PlanNode.Structure.BINARY){
								PlanNode gg=((PlanNode.Binary)stkop.peek()).getLHS();
								SelectionNode sel=new SelectionNode(map.get(keys.get(0)));
								((PlanNode.Binary)stkop.peek()).setLHS(sel);
								sel.setChild(gg);
								break;
							}
							else{
								PlanNode gg=((PlanNode.Unary)stkop.peek()).getChild();
								SelectionNode sel=new SelectionNode(map.get(keys.get(0)));
								((PlanNode.Unary)stkop.peek()).setChild(sel);
								sel.setChild(gg);
								break;
							}
						}
					} catch (Exception e) {
						continue;
					}
				}
			}
		}
		
		
		return q;
	}
	
	private PlanNode sepExpr(ExprTree cond){
		map=new HashMap<String[],ExprTree>();
		ExprTree expr=recurse(cond);
		if(expr!=null&&expr.size()!=0){
			killme(expr);
				while(varstk.size()!=1){
					ExprTree.OpCode op=opstk.pop();
					ExprTree a=varstk.pop();
					//System.out.println(a);
					ExprTree b=null;
					try{
						b=varstk.pop();
						//System.out.println(b);
					}
					catch(Exception e){
						return new SelectionNode(a);
					}
					ExprTree newex=new ExprTree(op,a,b);
					if(varstk.isEmpty()){
						return new SelectionNode(newex);
					}
					else{
						varstk.push(newex);
					}
					
					
				}
				return new SelectionNode(varstk.pop());
		}
		else{
			return new SelectionNode(expr);
		}
		
		
	}
	
	
	private ExprTree recurse(ExprTree exp){
		//System.out.println("root: "+exp.op);
		if(exp.op==ExprTree.OpCode.AND||exp.op==ExprTree.OpCode.OR){
			//System.out.println("root left : "+exp.get(0).op);
			ExprTree left=recurse(exp.get(0));
			//System.out.println("root right : "+exp.get(1).op);
			ExprTree right=recurse(exp.get(1));
			if(left==null){
				//System.out.println("removing left null : "+exp.get(0));
				exp.remove(0);
			}
			else if(left.size()==0){
				//System.out.println("removing left size 0 : "+exp.get(0));
				exp.remove(0);
			}
			else if(left.size()==1){
				exp.remove(0);
				exp.add(0, left.get(0));
			}
			
			
			if(right==null){
				if(exp.size()==1){
					//System.out.println("removing right null exp=2 : "+exp.get(1));
					exp.remove(0);
				}
				else{
					//System.out.println("removing right null exp=1: "+exp.get(0));
					exp.remove(1);
				}
			}
			else if(right.size()==0){
				if(exp.size()==1){
					//System.out.println("removing right size 0 exp=2 : "+exp.get(1));
					exp.remove(0);
				}
				else{
					//System.out.println("removing right size 0 exp=1 : "+exp.get(0));
					exp.remove(1);
				}
			}
			else if(right.size()==1){
					if(exp.size()==1){
						exp.remove(0);
						exp.add(0, right.get(0));
					}
					else{
						exp.remove(1);
						exp.add(1,right.get(0));
					}
				
			}
			//System.out.println("returning: "+exp);
			return exp;
		}
		else{
			//System.out.println("leaf: "+exp);
			if(exp.op==ExprTree.OpCode.EQ){
				String[] key=new String[2];
				key[0]=exp.get(0).toString().substring(0, 1);
				key[1]=exp.get(1).toString().substring(0, 1);
				this.keys.add(key);
				map.put(key, exp);
				return null;
			}
			else{
				//System.out.println("returning invalid: "+exp);
				return exp;
			}
		}
	}
	
	private void getList(PlanNode p,ArrayList<String> list){
		if(p.type!=PlanNode.Type.SCAN){
			if(p.struct==PlanNode.Structure.UNARY){
				getList(((PlanNode.Unary)p).getChild(),list);
			}
			else{
				getList(((PlanNode.Binary)p).getLHS(),list);
				getList(((PlanNode.Binary)p).getRHS(),list);
			}
		}
		else{
			ScanNode scan=(ScanNode)p;
			list.add(scan.table.toString().substring(0, 1).toLowerCase());
		}
	}
	
	private PlanNode makeJoin(ArrayList<String> list){
		int match=0;
		String[] key=null;
		int idx=-1;
		for(int k=keys.size()-1;k>=0;k--){
			match=0;
			String[] elem=keys.get(k);
			for(int i=0;i<elem.length;i++){
				for(String s:list){
					if(elem[i].equalsIgnoreCase(s)){
						//System.out.println("match found: "+s);
						match++;
						idx=k;
						break;
					}
				}
				if(match==2){
					break;
				}
			}
			if(match==2){
				break;
			}
		}
		
		if(match==2){
			key=keys.get(idx);
			//System.out.println("got key: "+key[0]+" "+key[1]);
			ExprTree exp=map.get(key);
			JoinNode node=new JoinNode("smj",exp);
			node.setJoinType(JType.MERGE);
			keys.remove(key);
			map.remove(key);
			return node;
		}
		return null;
	}
	
	private boolean check(ExprTree exp){
		checklist.add(exp.get(0).toString().substring(0,1));
		checklist.add(exp.get(1).toString().substring(0,1));
		String[] tmp=keys.get(0);
		int match=0;
		
		for(int i=0;i<tmp.length;i++){
			for(int j=0;j<checklist.size();j++){
				if(checklist.get(j).equalsIgnoreCase(tmp[i])){
					++match;
					break;
				}
			}
		}
		if(match==2){
			return true;
		}
		return false;
	}
	
	private void killme(ExprTree exp){
		if(exp.op==ExprTree.OpCode.AND||exp.op==ExprTree.OpCode.OR){
			opstk.push(exp.op);
			if(exp.size()==1){
				killme(exp.get(0));
			}
			else{
				killme(exp.get(0));
				killme(exp.get(1));
			}
			
		}
		else{
			varstk.push(exp);
		}
	}
	
}

