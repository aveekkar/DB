package edu.buffalo.cse.sql.optimizer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Stack;

import edu.buffalo.cse.sql.ParseEngine;
import edu.buffalo.cse.sql.SqlException;
import edu.buffalo.cse.sql.plan.ExprTree;
import edu.buffalo.cse.sql.plan.NullSourceNode;
import edu.buffalo.cse.sql.plan.PlanNode;
import edu.buffalo.cse.sql.plan.ScanNode;
import edu.buffalo.cse.sql.plan.SelectionNode;

public class PushDownSelect {

	HashMap<String,ExprTree> map;
	Stack<ExprTree.OpCode> opstk=new Stack<ExprTree.OpCode>();
	Stack<ExprTree> varstk=new Stack<ExprTree>();

	protected PlanNode apply(PlanNode node,ArrayList<String> list) throws SqlException {
	
		ExprTree exproot=((SelectionNode)node).getCondition();
		ArrayList<ExprTree> explist=new ArrayList<ExprTree>();
		map=new HashMap<String,ExprTree>();
		for(int i=0;i<list.size()+1;i++){
			explist.add(makeExp(exproot));
			
		}
		
		
		for(int i=0;i<list.size();i++){
			//System.out.println(exproot);
			ExprTree temp=recurse(explist.get(i),list.get(i));
			if(temp!=null&&temp.size()==1){
				temp=temp.get(0);
			}
			else if(temp!=null&&temp.size()==0){
				temp=null;
			}
			if(temp==null){
				continue;
			}
			else{
				map.put(list.get(i), temp);
			}
		}
		
		
		if(map.isEmpty()){
			return new SelectionNode(explist.get(list.size()));
		}
		//System.out.println(map.get("l"));
		//System.out.println(map.get("c"));
		//System.out.println(map.get("o"));
		
		ExprTree temp=recurse(explist.get(list.size()));
		if(temp!=null&&temp.size()==1){
			temp=temp.get(0);
		}
		else if(temp!=null&&temp.size()==0){
			temp=null;
		}
		else if(temp!=null&&temp.size()>1){
			killme(temp);
			while(!opstk.isEmpty()){
				ExprTree.OpCode op=opstk.pop();
				ExprTree left=varstk.pop();
				ExprTree right=varstk.pop();
				if(left.get(0).toString().equals(right.get(0).toString())&&left.get(1).toString().equals(right.get(1).toString())){
					varstk.push(right);
				}
				else{
					ExprTree tmp=new ExprTree(op,left,right);
					varstk.push(tmp);
				}
			}
			temp=varstk.pop();
		}
		//System.out.println(temp);
		return new SelectionNode(temp);
	}
	
	public PlanNode rewrite(PlanNode node) throws SqlException{
		ParseEngine parse=new ParseEngine();
		Stack<PlanNode> stk=parse.getOps(node);
		ArrayList<String> list=new ArrayList<String>();
		while(!stk.isEmpty()){
			if(stk.peek().type==PlanNode.Type.SCAN){
				list.add(((ScanNode)stk.pop()).table.toString().substring(0, 1).toLowerCase());
			}
			else{
				stk.pop();
			}
		}
		PlanNode n;
		stk=parse.getOps(node);
		while(!stk.isEmpty()){
			n=stk.pop();
			if(n.struct==PlanNode.Structure.UNARY){
				PlanNode.Unary unode=(PlanNode.Unary)n;
				if(unode.getChild().type==PlanNode.Type.SELECT){
					SelectionNode temp=(SelectionNode) apply(unode.getChild(),list);
					if(temp.getCondition()==null){
						SelectionNode next=(SelectionNode) unode.getChild();
						unode.setChild(next.getChild());
						continue;
					}
					SelectionNode next=(SelectionNode) unode.getChild();
					PlanNode temp1=next.getChild();
					unode.setChild(temp);
					temp.setChild(temp1);
				}
				
			}
			else{
				
			}
		}
		set(node);
		return node;
		
	}
	
	private ExprTree recurse(ExprTree exp,String r){
		//System.out.println("root: "+exp.op);
		if(exp.op==ExprTree.OpCode.OR||exp.op==ExprTree.OpCode.AND){
			//System.out.println("root left : "+exp.get(0).op);
			ExprTree left=recurse(exp.get(0),r);
			//System.out.println("root right : "+exp.get(1).op);
			ExprTree right=recurse(exp.get(1),r);
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
			if(exp.get(1).op!=ExprTree.OpCode.VAR&&exp.get(0).toString().substring(0, 1).equalsIgnoreCase(r)){
				//System.out.println("returning valid: "+exp);
				return exp;
			}
			else{
				//System.out.println("returning invalid: "+exp);
				return null;
			}
		}
	}
	
	private ExprTree recurse(ExprTree exp){
		//System.out.println("root: "+exp.op);
		if(exp.op==ExprTree.OpCode.OR||exp.op==ExprTree.OpCode.AND){
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
			if(exp.get(1).op==ExprTree.OpCode.VAR){
				//System.out.println("returning valid: "+exp);
				return exp;
			}
			else{
				//System.out.println("returning invalid: "+exp);
				return null;
			}
		}
	}
	
	private void set(PlanNode node){
		ParseEngine parse=new ParseEngine();
		Stack<PlanNode> stk=parse.getOps(node);
		while(!stk.empty()){
			PlanNode temp=stk.pop();
			if(temp.struct==PlanNode.Structure.LEAF){
				continue;
			}
			else if(temp.struct==PlanNode.Structure.UNARY){
				PlanNode.Unary unode = (PlanNode.Unary)temp;
				if(unode.getChild().type==PlanNode.Type.SCAN){
					ScanNode scan=(ScanNode) unode.getChild();
					String key=scan.table.toString().substring(0, 1).toLowerCase();
					if(map!=null&&map.containsKey(key)){
						SelectionNode select=new SelectionNode(map.get(key));
						if(select.getCondition()==null){
							continue;
						}
						select.setChild(scan);
						unode.setChild(select);
					}
				}
			}
			else{
				PlanNode.Binary bnode=(PlanNode.Binary)temp;
				PlanNode rhs=bnode.getRHS();
				PlanNode lhs=bnode.getLHS();
				if(lhs.type==PlanNode.Type.SCAN){
					ScanNode scan=(ScanNode) lhs;
					String key=scan.table.toString().substring(0, 1).toLowerCase();
					if(map!=null){
						SelectionNode select=new SelectionNode(map.get(key));
						if(select.getCondition()==null){
						}
						else{
							select.setChild(scan);
							bnode.setLHS(select);
						}
					}
					
				}
				if(rhs.type==PlanNode.Type.SCAN){
					ScanNode scan=(ScanNode) rhs;
					String key=scan.table.toString().substring(0, 1).toLowerCase();
					if(map!=null){
						SelectionNode select=new SelectionNode(map.get(key));
						if(select.getCondition()==null){
							continue;
						}
						select.setChild(scan);
						bnode.setRHS(select);
					}
				}
				
			}
		}
	}
	
	private ExprTree makeExp(ExprTree root){
if((root.op==ExprTree.OpCode.OR||root.op==ExprTree.OpCode.AND)){
			
			ExprTree left=makeExp(root.get(0));
			ExprTree.OpCode op=root.op;
			ExprTree right=makeExp(root.get(1));
			return new ExprTree(op,left,right);
		}
		else{
			return root;
		}
	}
	
	private void killme(ExprTree exp){
		if(exp.op==ExprTree.OpCode.AND||exp.op==ExprTree.OpCode.OR){
			opstk.push(exp.op);
			killme(exp.get(0));
			killme(exp.get(1));
		}
		else{
			varstk.push(exp);
		}
	}

}
