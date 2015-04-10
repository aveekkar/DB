package edu.buffalo.cse.sql;

import edu.buffalo.cse.sql.plan.PlanNode;
import edu.buffalo.cse.sql.plan.JoinNode;
import edu.buffalo.cse.sql.plan.ProjectionNode;
import edu.buffalo.cse.sql.plan.UnionNode;
import edu.buffalo.cse.sql.plan.SelectionNode;
import edu.buffalo.cse.sql.plan.AggregateNode;
import java.util.Stack;



public class ParseEngine {
	int i;
	Stack<PlanNode> stkop=new Stack<PlanNode>();
	Stack<PlanNode> stkleaf=new Stack<PlanNode>();

	public Stack<PlanNode> getOps(PlanNode var){
		if(var.struct==PlanNode.Structure.LEAF){
			stkleaf.push(var);
			stkop.push(var);
			return stkop;
		}
		else if(var.struct==PlanNode.Structure.UNARY){
			if(var.type==PlanNode.Type.PROJECT){
				stkop.push(var);
				getOps(((ProjectionNode)var).getChild());
			}
			else if(var.type==PlanNode.Type.SELECT){
				stkop.push(var);
				getOps(((SelectionNode)var).getChild());
			} 
			else if(var.type==PlanNode.Type.AGGREGATE){
				stkop.push(var);
				getOps(((AggregateNode)var).getChild());
			}

		}
		else {
			if(var.type==PlanNode.Type.JOIN){
				stkop.push(var);
				getOps(((JoinNode)var).getLHS());
				getOps(((JoinNode)var).getRHS());
			}
			else if(var.type==PlanNode.Type.UNION){
				stkop.push(var);
				getOps(((UnionNode)var).getLHS());
				getOps(((UnionNode)var).getRHS());
			}
		}
		return stkop;
	}
	public Stack<PlanNode> getLeaf(){

		return stkleaf;
	}

}

