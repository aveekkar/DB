package edu.buffalo.cse.sql;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import edu.buffalo.cse.sql.Schema.TableFromFile;
import edu.buffalo.cse.sql.buffer.BufferManager;
import edu.buffalo.cse.sql.buffer.FileManager;
import edu.buffalo.cse.sql.buffer.ManagedFile;
import edu.buffalo.cse.sql.index.ISAMIndex;
import edu.buffalo.cse.sql.optimizer.FinalRewrite;

public class CreateIndex {
	HashMap<String,ISAMIndex> idxmap=new HashMap<String,ISAMIndex>();
	
	public void create(String table) throws Exception{
		if(Sql.tpch){

			ISAMIndex idx=null;
			if(table.equalsIgnoreCase("l")){
				HashMap<String, TableFromFile> tables
			      = new HashMap<String, Schema.TableFromFile>();
			    Schema.TableFromFile table_R;
			    table_R = new Schema.TableFromFile(new File("test/lineitem.tbl"));
			    table_R.add(new Schema.Column("l", "orderkey", Schema.Type.INT));
			    table_R.add(new Schema.Column("l", "partkey", Schema.Type.INT));
			    table_R.add(new Schema.Column("l", "suppkey", Schema.Type.INT));
			    table_R.add(new Schema.Column("l", "linenumber", Schema.Type.INT));
			    table_R.add(new Schema.Column("l", "quantity", Schema.Type.FLOAT));
			    table_R.add(new Schema.Column("l", "extendedprice", Schema.Type.FLOAT));
			    table_R.add(new Schema.Column("l", "discount", Schema.Type.FLOAT));
			    table_R.add(new Schema.Column("l", "tax", Schema.Type.FLOAT));
			    table_R.add(new Schema.Column("l", "returnflag", Schema.Type.STRING));
			    table_R.add(new Schema.Column("l", "linestatus", Schema.Type.STRING));
			    table_R.add(new Schema.Column("l", "shipdate", Schema.Type.INT));
			    table_R.add(new Schema.Column("l", "commitdate", Schema.Type.INT));
			    table_R.add(new Schema.Column("l", "receiptdate", Schema.Type.INT));
			    table_R.add(new Schema.Column("l", "shipinstruct", Schema.Type.STRING));
			    table_R.add(new Schema.Column("l", "shipmode", Schema.Type.STRING));
			    table_R.add(new Schema.Column("l", "comment", Schema.Type.STRING));
			    tables.put("l", table_R);
			    TestIt a=new TestIt(tables,"l");
			    File file=new File("l");
			    BufferManager bm= new BufferManager(10240);
				FileManager fm= new FileManager(bm);
				ManagedFile mf=fm.open(file);
			    Schema.Type[] schema={Schema.Type.INT,Schema.Type.INT,Schema.Type.INT,Schema.Type.INT
			    		,Schema.Type.FLOAT,Schema.Type.FLOAT,Schema.Type.FLOAT,Schema.Type.FLOAT,Schema.Type.STRING,Schema.Type.STRING
			    		,Schema.Type.INT,Schema.Type.INT,Schema.Type.INT,Schema.Type.STRING,Schema.Type.STRING,Schema.Type.STRING};
			    Sort sort=new Sort(a,new int[] {FinalRewrite.map.get("l")},new File("line"));
			    sort.sort();
			    idx=new ISAMIndex(mf,null,schema);
			    idx.create(fm, file, sort, new int[] {FinalRewrite.map.get("l")});
			    idxmap.put("l", idx);
			}
			else if(table.equalsIgnoreCase("p")){
				HashMap<String, TableFromFile> tables
			      = new HashMap<String, Schema.TableFromFile>();
			    Schema.TableFromFile table_R;
			    table_R = new Schema.TableFromFile(new File("test/part.tbl"));
			    table_R.add(new Schema.Column("p", "partkey", Schema.Type.INT));
			    table_R.add(new Schema.Column("p", "name", Schema.Type.STRING));
			    table_R.add(new Schema.Column("p", "mfgr", Schema.Type.STRING));
			    table_R.add(new Schema.Column("p", "brand", Schema.Type.STRING));
			    table_R.add(new Schema.Column("p", "type", Schema.Type.STRING));
			    table_R.add(new Schema.Column("p", "size", Schema.Type.INT));
			    table_R.add(new Schema.Column("p", "container", Schema.Type.STRING));
			    table_R.add(new Schema.Column("p", "retailprice", Schema.Type.FLOAT));
			    table_R.add(new Schema.Column("p", "comment", Schema.Type.STRING));
			    tables.put("p", table_R);
			    TestIt a=new TestIt(tables,"p");
			    File file=new File("p");
			    BufferManager bm= new BufferManager(10240);
				FileManager fm= new FileManager(bm);
				ManagedFile mf=fm.open(file);
				Schema.Type[] schema={Schema.Type.INT,Schema.Type.STRING,Schema.Type.STRING,Schema.Type.STRING
						,Schema.Type.STRING,Schema.Type.INT,Schema.Type.STRING,Schema.Type.FLOAT,Schema.Type.STRING};
				 Sort sort=new Sort(a,new int[] {FinalRewrite.map.get("p")},new File("par"));
				 sort.sort();
			    idx=new ISAMIndex(mf,null,schema);
			    idx.create(fm, file, sort, new int[] {FinalRewrite.map.get("p")});
			    idxmap.put("p", idx);
			}
			else if(table.equalsIgnoreCase("s")){
				HashMap<String, TableFromFile> tables
			      = new HashMap<String, Schema.TableFromFile>();
			    Schema.TableFromFile table_R;
			    table_R = new Schema.TableFromFile(new File("test/supplier.tbl"));
			    table_R.add(new Schema.Column("s", "suppkey", Schema.Type.INT));
			    table_R.add(new Schema.Column("s", "name", Schema.Type.STRING));
			    table_R.add(new Schema.Column("s", "address", Schema.Type.STRING));
			    table_R.add(new Schema.Column("s", "nationkey", Schema.Type.INT));
			    table_R.add(new Schema.Column("s", "phone", Schema.Type.STRING));
			    table_R.add(new Schema.Column("s", "acctbal", Schema.Type.FLOAT));
			    table_R.add(new Schema.Column("s", "comment", Schema.Type.STRING));
			    tables.put("s", table_R);
			    TestIt a=new TestIt(tables,"s");
			    File file=new File("s");
			    BufferManager bm= new BufferManager(10240);
				FileManager fm= new FileManager(bm);
				ManagedFile mf=fm.open(file);
				Schema.Type[] schema={Schema.Type.INT,Schema.Type.STRING,Schema.Type.STRING,Schema.Type.INT
						,Schema.Type.STRING,Schema.Type.FLOAT,Schema.Type.STRING};
				 Sort sort=new Sort(a,new int[] {FinalRewrite.map.get("s")},new File("supp"));
				 sort.sort();
			    idx=new ISAMIndex(mf,null,schema);
			    idx.create(fm, file, sort, new int[] {FinalRewrite.map.get("s")});
			    idxmap.put("s", idx);
			}
			else if(table.equalsIgnoreCase("c")){
				HashMap<String, TableFromFile> tables
			      = new HashMap<String, Schema.TableFromFile>();
			    Schema.TableFromFile table_R;
			    table_R = new Schema.TableFromFile(new File("test/customer.tbl"));
			    table_R.add(new Schema.Column("c", "custkey", Schema.Type.INT));
			    table_R.add(new Schema.Column("c", "name", Schema.Type.STRING));
			    table_R.add(new Schema.Column("c", "address", Schema.Type.STRING));
			    table_R.add(new Schema.Column("c", "nationkey", Schema.Type.INT));
			    table_R.add(new Schema.Column("c", "phone", Schema.Type.STRING));
			    table_R.add(new Schema.Column("c", "acctbal", Schema.Type.FLOAT));
			    table_R.add(new Schema.Column("c", "mktsegment", Schema.Type.STRING));
			    table_R.add(new Schema.Column("c", "comment", Schema.Type.STRING));
			    tables.put("c", table_R);
			    TestIt a=new TestIt(tables,"c");
			    File file=new File("c");
			    BufferManager bm= new BufferManager(10240);
				FileManager fm= new FileManager(bm);
				ManagedFile mf=fm.open(file);
				Schema.Type[] schema={Schema.Type.INT,Schema.Type.STRING,Schema.Type.STRING,Schema.Type.INT
						,Schema.Type.STRING,Schema.Type.FLOAT,Schema.Type.STRING,Schema.Type.STRING};
				 Sort sort=new Sort(a,new int[] {FinalRewrite.map.get("c")},new File("cust"));
				 sort.sort();
			    idx=new ISAMIndex(mf,null,schema);
			    idx.create(fm, file, sort, new int[] {FinalRewrite.map.get("c")});
			    idxmap.put("c", idx);
			}
			else if(table.equalsIgnoreCase("o")){
				HashMap<String, TableFromFile> tables
			      = new HashMap<String, Schema.TableFromFile>();
			    Schema.TableFromFile table_R;
			    table_R = new Schema.TableFromFile(new File("test/customer.tbl"));
			    table_R.add(new Schema.Column("o", "orderkey", Schema.Type.INT));
			    table_R.add(new Schema.Column("o", "custkey", Schema.Type.INT));
			    table_R.add(new Schema.Column("o", "orderstatus", Schema.Type.STRING));
			    table_R.add(new Schema.Column("o", "totalprice", Schema.Type.FLOAT));
			    table_R.add(new Schema.Column("o", "orderdate", Schema.Type.INT));
			    table_R.add(new Schema.Column("o", "orderpriority", Schema.Type.STRING));
			    table_R.add(new Schema.Column("o", "clerk", Schema.Type.STRING));
			    table_R.add(new Schema.Column("o", "shippriority", Schema.Type.INT));
			    table_R.add(new Schema.Column("o", "comment", Schema.Type.STRING));
			    tables.put("o", table_R);
			    TestIt a=new TestIt(tables,"o");
			    File file=new File("o");
			    BufferManager bm= new BufferManager(10240);
				FileManager fm= new FileManager(bm);
				ManagedFile mf=fm.open(file);
				Schema.Type[] schema={Schema.Type.INT,Schema.Type.INT,Schema.Type.STRING,Schema.Type.FLOAT
						,Schema.Type.INT,Schema.Type.STRING,Schema.Type.STRING,Schema.Type.INT,Schema.Type.STRING};
				 Sort sort=new Sort(a,new int[] {FinalRewrite.map.get("o")},new File("ordrs"));
				 sort.sort();
			    idx=new ISAMIndex(mf,null,schema);
			    idx.create(fm, file, sort, new int[] {FinalRewrite.map.get("o")});
			    idxmap.put("o", idx);
			}
			else if(table.equalsIgnoreCase("n")){
				HashMap<String, TableFromFile> tables
			      = new HashMap<String, Schema.TableFromFile>();
			    Schema.TableFromFile table_R;
			    table_R = new Schema.TableFromFile(new File("test/nation.tbl"));
			    table_R.add(new Schema.Column("n", "nationkey", Schema.Type.INT));
			    table_R.add(new Schema.Column("n", "name", Schema.Type.STRING));
			    table_R.add(new Schema.Column("n", "regionkey", Schema.Type.INT));
			    table_R.add(new Schema.Column("n", "comment", Schema.Type.STRING));
			    tables.put("n", table_R);
			    TestIt a=new TestIt(tables,"n");
			    File file=new File("n");
			    BufferManager bm= new BufferManager(10240);
				FileManager fm= new FileManager(bm);
				ManagedFile mf=fm.open(file);
				Schema.Type[] schema={Schema.Type.INT,Schema.Type.STRING,Schema.Type.INT,Schema.Type.STRING};
				 Sort sort=new Sort(a,new int[] {FinalRewrite.map.get("n")},new File("nat"));
				 sort.sort();
			    idx=new ISAMIndex(mf,null,schema);
			    idx.create(fm, file, sort, new int[] {FinalRewrite.map.get("n")});
			    idxmap.put("n", idx);
			}
			else{
				HashMap<String, TableFromFile> tables
			      = new HashMap<String, Schema.TableFromFile>();
			    Schema.TableFromFile table_R;
			    table_R = new Schema.TableFromFile(new File("test/region.tbl"));
			    table_R.add(new Schema.Column("r", "nationkey", Schema.Type.INT));
			    table_R.add(new Schema.Column("r", "name", Schema.Type.STRING));
			    table_R.add(new Schema.Column("r", "comment", Schema.Type.STRING));
			    tables.put("r", table_R);
			    TestIt a=new TestIt(tables,"r");
			    File file=new File("r");
			    BufferManager bm= new BufferManager(10240);
				FileManager fm= new FileManager(bm);
				ManagedFile mf=fm.open(file);
				Schema.Type[] schema={Schema.Type.INT,Schema.Type.STRING,Schema.Type.STRING};
				 Sort sort=new Sort(a,new int[] {FinalRewrite.map.get("r")},new File("regn"));
				 sort.sort();
			    idx=new ISAMIndex(mf,null,schema);
			    idx.create(fm, file, sort, new int[] {FinalRewrite.map.get("r")});
			    idxmap.put("r", idx);
			}
		
		}
		else{
			ISAMIndex idx=null;
			if(table.equalsIgnoreCase("r")){
				HashMap<String, TableFromFile> tables
			      = new HashMap<String, Schema.TableFromFile>();
			    Schema.TableFromFile table_R;
			    table_R = new Schema.TableFromFile(new File("test/r.dat"));
			    table_R.add(new Schema.Column("r", "a", Schema.Type.INT));
			    table_R.add(new Schema.Column("r", "b", Schema.Type.INT));
			    tables.put("r", table_R);
			    TestIt a=new TestIt(tables,"r");
			    File file=new File("rsq");
			    BufferManager bm= new BufferManager(1024);
				FileManager fm= new FileManager(bm);
				ManagedFile mf=fm.open(file);
				Schema.Type[] schema={Schema.Type.INT,Schema.Type.INT};
				 Sort sort=new Sort(a,new int[] {FinalRewrite.map.get("r")},new File("rsql"));
				 sort.sort();
			    idx=new ISAMIndex(mf,null,schema);
			    idx.create(fm, file, sort, new int[] {FinalRewrite.map.get("r")});
			    idxmap.put("r", idx);
			}
			else if(table.equalsIgnoreCase("s")){
				HashMap<String, TableFromFile> tables
			      = new HashMap<String, Schema.TableFromFile>();
			    Schema.TableFromFile table_R;
			    table_R = new Schema.TableFromFile(new File("test/s.dat"));
			    table_R.add(new Schema.Column("s", "b", Schema.Type.INT));
			    table_R.add(new Schema.Column("s", "c", Schema.Type.INT));
			    tables.put("s", table_R);
			    TestIt a=new TestIt(tables,"s");
			    File file=new File("ssq");
			    BufferManager bm= new BufferManager(1024);
				FileManager fm= new FileManager(bm);
				ManagedFile mf=fm.open(file);
				Schema.Type[] schema={Schema.Type.INT,Schema.Type.INT};
				 Sort sort=new Sort(a,new int[] {FinalRewrite.map.get("s")},new File("ssql"));
				 sort.sort();
			    idx=new ISAMIndex(mf,null,schema);
			    idx.create(fm, file, sort, new int[] {FinalRewrite.map.get("s")});
			    idxmap.put("s", idx);
			}
			else{
				HashMap<String, TableFromFile> tables
			      = new HashMap<String, Schema.TableFromFile>();
			    Schema.TableFromFile table_R;
			    table_R = new Schema.TableFromFile(new File("test/t.dat"));
			    table_R.add(new Schema.Column("t", "c", Schema.Type.INT));
			    table_R.add(new Schema.Column("t", "d", Schema.Type.INT));
			    tables.put("t", table_R);
			    TestIt a=new TestIt(tables,"t");
			    File file=new File("tsq");
			    BufferManager bm= new BufferManager(1024);
				FileManager fm= new FileManager(bm);
				ManagedFile mf=fm.open(file);
				Schema.Type[] schema={Schema.Type.INT,Schema.Type.INT};
				 Sort sort=new Sort(a,new int[] {FinalRewrite.map.get("t")},new File("tsql"));
				 sort.sort();
			    idx=new ISAMIndex(mf,null,schema);
			    idx.create(fm, file, sort, new int[] {FinalRewrite.map.get("t")});
			    idxmap.put("t", idx);
			}
		}
	}
	
	public HashMap<String,ISAMIndex> getIndex(){
		return idxmap;
	}
}
