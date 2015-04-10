package edu.buffalo.cse.sql;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;

import edu.buffalo.cse.sql.Schema.TableFromFile;
import edu.buffalo.cse.sql.buffer.BufferManager;
import edu.buffalo.cse.sql.buffer.FileManager;
import edu.buffalo.cse.sql.buffer.ManagedFile;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.index.ISAMIndex;

public class CheckIndex {
	public static void main(String[] args) throws Exception {
		File file = new File("index.dat");
		if (!file.exists()) {
			file.createNewFile();
		}
		Gen1 gen=new Gen1();
		BufferManager bm= new BufferManager(1024);
		FileManager fm= new FileManager(bm);
		Sort sort=new Sort(gen,new int[]{1},new File("hik"));
		sort.sort();
		ManagedFile mf=fm.open(file);
		Schema.Type[] var={Schema.Type.INT,Schema.Type.INT};
		ISAMIndex idx=new ISAMIndex(mf,null,var);
		idx.create(fm, file, sort, new int[] {1});
		Datum sss=new Datum.Int(3343);
		Datum arr[]={sss};
		Datum[] got=idx.get(arr);
		if(got!=null){
			System.out.println(Datum.stringOfRow(got));
		}
		else{
			System.out.println("ghanta!!! :-p");
		}
		Gen1 gen1= new Gen1();
		/*while(gen1.hasNext()){
			Datum d=gen1.next()[1];
			System.out.println("getting: "+d.toString());
			Datum[] darr= idx.get(new Datum[] {d});
			if(darr==null){ 
				throw new Exception("failed!!!");
				
			}
			else{
				System.out.println(Datum.stringOfRow(darr));
			}
		}*/
		/*Datum[] arr1={new Datum.Int(5543)};
		Iterator ob=idx.rangeScan(arr, arr1);
		while(ob.hasNext()){
			Datum[] pp=(Datum[]) ob.next();
			if(pp!=null){
				System.out.println(Datum.stringOfRow(pp));
			}
			else{
				System.err.println("debug!!!!");
			}
		}
		
		System.out.println("passed!!");*/
		
		
/*	
	Sort sort=new Sort(gen,new int[] {1},file);
	while(sort.hasNext()){
		Datum[] row=sort.next();
		System.out.println(Datum.stringOfRow(row));
	}
		System.out.println(sort.getChunks());
	
*/
	}
}

class Gen1 implements Iterator {
	TestIt a;
	
	public Gen1(){
		HashMap<String, TableFromFile> tables
	      = new HashMap<String, Schema.TableFromFile>();
	    Schema.TableFromFile table_R;
	    table_R = new Schema.TableFromFile(new File("test/s.dat"));
	    table_R.add(new Schema.Column("R", "A", Schema.Type.INT));
	    table_R.add(new Schema.Column("R", "B", Schema.Type.INT));
	   /* table_R.add(new Schema.Column("R", "C", Schema.Type.INT));
	    table_R.add(new Schema.Column("R", "D", Schema.Type.FLOAT));
	    table_R.add(new Schema.Column("R", "E", Schema.Type.STRING));*/
	    tables.put("R", table_R);
	    a=new TestIt(tables,"R");
	}

	@Override
	public boolean hasNext() {
		return a.hasNext();
	}

	@Override
	public Datum[] next() {
		return a.next();
	}

	@Override
	public void remove() {
		
	}
	
}
