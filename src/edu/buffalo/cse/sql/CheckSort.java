package edu.buffalo.cse.sql;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import edu.buffalo.cse.sql.Schema.TableFromFile;
import edu.buffalo.cse.sql.buffer.BufferException;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.InsufficientSpaceException;
import edu.buffalo.cse.sql.plan.ScanNode;

public class CheckSort {

	public static void main(String[] args) throws IOException, BufferException, InsufficientSpaceException {
		File file = new File("testsort1.txt");
		if (!file.exists()) {
			file.createNewFile();
		}
	Gen gen=new Gen();
	Sort sort=new Sort(gen,new int[] {0},file);
	sort.sort();
	int i=0;
	while(sort.hasNext()){
		System.out.println(Datum.stringOfRow(sort.next()));
		i++;
	}
	System.out.println(i);
	
	
	}
	
}

class Gen implements Iterator {
	TestIt a;
	int cnt=0;
	
	public Gen(){
		HashMap<String, TableFromFile> tables
	      = new HashMap<String, Schema.TableFromFile>();
	    Schema.TableFromFile table_R;
	    table_R = new Schema.TableFromFile(new File("t.dat"));
	    table_R.add(new Schema.Column("R", "A", Schema.Type.INT));
	    table_R.add(new Schema.Column("R", "B", Schema.Type.INT));
	   /* table_R.add(new Schema.Column("R", "C", Schema.Type.INT));
	    table_R.add(new Schema.Column("R", "C1", Schema.Type.INT));
	    table_R.add(new Schema.Column("R", "D", Schema.Type.FLOAT));
	    table_R.add(new Schema.Column("R", "D1", Schema.Type.FLOAT));
	    table_R.add(new Schema.Column("R", "D2", Schema.Type.FLOAT));
	    table_R.add(new Schema.Column("R", "D3", Schema.Type.FLOAT));
	    table_R.add(new Schema.Column("R", "E", Schema.Type.STRING));
	    table_R.add(new Schema.Column("R", "E1", Schema.Type.STRING));
	    table_R.add(new Schema.Column("R", "A1", Schema.Type.INT));
	    table_R.add(new Schema.Column("R", "B1", Schema.Type.INT));
	    table_R.add(new Schema.Column("R", "C2", Schema.Type.INT));
	    table_R.add(new Schema.Column("R", "E2", Schema.Type.STRING));
	    table_R.add(new Schema.Column("R", "E3", Schema.Type.STRING));
	    table_R.add(new Schema.Column("R", "E4", Schema.Type.STRING));*/
	    tables.put("R", table_R);
	    a=new TestIt(tables,"R");
	}

	@Override
	public boolean hasNext() {
		if(cnt==200){
			return false;
		}
		return a.hasNext();
	}

	@Override
	public Datum[] next() {
		cnt++;
		return a.next();
		
	}

	@Override
	public void remove() {
		
	}
	
}
