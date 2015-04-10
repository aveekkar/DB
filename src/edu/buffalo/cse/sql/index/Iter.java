package edu.buffalo.cse.sql.index;

import java.io.IOException;

import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.SqlException;
import edu.buffalo.cse.sql.buffer.BufferException;
import edu.buffalo.cse.sql.buffer.ManagedFile;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.DatumBuffer;

public class Iter implements IndexIterator {
	
	int startpage;
	int endpage;
	int startrow;
	int endrow;
	ManagedFile file;
	Schema.Type[] schema;
	int currpage;
	int currrow;
    int i;
	
	public Iter(ManagedFile file, int startpage,int startrow,int endpage,int endrow,Schema.Type[] schema){
		this.startpage=startpage;
		this.startrow=startrow;
		this.endpage=endpage;
		this.endrow=endrow;
		this.file=file;
		this.schema=schema;
		this.currpage=startpage;
		this.currrow=startrow;
	}

	@Override
	public boolean hasNext() {
		if((currpage==endpage+1&&currrow==endrow)||(currpage==endpage&&currrow==endrow+1)
				||(currpage==endpage+1&&currrow==0)){
			return false;
		}
		else{
			return true;
		}
		
	}

	@Override
	public Datum[] next()  {
		i++;
		//System.out.println("page: "+currpage+" row: "+currrow+" num records: "+i);
		Datum[] row=new Datum[schema.length];
		try {
			DatumBuffer datbuff=new DatumBuffer(file.getBuffer(currpage),schema);
			if(hasNext()){
				row=datbuff.read(currrow);
				if(file.getBuffer(currpage).getInt(10)==currrow){
					currpage++;
					currrow=0;
				}
				else{
					currrow++;
				}
				return row;
			}
			else{
				throw new Exception("Stream ended!!");
			}
			
		} catch (BufferException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			System.exit(-1);
		}
		return null;
	}

	@Override
	public void remove() {
		System.err.println("unsupported operation");
		
	}

	@Override
	public void close() throws SqlException {
		try {
			file.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
}
