package edu.buffalo.cse.sql;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

import edu.buffalo.cse.sql.buffer.BufferException;
import edu.buffalo.cse.sql.buffer.BufferManager;
import edu.buffalo.cse.sql.buffer.FileManager;
import edu.buffalo.cse.sql.buffer.ManagedFile;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.DatumBuffer;
import edu.buffalo.cse.sql.data.InsufficientSpaceException;
import edu.buffalo.cse.sql.data.Datum.CastError;

public class Sort implements Iterator {
	
	Iterator ob;
	int[] index;
	File file;
	ManagedFile sortfile;
	BufferManager bm;
	Schema.Type[] schema;
	ArrayList<Integer> stream;
	FileManager fm;
	int lastpage;
	int lastrow;
	int currpage;
	int currrow;
	int count;
	int numchunks=0;
	ArrayList<Integer> tmp;
	int tempstart=0;
	
	public Sort(Iterator ob,int[] index,File file) throws IOException, BufferException, InsufficientSpaceException{
		this.ob=ob;
		this.index=index;
		this.file=file;
		bm = new BufferManager(10240);
		fm = new FileManager(bm);
		sortfile=fm.open(file);
		sortfile.resize(10);
		ArrayList<Datum[]> dat=new ArrayList<Datum[]>();
		ArrayList<Integer> chunks=new ArrayList<Integer>();
		ob.hasNext();
		Datum[] datfirst=((Datum[]) ob.next());
		schema=new Schema.Type[datfirst.length];
		for(int i=0;i<datfirst.length;i++){
			schema[i]=datfirst[i].getType();
		}
		DatumBuffer datbuff=new DatumBuffer(sortfile.getBuffer(0),schema);
		datbuff.initialize(40);
		sortfile.getBuffer(0).putInt(0,1);
		sortfile.dirty(0);
		dat.add(datfirst);
		//System.out.println("size of first row: "+size1);
		//System.out.println("after first write remaining: "+datbuff.remaining());
		int count=-1;
		int rec;
		//System.out.println(Datum.stringOfRow(datfirst));
		while(ob.hasNext()){
			if(dat.isEmpty()||((dat.size())<3000)){
				dat.add((Datum[]) ob.next());
			}
			else{
				dat.add((Datum[]) ob.next());
				++count;
				datbuff=new DatumBuffer(sortfile.safeGetBuffer(count),schema);
				datbuff.initialize(40);
				sortfile.getBuffer(count).putInt(0,1);
				sortfile.dirty(count);
				chunks.add(count);
				++numchunks;
				Comp comp=new Comp(index,index.length);
				Collections.sort(dat,comp);
				for(Datum[] d:dat){

					Datum[] temp=d;
					//System.out.println(Datum.stringOfRow(temp));
					int size=0;
					for(int i=0;i<temp.length;i++){
						if(temp[i].getType()==Schema.Type.INT||temp[i].getType()==Schema.Type.FLOAT||temp[i].getType()==Schema.Type.DATE){
							size += 4;
						}
						else{
							String s=((Datum.Str)temp[i]).toString();
							s=s.substring(1,s.length()-1);
							size += s.getBytes().length+4;
							temp[i]=new Datum.Str(s);
						}
					}
					
					//System.out.println("size: "+size+" remaining: "+datbuff.remaining()+" page: "+count);
					
					if(datbuff.remaining()-8>size){
						rec=datbuff.write(temp);
						sortfile.dirty(count);
						sortfile.getBuffer(count).putInt(10, rec);
						sortfile.dirty(count);
					}
					else{
						sortfile.getBuffer(count).putInt(0, 0);
						sortfile.dirty(count);
						datbuff=new DatumBuffer(sortfile.safeGetBuffer(++count),schema);
						datbuff.initialize(40);
						sortfile.getBuffer(count).putInt(0, 1);
						rec=datbuff.write(temp);
						sortfile.dirty(count);
						sortfile.getBuffer(count).putInt(10, rec);
						sortfile.dirty(count);
					}
					
				
				}
				dat.clear();
			}
		}
		
		
		if(!dat.isEmpty()){
			Comp comp=new Comp(index,index.length);
			Collections.sort(dat,comp);
			++count;
			datbuff=new DatumBuffer(sortfile.getBuffer(count),schema);
			datbuff.initialize(40);
			sortfile.safeGetBuffer(count).putInt(0, 1);
			sortfile.dirty(count);
			++numchunks;
			chunks.add(count);
			
			for(Datum[] d:dat){
				Datum[] temp=d;
				//System.out.println(Datum.stringOfRow(temp));
				int size=0;
				for(int i=0;i<d.length;i++){
					if(temp[i].getType()==Schema.Type.INT||temp[i].getType()==Schema.Type.FLOAT||temp[i].getType()==Schema.Type.DATE){
						size += 4;
					}
					else{
						String s=((Datum.Str)temp[i]).toString();
						s=s.substring(1,s.length()-1);
						size += s.getBytes().length+4;
						temp[i]=new Datum.Str(s);
					}
				}
				
				//System.out.println("size: "+size+" remaining: "+datbuff.remaining()+" page: "+count);
				if(datbuff.remaining()-8>size){
					//System.out.println(count);
					rec=datbuff.write(temp);
					sortfile.dirty(count);
					sortfile.getBuffer(count).putInt(10, rec);
					sortfile.dirty(count);
				}
				else{
					sortfile.getBuffer(count).putInt(0, 0);
					sortfile.dirty(count);
					datbuff=new DatumBuffer(sortfile.safeGetBuffer(++count),schema);
					datbuff.initialize(40);
					sortfile.getBuffer(count).putInt(0, 1);
					rec=datbuff.write(temp);
					sortfile.dirty(count);
					sortfile.getBuffer(count).putInt(10, rec);
					sortfile.dirty(count);
				}
				
			
			}
		}
		
		
		this.count=count;
		lastpage=count;
		lastrow=sortfile.getBuffer(count).getInt(10);
		currpage=0;
		currrow=0;
		tmp=chunks;
		sortfile.flush();
		fm.close(file);
	}
	
	public void sort() throws IOException, BufferException, InsufficientSpaceException{
		sortfile=fm.open(file);
		int page=count+1;
		currpage=page;
		currrow=0;
		tempstart=page;
		DatumBuffer datbuff;
		stream=new ArrayList<Integer>();
		for(int i=0;i<numchunks;i++){
			stream.add(0);
		}
		Comp comp=new Comp(index,index.length);
		PriorityQueue<Datum[]> queue=new PriorityQueue<Datum[]>(numchunks,comp);
		for(int i=0;i<numchunks;i++){
			//System.out.println(tmp.get(i));
			datbuff=new DatumBuffer(sortfile.getBuffer(tmp.get(i)),schema);
			Datum[] temp=datbuff.read(0);
			queue.add(temp);
			//System.out.println("added init: "+Datum.stringOfRow(temp));
		}
		
		DatumBuffer newwrite;
		sortfile.safeGetBuffer(page).putInt(0,1);
		newwrite=new DatumBuffer(sortfile.getBuffer(page),schema);
		newwrite.initialize(40);
		sortfile.dirty(page);
		int search;
		while(!queue.isEmpty()){
			Datum[] temp=queue.poll();
			//System.out.println("polled: "+Datum.stringOfRow(temp));
			search=search(temp);
			//System.out.println("got search: "+search);
			int size=0;
			for(int i=0;i<temp.length;i++){
				if(temp[i].getType()==Schema.Type.INT||temp[i].getType()==Schema.Type.FLOAT||temp[i].getType()==Schema.Type.DATE){
					size += 4;
				}
				else{
					String s=((Datum.Str)temp[i]).toString();
					s=s.substring(1,s.length()-1);
					size += s.getBytes().length+4;
					temp[i]=new Datum.Str(s);
				}
			}
			int rec;
			if(newwrite.remaining()-8>size){
				rec=newwrite.write(temp);
				sortfile.safeGetBuffer(page).putInt(10, rec);
				sortfile.dirty(page);
			}
			else{
				sortfile.getBuffer(page).putInt(0, 0);
				sortfile.dirty(page);
				newwrite=new DatumBuffer(sortfile.safeGetBuffer(++page),schema);
				newwrite.initialize(40);
				sortfile.getBuffer(page).putInt(0, 1);
				rec=newwrite.write(temp);
				sortfile.dirty(page);
				sortfile.getBuffer(page).putInt(10, rec);
				sortfile.dirty(page);
			}
			
			if(stream.get(search)==sortfile.getBuffer(tmp.get(search)).getInt(10)){
				if(sortfile.getBuffer(tmp.get(search)).getInt(0)==1){
					stream.set(search,-1);
				}
				else{
					int gg=tmp.get(search)+1;
					tmp.set(search,gg);
					stream.set(search,0);
					datbuff=new DatumBuffer(sortfile.getBuffer(gg),schema);
					Datum[] hhh=datbuff.read(0);
					queue.add(hhh);
					//System.out.println("added nxt: "+Datum.stringOfRow(hhh));
				}
			}
			else{
				int gg=stream.get(search)+1;
				//System.out.println("row: "+gg+" page: "+tmp.get(search) );
				stream.set(search,gg);
				datbuff=new DatumBuffer(sortfile.getBuffer(tmp.get(search)),schema);
				Datum[] hhh=datbuff.read(gg);
				queue.add(hhh);
				//System.out.println("added bhak: "+Datum.stringOfRow(hhh));
			}
			
		}
		
		lastpage=page;
		count=page;
		lastrow=sortfile.getBuffer(page).getInt(10);
	}
	
	private int search(Datum[] dat) throws BufferException, IOException{
		int hold=-1;
		for(int i=0;i<numchunks;i++){
			if(stream.get(i)==-1){
				//System.out.println("hmm..");
				continue;
			}
			DatumBuffer datbuff=new DatumBuffer(sortfile.getBuffer(tmp.get(i)),schema);
			if(Datum.compareRows(dat,datbuff.read(stream.get(i)))==0){
				hold= i;
				break;
			}
		}
		return hold;
	}
	
	
	public ArrayList<Integer> getChunks(){
		return tmp;
	}
	
	
	

	@Override
	public boolean hasNext() {
		if(currpage>lastpage&&currrow==0){
			return false;
		}
		else{
			return true;
		}
	}

	@Override
	public Datum[] next() {
		try {
			//System.out.println("cur: "+currpage+" lst: "+lastpage);
			DatumBuffer datbuff=new DatumBuffer(sortfile.safeGetBuffer(currpage),schema);
			Datum[] ret=datbuff.read(currrow);
			++currrow;
			if(currrow>sortfile.safeGetBuffer(currpage).getInt(10)){
				currrow=0;
				++currpage;
			}
			return ret;
		} catch (BufferException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void remove() {
		System.err.println("unsupported operation");
	}
	
	public void reset() throws BufferException, IOException{
		lastpage=count;
		lastrow=sortfile.getBuffer(count).getInt(10);
		currpage=tempstart;
		currrow=0;
	}
	
	public void moveBack(int num){
		for(int i=0;i<num;i++){
			--currrow;
			if(currrow==-1){
				--currpage;
				try {
					currrow=sortfile.getBuffer(currpage).getInt(10);
				} catch (BufferException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		if(currpage<tempstart){
			currpage=tempstart;
			currrow=0;
		}
	}
	
	 public class Comp implements Comparator<Datum[]>{
		 	int[] arr;
		 	int size;
		 	public Comp(int arr[],int size){
		 		this.size=size;
		 		this.arr=new int[size];
		 		this.arr=arr;
		 	}

			@Override
			public int compare(Datum[] lhs, Datum[] rhs) {
				int comp=0;
				for(int i=0;i<size;i++){
					comp=comp(lhs[arr[i]],rhs[arr[i]]);
					if(comp!=0){
						break;
					}
				}
				return comp;
	
			}
			
			 public int comp(Datum left,Datum right)
			  {
			    if(left.equals(right)) return 0;
			    try {
			      switch(left.getType()){
			        case INT: 
			          switch(left.getType()){
			            case INT: return left.toInt() > right.toInt() ? 1 : -1;
			            case FLOAT: return left.toFloat() > right.toFloat() ? 1 : -1;
			            case STRING: return -1;
			            case BOOL: return -1;
			          }
			        case FLOAT:
			          switch(left.getType()){
			            case INT: 
			            case FLOAT: return left.toFloat() > right.toFloat() ? 1 : -1;
			            case STRING: return -1;
			            case BOOL: return -1;
			          }
			        case STRING: 
			          switch(left.getType()){
			            case INT: 
			            case FLOAT: 
			            case BOOL: return 1;
			            case STRING: return left.toString().compareTo(right.toString());
			          }
			        case BOOL:
			          switch(left.getType()){
			            case INT: 
			            case FLOAT: return 1;
			            case BOOL: return left.toBool() ? -1 : 1;
			            case STRING: return -1;
			          }        
			      } 
			    } catch (CastError e){}
			    return 0;
			  }
			 
		 }
	
}
