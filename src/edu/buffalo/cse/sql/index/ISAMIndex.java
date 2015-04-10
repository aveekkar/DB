
package edu.buffalo.cse.sql.index;

import java.awt.image.DataBuffer;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;

import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.SqlException;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.Datum.CastError;
import edu.buffalo.cse.sql.data.DatumBuffer;
import edu.buffalo.cse.sql.data.DatumSerialization;
import edu.buffalo.cse.sql.data.ISAMDatum;
import edu.buffalo.cse.sql.data.InsufficientSpaceException;
import edu.buffalo.cse.sql.buffer.BufferManager;
import edu.buffalo.cse.sql.buffer.BufferException;
import edu.buffalo.cse.sql.buffer.ManagedFile;
import edu.buffalo.cse.sql.buffer.FileManager;
import edu.buffalo.cse.sql.test.TestDataStream;

public class ISAMIndex implements IndexFile,Iterator {
	
	Schema.Type[] schema;		//((TestDataStream)dataSource).getSchema();// for rowschema
	Schema.Type[] keyschema={Schema.Type.INT};
	int row;
	
	ManagedFile file;
	IndexKeySpec keyspec;
	boolean notfound=false;
	int iidx;

	public ISAMIndex(ManagedFile file, IndexKeySpec keySpec)
			throws IOException, SqlException
			{
		this.file=file;
		this.keyspec=keySpec;
			}
	public ISAMIndex(ManagedFile file, IndexKeySpec keySpec,Schema.Type[] schema)
			throws IOException, SqlException
			{
		this.file=file;
		this.schema=schema;
		this.keyspec=keySpec;
			}

	public static ISAMIndex create(FileManager fm,
			File path,
			Iterator<Datum[]> dataSource,
			IndexKeySpec key)
					throws Exception
					{

		System.out.println("please wait while index is being created!!");
		int rows=0;
		ManagedFile isamindex=fm.open(path);
		Schema.Type[] schema=((TestDataStream)dataSource).getSchema();
		Schema.Type[] keyschema=key.keySchema();
		isamindex.resize(1);
		int tempptr=0;
		DatumBuffer datbuff=new DatumBuffer(isamindex.getBuffer(0),schema);
		datbuff.initialize(40);
		File datfile=new File("data.txt");
		FileWriter fw = new FileWriter(datfile.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		while(dataSource.hasNext()){
			if(new DatumBuffer(isamindex.getBuffer(tempptr),schema).remaining()-8>schema.length*4){
				//System.out.println("writing on page: "+tempptr);
				datbuff=new DatumBuffer(isamindex.getBuffer(tempptr),schema);
				Datum[] temp=dataSource.next();
				int idx=datbuff.write(temp);
				bw.write(Datum.Int.stringOfRow(temp));bw.write(Integer.toString(tempptr));
				bw.newLine();
				rows++;
				isamindex.getBuffer(tempptr).putInt(5, 1);
				isamindex.getBuffer(tempptr).putInt(25, 0);
				isamindex.getBuffer(tempptr).putInt(0, -1);
				isamindex.getBuffer(tempptr).putInt(10, idx);
				isamindex.dirty(tempptr);
			}
			else{
				isamindex.getBuffer(tempptr).putInt(5, 0);
				isamindex.getBuffer(tempptr).putInt(0, -1);
				isamindex.dirty(tempptr);
				tempptr++;
				//System.out.println("writing on new page: "+tempptr);
				DatumBuffer dat=new DatumBuffer(isamindex.safeGetBuffer(tempptr),schema);
				dat.initialize(40);
				Datum[] temp=dataSource.next();
				int idx=dat.write(temp);
				bw.write(Datum.Int.stringOfRow(temp));bw.write(Integer.toString(tempptr));
				bw.newLine();
				rows++;
				isamindex.getBuffer(tempptr).putInt(5, 1);
				isamindex.getBuffer(tempptr).putInt(25, 0);
				isamindex.getBuffer(tempptr).putInt(10, idx);
				isamindex.dirty(tempptr);
			}
		}
		bw.close();
		//System.out.println("Done Writting pages. no. of pages written = "+(tempptr+1));
		//System.out.println("total rows written: "+rows);
		int indxptr=tempptr+2;
		int to=indxptr;
		isamindex.getBuffer(0).putInt(15, indxptr);
		isamindex.dirty(0);
		ISAMDatum isam=new ISAMDatum(isamindex.safeGetBuffer(indxptr),keyschema);
		isam.initialize(40);
		isamindex.safeGetBuffer(indxptr).putInt(20, 0);
		int count=0;
		boolean last=false;
		boolean boolhold=false;
		int hold=count;
		count++;
		int skip=1;


		while(last==false){
			//System.out.println("flag of page: "+isamindex.getBuffer(count).getInt(5));
			
			if((isamindex.getBuffer(hold).getInt(5)!=1)&&(boolhold==false)){
				//System.out.println("inside while if for page: "+hold);
				count=hold+skip;
				int max=isamindex.getBuffer(hold).getInt(10);
				DatumBuffer readbuff1=new DatumBuffer(isamindex.getBuffer(hold),schema);
				Datum[] temp1=key.createKey(readbuff1.read(max));

				DatumBuffer readbuff2=new DatumBuffer(isamindex.getBuffer(count),schema);
				Datum[] temp2=key.createKey(readbuff2.read(0));
				
				if(skip>1){
					max=isamindex.getBuffer(count-1).getInt(10);
					readbuff1=new DatumBuffer(isamindex.getBuffer(count-1),schema);
					temp1=key.createKey(readbuff1.read(max));
				}

				if(Datum.compareRows(temp1, temp2)==0){
					//System.out.println("page on hold: "+hold);
					isamindex.getBuffer(hold).putInt(20, 1);
					isamindex.dirty(hold);
					if((isamindex.getBuffer(count).getInt(5)==1)){
						boolhold=true;
					}
					//System.out.println("skipping page: "+count);
					isamindex.getBuffer(count).putInt(25, 1);
					isamindex.getBuffer(count).putInt(30, 1);
					isamindex.getBuffer(count-1).putInt(30, 0);
					isamindex.dirty(count);
					isamindex.getBuffer(hold+skip-1).putInt(0, count);
					isamindex.dirty(count);
					isamindex.dirty(count-1);
					isamindex.dirty(hold+skip-1);
					skip++;
					continue;
				}
				else{
					if(skip>1){
						max=isamindex.getBuffer(count-1).getInt(10);
						readbuff1=new DatumBuffer(isamindex.getBuffer(count-1),schema);
						temp1=key.createKey(readbuff1.read(max));
					}
					Datum[] mean=new Datum[keyschema.length];
					for(int i=0;i<keyschema.length;i++){
						mean[i]=new Datum.Flt((float)(temp1[i].toInt()+temp2[i].toInt())/2);
					}
					//System.out.println("mean"+Datum.stringOfRow(mean));
					//System.out.println("space free= "+isam.remaining());
					if(keyschema.length*4+20<=isam.remaining()){
						if(isam.getNumRows()==0){
							isamindex.safeGetBuffer(indxptr).putInt(20,hold);
						}
						isamindex.getBuffer(indxptr).putInt(0, 1);
						isamindex.getBuffer(indxptr).putInt(5, 1);
						//System.out.println("writing index on page: "+indxptr);
						isam.createPointer(hold);
						isam.putKey(mean);
						isamindex.dirty(indxptr);
					}
					else{
						isam.createPointer(hold);
						if(skip>1){
							isamindex.safeGetBuffer(indxptr).putInt(25,count-1);
						}
						else{
							isamindex.safeGetBuffer(indxptr).putInt(25,hold);
						}
						//isam.showPage();
						isamindex.getBuffer(indxptr).putInt(0, 0);
						isamindex.dirty(indxptr);
						indxptr++;
						isam=new ISAMDatum(isamindex.safeGetBuffer(indxptr),keyschema);
						isam.initialize(40);
						//System.out.println("created idx page: "+indxptr);
						isamindex.getBuffer(indxptr).putInt(0, 1);
						isamindex.getBuffer(indxptr).putInt(5, 1);
						isamindex.dirty(indxptr);
					}
					skip=1;
					hold=count;
					//System.out.println("next page: "+hold);
				}}
				else{
					isam.createPointer(hold);
					isamindex.safeGetBuffer(indxptr).putInt(25,to-2);
					last=true;
				}
			//System.out.println("hold: "+hold+"count: "+count);
			
		}
	
		//isam.showPage();
		
		
		
		int from=indxptr;
		int breadth=from-to+1;
		int level=2;
		
		//System.out.println("before layering up index: "+"to: "+to+" from: "+from);
		
		while(breadth!=1){
			isam=new ISAMDatum(isamindex.getBuffer(to),keyschema);
			ISAMDatum newisam=new ISAMDatum(isamindex.safeGetBuffer(from+1),keyschema);
			newisam.initialize(40);
			isamindex.safeGetBuffer(from+1).putInt(20, isamindex.safeGetBuffer(to).getInt(20));
			for(int i=0;i<breadth;i++){
				//System.out.println("index flag at 0: "+isamindex.getBuffer(to).getInt(0)
									//+" for page: "+to);
				if(isamindex.getBuffer(to).getInt(0) !=1){
					
					Datum[] max=new Datum[keyschema.length];
					int page1=isamindex.getBuffer(to).getInt(25);
					DatumBuffer bufmax=new DatumBuffer(isamindex.getBuffer(page1),keyschema);
					max=bufmax.read(isamindex.getBuffer(page1).getInt(10));
					isam=new ISAMDatum(isamindex.getBuffer(to+1),keyschema);
					Datum[] min=new Datum[keyschema.length];
					int page2=isamindex.getBuffer(to+1).getInt(20);
					DatumBuffer bufmin=new DatumBuffer(isamindex.getBuffer(page2),keyschema);
					min=bufmin.read(0);
					
					Datum[] mean=new Datum[keyschema.length];
					for(int l=0;l<keyschema.length;l++){
						mean[l]=new Datum.Flt((max[l].toFloat()+min[l].toFloat())/2);
					}
					
					
					if(keyschema.length*4+20<=newisam.remaining()){
						if(newisam.getNumRows()==0){
							isamindex.safeGetBuffer(from+1).putInt(20, isamindex.safeGetBuffer(to).getInt(20));
						}
						isamindex.getBuffer(from+1).putInt(0, 1);
						isamindex.getBuffer(from+1).putInt(5, level);
						//System.out.println("writing index on page: "+(from+1)+" for index at: "+to);
						newisam.createPointer(to);
						newisam.putKey(mean);
						isamindex.dirty(from+1);
						to++;
					}
					else{
						//System.out.println("writing last ptr on page: "+(from+1)+" for index at: "+to);
						newisam.createPointer(to);
						isamindex.safeGetBuffer(from+1).putInt(25, isamindex.safeGetBuffer(to).getInt(25));
						//newisam.showPage();
						isamindex.getBuffer(from+1).putInt(0, 0);
						isamindex.dirty(from+1);
						from++;
						to++;
						newisam=new ISAMDatum(isamindex.safeGetBuffer(from+1),keyschema);
						newisam.initialize(40);
						//System.out.println("created idx page: "+(from+1));
						isamindex.getBuffer(from+1).putInt(0, 1);
						isamindex.getBuffer(from+1).putInt(5, level);
						isamindex.dirty(from+1);
					}
				}
				else{
					//System.out.println("writing closing ptr at: "+(from+1)+" for index at: "+to);
					newisam.createPointer(to);
					isamindex.safeGetBuffer(from+1).putInt(25, isamindex.safeGetBuffer(to).getInt(25));
					isamindex.dirty(from+1);
					isamindex.dirty(to);
					to++;
				}
			}
			level++;
			from++;
			//System.out.println("to: "+to+" from: "+from+" for level"+(level-1));
			breadth=from-to+1;
			//System.out.println("breadth: "+breadth);
			//newisam.showPage();
		}
		isamindex.getBuffer(0).putInt(35, from);
		isamindex.dirty(0);
		ISAMDatum whatever=new ISAMDatum(isamindex.getBuffer(from),keyschema);
		//whatever.showPage();
		System.out.println("index created successfuly!! ");
		fm.close(path);
		return null;
					}

	public IndexIterator scan() 
			throws SqlException, IOException
			{
		int endpage=file.getBuffer(0).getInt(15)-2;
		int endrow=file.getBuffer(endpage).getInt(10);
		//System.out.println("endpage: "+endpage+" endrow: "+endrow);
		Iter iter=new Iter(file,0,0,endpage,endrow,schema);
		return iter;
			}

	public IndexIterator rangeScanTo(Datum[] toKey)
			throws SqlException, IOException
			{
		
		int[] pageandnum=getPageNum(toKey);
		int endpage=pageandnum[0];
		int endnum=pageandnum[1];
		int endrow=endnum;
		int page=endpage;
		if(pageandnum[2]==1){
			if(endrow!=0){
				endrow=endrow-1;
			}
			else{
				page--;
				endrow=file.getBuffer(page).getInt(10);
			}
		}
		else{
			while(true){
				if(endpage==file.getBuffer(0).getInt(15)-2){
					if(endnum==file.getBuffer(endpage).getInt(10)){
						break;
					}
					else{
						endnum++;
					}
				}
				else{
					if(endnum==file.getBuffer(endpage).getInt(10)){
						endpage++;
						endnum=0;
					}
					else{
						endnum++;
					}
				}
				
				DatumBuffer datfind=new DatumBuffer(file.getBuffer(endpage),schema);
				if(toKey[0].compareTo(datfind.read(endnum)[iidx])!=0){
					break;
				}
				else{
					endrow=endnum;
					page=endpage;
				}
			}
		}
		
	//	DatumBuffer fff=new DatumBuffer(file.getBuffer(167),keyspec.keySchema());
	//	System.out.println("test dat/////////"+Datum.stringOfRow(fff.read(23)));
		
		Iter iter=new Iter(file,0,0,page,endrow,schema);
		return iter;
			}

	public IndexIterator rangeScanFrom(Datum[] fromKey)
			throws SqlException, IOException
			{
		int[] pageandnum=getPageNum(fromKey);
		int pagenum=pageandnum[0];
		int row=pageandnum[1];
		int endpage=file.getBuffer(0).getInt(15)-2;
		int endrow=file.getBuffer(endpage).getInt(10);
		int startrow=row;
		int startpage=pagenum;
		if(row==-1){
			
		}
		else{
			while(true){
				if(pagenum==0){
					if(row==0){
						break;
					}
					else{
						row--;
					}
				}
				else{
					if(row==0){
						pagenum--;
						row=file.getBuffer(pagenum).getInt(10);
					}
					else{
						row--;
					}
				}
				
				DatumBuffer datfind=new DatumBuffer(file.getBuffer(pagenum),schema);
				if(fromKey[0].compareTo(datfind.read(row)[iidx])!=0){
					break;
				}
				else{
					startrow=row;
					startpage=pagenum;
				}
			}
		}
		// new code ends here
		Iter iter=new Iter(file,startpage,startrow,endpage,endrow,schema);
		return iter;
			}

	public IndexIterator rangeScan(Datum[] start, Datum[] end)
			throws SqlException, IOException
			{
		
		int[] pageandnum=getPageNum(start);
		int pagenum=pageandnum[0];
		int row=pageandnum[1];
		int startrow=row;
		int startpage=pagenum;
		if(row==-1){
			
		}
		else{
			while(true){
				if(pagenum==0){
					if(row==0){
						break;
					}
					else{
						row--;
					}
				}
				else{
					if(row==0){
						pagenum--;
						row=file.getBuffer(pagenum).getInt(10);
					}
					else{
						row--;
					}
				}
				
				DatumBuffer datfind=new DatumBuffer(file.getBuffer(pagenum),schema);
				if(start[0].compareTo(datfind.read(row)[iidx])!=0){
					break;
				}
				else{
					startrow=row;
					startpage=pagenum;
				}
			}
		}
		
		int[] pageandnum1=getPageNum(end);
		int endpage=pageandnum1[0];
		int endnum=pageandnum1[1];
		int endrow=endnum;
		int page=endpage;
		if(pageandnum1[2]==1){
			if(endrow!=0){
				endrow=endrow-1;
			}
			else{
				page--;
				endrow=file.getBuffer(page).getInt(10);
			}
		}
		else{
			while(true){
				if(endpage==file.getBuffer(0).getInt(15)-2){
					if(endnum==file.getBuffer(endpage).getInt(10)){
						break;
					}
					else{
						endnum++;
					}
				}
				else{
					if(endnum==file.getBuffer(endpage).getInt(10)){
						endpage++;
						endnum=0;
					}
					else{
						endnum++;
					}
				}
				
				DatumBuffer datfind=new DatumBuffer(file.getBuffer(endpage),schema);
				if(end[0].compareTo(datfind.read(endnum)[iidx])!=0){
					break;
				}
				else{
					endrow=endnum;
					page=endpage;
				}
			}
		}
		
		Iter iter =new Iter(file,startpage,startrow,page,endrow,schema);
		return iter;
			}

	public Datum[] get(Datum[] key)
			throws SqlException, IOException
			{
		
		int page=getPage(key);
		//System.out.println("found page: "+page);
		Datum[] ret=null;
		DatumBuffer find=new DatumBuffer(file.getBuffer(page),schema);
		DatumBuffer find1=new DatumBuffer(file.getBuffer(page),schema);
		//System.out.println(file.getBuffer(page).getInt(10)+" = high");
		int found=find(file.getBuffer(page),key,file.getBuffer(page).getInt(10),0);
		//System.out.println("found: "+found);
		//System.out.println("skip flag on page: "+page+" is "+file.getBuffer(page).getInt(20));
		if(found!=-1&&key[0].compareTo(find.read(found)[iidx])==0){
			ret=find1.read(found);
		}
		else{
			if(file.getBuffer(page).getInt(20)==0){
				ret=null;
			}
			else{
				//System.out.println("last flag on page: "+page+" is "+file.getBuffer(page).getInt(30));
				while(file.getBuffer(page).getInt(30)!=1){
					page=file.getBuffer(page).getInt(0);
					//System.out.println("finding for page: "+page);
					find=new DatumBuffer(file.getBuffer(page),schema);
					find1=new DatumBuffer(file.getBuffer(page),schema);
					found=find(file.getBuffer(page),key,file.getBuffer(page).getInt(10),0);
					if(found!=-1&&key[0].compareTo(find.read(found)[iidx])==0){
						ret=find1.read(found);
						break;
					}
				}
			}
		}
		
		
		return ret;
			}
	private int getPage(Datum[] key) throws BufferException, IOException, CastError{
		Datum[] temp=new Datum[key.length];
		for(int i=0;i<key.length;i++){
			temp[i]=new Datum.Flt((float)key[i].toInt());
		}
		key=temp;
		int root=file.getBuffer(0).getInt(35);
		//System.out.println("root: "+root);
		int numpages=file.getBuffer(0).getInt(15) - 1;
		//System.out.println("number of data pages: "+numpages+" fileSize = "+file.size());
		int page=-1;
		int level=file.getBuffer(root).getInt(5);
		//System.out.println(level);
		ByteBuffer buff=file.getBuffer(root);
		ISAMDatum isam=new ISAMDatum(buff,keyschema);
		
		/*DatumBuffer d=new DatumBuffer(file.getBuffer(430),keyspec.keySchema());
		for(int i=0;i<22;i++){
		Datum[] a=d.read(i);
		System.out.println("choose keys from: "+Datum.Int.stringOfRow(a));}*/
		
		for(int i=0;i<level;i++){
			isam=new ISAMDatum(file.getBuffer(root),keyschema);
			//isam.showPage();
			root=isam.pointer(key);
			
		}
		/*isam=new ISAMDatum(file.getBuffer(4548),keyspec.keySchema());
		isam.showPage();*/
		page=root;
		return page;
	}
	
	protected int find(ByteBuffer dat,Datum[] key,int high,int low){
		DatumBuffer datbuff=new DatumBuffer(dat,schema);
		int mid=(high+low)/2;
		if(high-low==1||high-low==0){
			//System.out.println("high: "+high+" low: "+low);
			System.out.println();
			if(key[0].compareTo(datbuff.read(low)[iidx])==0){
				return low;
			}
			else if(key[0].compareTo(datbuff.read(high)[iidx])==0){
				return high;
			}
			else{
				return -1;
			}
		}
		else if(key[0].compareTo(datbuff.read(mid)[iidx])==0){
			//System.out.println(mid);
			return mid;
		}
		else if(key[0].compareTo(datbuff.read(mid)[iidx])==1){
			//System.out.println(mid);
			low=mid+1;
			return find(dat,key,high,low);
		}
		else{
			//System.out.println(mid);
			high=mid-1;
			return find(dat,key,high,low);
		}
       
	}
	
	private int[] getPageNum(Datum[] key) throws BufferException, IOException, CastError{
		int[] toRet=new int[3];
		toRet[2]=0;
		int page=getPage(key);
		//System.out.println("@ getpagenum found page: "+page);
		Datum[] ret=null;
		DatumBuffer find=new DatumBuffer(file.getBuffer(page),schema);
		DatumBuffer find1=new DatumBuffer(file.getBuffer(page),schema);
		//System.out.println(file.getBuffer(page).getInt(10)+" = high");
		int found=find(file.getBuffer(page),key,file.getBuffer(page).getInt(10),0);
		//System.out.println("@ getpagenum found: "+found);
		if(found==-1){
			if(file.getBuffer(page).getInt(20)==0){
				found=-1;
				//page=-1;
			}
			else{
				//System.out.println("last flag on page: "+page+" is "+file.getBuffer(page).getInt(30));
				while(file.getBuffer(page).getInt(30)!=1){
					page=file.getBuffer(page).getInt(0);
					//System.out.println("finding for page: "+page);
					find=new DatumBuffer(file.getBuffer(page),schema);
					find1=new DatumBuffer(file.getBuffer(page),schema);
					found=find(file.getBuffer(page),key,file.getBuffer(page).getInt(10),0);
					if(key[0].compareTo(find.read(found)[iidx])==0){
						ret=find1.read(found);
						break;
					}
				}
			}
		}
		if(found==-1){
			int i=0;
			page=getPage(key);
			toRet[2]=1;
			while(true){
				DatumBuffer datbuff=new DatumBuffer(file.getBuffer(page),schema);
				if(datbuff.read(i)[iidx].compareTo(key[0])>0){
					toRet[0]=page;
					toRet[1]=i;
					return toRet;
				}
				else{
					if(page==file.getBuffer(0).getInt(15)-2){
						if(i!=file.getBuffer(page).getInt(10)){
							i++;
						}
						else{
							toRet[0]=page;
							toRet[1]=i;
							return toRet;
						}
					}
					else{
						if(i!=file.getBuffer(page).getInt(10)){
							i++;
						}
						else{
							page++;
							i=0;
						}
					}
				}
			}
		}
		//System.out.println("test "+(find.read(found)==0));
		//System.out.println("skip flag on page: "+page+" is "+file.getBuffer(page).getInt(20));
		
		//System.out.println("found page: "+page);
		//System.out.println("found row: "+found);
		toRet[0]=page;
		toRet[1]=found;
		return toRet;
	}
	
	
	public ISAMIndex create(FileManager fm,
			File path,
			Iterator<Datum[]> dataSource,
			int[] callkey
			//IndexKeySpec key
			)
					throws Exception
					{
		this.iidx=callkey[0];
		
		System.out.println("please wait while index is being created!!");
		int rows=0;
		ManagedFile isamindex=fm.open(path);	
		isamindex.resize(1);
		int tempptr=0;
		DatumBuffer datbuff=new DatumBuffer(isamindex.getBuffer(0),schema);
		datbuff.initialize(40);
		File datfile=new File("data.txt");
		FileWriter fw = new FileWriter(datfile.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		while(dataSource.hasNext()){
			Datum[] temp=dataSource.next();
			
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
			if(new DatumBuffer(isamindex.getBuffer(tempptr),schema).remaining()-8>size){
				//System.out.println("writing on page: "+tempptr);
				datbuff=new DatumBuffer(isamindex.getBuffer(tempptr),schema);
				int idx=datbuff.write(temp);
				bw.write(Datum.stringOfRow(temp));bw.write(Integer.toString(tempptr));
				bw.newLine();
				rows++;
				isamindex.getBuffer(tempptr).putInt(5, 1);
				isamindex.getBuffer(tempptr).putInt(25, 0);
				isamindex.getBuffer(tempptr).putInt(0, -1);
				isamindex.getBuffer(tempptr).putInt(10, idx);
				isamindex.dirty(tempptr);
			}
			else{
				isamindex.getBuffer(tempptr).putInt(5, 0);
				isamindex.getBuffer(tempptr).putInt(0, -1);
				isamindex.dirty(tempptr);
				tempptr++;
				//System.out.println("writing on new page: "+tempptr);
				DatumBuffer dat=new DatumBuffer(isamindex.safeGetBuffer(tempptr),schema);
				dat.initialize(40);
				int idx=dat.write(temp);
				bw.write(Datum.stringOfRow(temp));bw.write(Integer.toString(tempptr));
				bw.newLine();
				rows++;
				isamindex.getBuffer(tempptr).putInt(5, 1);
				isamindex.getBuffer(tempptr).putInt(25, 0);
				isamindex.getBuffer(tempptr).putInt(10, idx);
				isamindex.dirty(tempptr);
			}
		}
		bw.close();
		//System.out.println("Done Writting pages. no. of pages written = "+(tempptr+1));
		//System.out.println("total rows written: "+rows);
		int indxptr=tempptr+2;
		int to=indxptr;
		isamindex.getBuffer(0).putInt(15, indxptr);
		isamindex.dirty(0);
		ISAMDatum isam=new ISAMDatum(isamindex.safeGetBuffer(indxptr),keyschema);
		isam.initialize(40);
		isamindex.safeGetBuffer(indxptr).putInt(20, 0);
		int count=0;
		boolean last=false;
		boolean boolhold=false;
		int hold=count;
		count++;
		int skip=1;


		while(last==false){
			//System.out.println("flag of page: "+isamindex.getBuffer(count).getInt(5));
			
			if((isamindex.getBuffer(hold).getInt(5)!=1)&&(boolhold==false)){
				//System.out.println("inside while if for page: "+hold);
				count=hold+skip;
				int max=isamindex.getBuffer(hold).getInt(10);
				DatumBuffer readbuff1=new DatumBuffer(isamindex.getBuffer(hold),schema);
				Datum[] temp1=new Datum[callkey.length];
				Datum[] darray=readbuff1.read(max);
				for(int z=0;z<callkey.length;z++){
						temp1[z]= darray[callkey[z]];
				}

				DatumBuffer readbuff2=new DatumBuffer(isamindex.getBuffer(count),schema);
				Datum[] temp2=new Datum[callkey.length];
				Datum[] darray2=readbuff2.read(0);
				for(int z=0;z<callkey.length;z++){
						temp2[z]= darray2[callkey[z]];
				}
				
				if(skip>1){
					max=isamindex.getBuffer(count-1).getInt(10);
					readbuff1=new DatumBuffer(isamindex.getBuffer(count-1),schema);
					darray=readbuff1.read(max);
					for(int z=0;z<callkey.length;z++){
							temp1[z]= darray[callkey[z]];
					}
				}

				if(Datum.compareRows(temp1, temp2)==0){
					//System.out.println("page on hold: "+hold);
					isamindex.getBuffer(hold).putInt(20, 1);
					isamindex.dirty(hold);
					if((isamindex.getBuffer(count).getInt(5)==1)){
						boolhold=true;
					}
					//System.out.println("skipping page: "+count);
					isamindex.getBuffer(count).putInt(25, 1);
					isamindex.getBuffer(count).putInt(30, 1);
					isamindex.getBuffer(count-1).putInt(30, 0);
					isamindex.dirty(count);
					isamindex.getBuffer(hold+skip-1).putInt(0, count);
					isamindex.dirty(count);
					isamindex.dirty(count-1);
					isamindex.dirty(hold+skip-1);
					skip++;
					continue;
				}
				else{
					if(skip>1){
						max=isamindex.getBuffer(count-1).getInt(10);
						readbuff1=new DatumBuffer(isamindex.getBuffer(count-1),schema);
						darray=readbuff1.read(max);
						for(int z=0;z<callkey.length;z++){
								temp1[z]= darray[callkey[z]];
						}
					}
					Datum[] mean=new Datum[keyschema.length];
					for(int i=0;i<keyschema.length;i++){
						mean[i]=new Datum.Flt((float)(temp1[i].toInt()+temp2[i].toInt())/2);
					}
					//System.out.println("mean"+Datum.stringOfRow(mean));
					//System.out.println("space free= "+isam.remaining());
					if(keyschema.length*4+20<=isam.remaining()){
						if(isam.getNumRows()==0){
							isamindex.safeGetBuffer(indxptr).putInt(20,hold);
						}
						isamindex.getBuffer(indxptr).putInt(0, 1);
						isamindex.getBuffer(indxptr).putInt(5, 1);
						//System.out.println("writing index on page: "+indxptr);
						isam.createPointer(hold);
						isam.putKey(mean);
						isamindex.dirty(indxptr);
					}
					else{
						isam.createPointer(hold);
						if(skip>1){
							isamindex.safeGetBuffer(indxptr).putInt(25,count-1);
						}
						else{
							isamindex.safeGetBuffer(indxptr).putInt(25,hold);
						}
					    //isam.showPage();
						isamindex.getBuffer(indxptr).putInt(0, 0);
						isamindex.dirty(indxptr);
						indxptr++;
						isam=new ISAMDatum(isamindex.safeGetBuffer(indxptr),keyschema);
						isam.initialize(40);
						//System.out.println("created idx page: "+indxptr);
						isamindex.getBuffer(indxptr).putInt(0, 1);
						isamindex.getBuffer(indxptr).putInt(5, 1);
						isamindex.dirty(indxptr);
					}
					skip=1;
					hold=count;
					//System.out.println("next page: "+hold);
				}}
				else{
					isam.createPointer(hold);
					isamindex.safeGetBuffer(indxptr).putInt(25,to-2);
					last=true;
				}
			//System.out.println("hold: "+hold+"count: "+count);
			
		}
	
		//isam.showPage();
		
		
		
		int from=indxptr;
		int breadth=from-to+1;
		int level=2;
		
		//System.out.println("before layering up index: "+"to: "+to+" from: "+from);
		
		while(breadth!=1){
			isam=new ISAMDatum(isamindex.getBuffer(to),keyschema);
			ISAMDatum newisam=new ISAMDatum(isamindex.safeGetBuffer(from+1),keyschema);
			newisam.initialize(40);
			isamindex.safeGetBuffer(from+1).putInt(20, isamindex.safeGetBuffer(to).getInt(20));
			for(int i=0;i<breadth;i++){
				//System.out.println("index flag at 0: "+isamindex.getBuffer(to).getInt(0)
									//+" for page: "+to);
				if(isamindex.getBuffer(to).getInt(0) !=1){
					
					Datum[] max=new Datum[keyschema.length];
					int page1=isamindex.getBuffer(to).getInt(25);
					DatumBuffer bufmax=new DatumBuffer(isamindex.getBuffer(page1),schema);
					max[0]=bufmax.read(isamindex.getBuffer(page1).getInt(10))[iidx];
					isam=new ISAMDatum(isamindex.getBuffer(to+1),keyschema);
					Datum[] min=new Datum[keyschema.length];
					int page2=isamindex.getBuffer(to+1).getInt(20);
					DatumBuffer bufmin=new DatumBuffer(isamindex.getBuffer(page2),schema);
					min[0]=bufmin.read(0)[iidx];
					
					Datum[] mean=new Datum[keyschema.length];
					for(int l=0;l<keyschema.length;l++){
						mean[l]=new Datum.Flt((max[l].toFloat()+min[l].toFloat())/2);
					}
					
					
					if(keyschema.length*4+20<=newisam.remaining()){
						if(newisam.getNumRows()==0){
							isamindex.safeGetBuffer(from+1).putInt(20, isamindex.safeGetBuffer(to).getInt(20));
						}
						isamindex.getBuffer(from+1).putInt(0, 1);
						isamindex.getBuffer(from+1).putInt(5, level);
						//System.out.println("writing index on page: "+(from+1)+" for index at: "+to);
						newisam.createPointer(to);
						newisam.putKey(mean);
						isamindex.dirty(from+1);
						to++;
					}
					else{
						//System.out.println("writing last ptr on page: "+(from+1)+" for index at: "+to);
						newisam.createPointer(to);
						isamindex.safeGetBuffer(from+1).putInt(25, isamindex.safeGetBuffer(to).getInt(25));
						//newisam.showPage();
						isamindex.getBuffer(from+1).putInt(0, 0);
						isamindex.dirty(from+1);
						from++;
						to++;
						newisam=new ISAMDatum(isamindex.safeGetBuffer(from+1),keyschema);
						newisam.initialize(40);
						//System.out.println("created idx page: "+(from+1));
						isamindex.getBuffer(from+1).putInt(0, 1);
						isamindex.getBuffer(from+1).putInt(5, level);
						isamindex.dirty(from+1);
					}
				}
				else{
					//System.out.println("writing closing ptr at: "+(from+1)+" for index at: "+to);
					newisam.createPointer(to);
					isamindex.safeGetBuffer(from+1).putInt(25, isamindex.safeGetBuffer(to).getInt(25));
					isamindex.dirty(from+1);
					isamindex.dirty(to);
					to++;
				}
			}
			level++;
			from++;
			//System.out.println("to: "+to+" from: "+from+" for level"+(level-1));
			breadth=from-to+1;
			//System.out.println("breadth: "+breadth);
			//newisam.showPage();
		}
		isamindex.getBuffer(0).putInt(35, from);
		isamindex.dirty(0);
		ISAMDatum whatever=new ISAMDatum(isamindex.getBuffer(from),keyschema);
		//whatever.showPage();
		System.out.println("index created successfuly!! ");
		fm.close(path);
		return null;
		
					}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Object next() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void remove() {
		// TODO Auto-generated method stub
		
	}

}