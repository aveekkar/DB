
package edu.buffalo.cse.sql.index;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.spec.KeySpec;
import java.util.ArrayList;
import java.util.Iterator;

import edu.buffalo.cse.sql.Index;
import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.SqlException;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.DatumBuffer;
import edu.buffalo.cse.sql.data.DatumSerialization;
import edu.buffalo.cse.sql.data.InsufficientSpaceException;
import edu.buffalo.cse.sql.buffer.BufferManager;
import edu.buffalo.cse.sql.buffer.BufferException;
import edu.buffalo.cse.sql.buffer.ManagedFile;
import edu.buffalo.cse.sql.buffer.FileManager;
import edu.buffalo.cse.sql.test.TestDataStream;

public class HashIndex implements IndexFile {

	ManagedFile file;
	IndexKeySpec keySpec;
	static int filesize=0;
	static int schemaSize=0;
	static int dirsize=0;


	public HashIndex(ManagedFile file, IndexKeySpec keySpec)
			throws IOException, SqlException
			{
		this.file=file;
		this.keySpec=keySpec;
			}

	public static HashIndex create(FileManager fm,
			File path,
			Iterator<Datum[]> dataSource,
			IndexKeySpec key,
			int directorySize)
					throws SqlException, IOException
					{
		long time=System.currentTimeMillis();//long time no c!!! :-p (for this project thank god!!!)
		
		
		dirsize=directorySize;
		System.out.println("dir size: "+directorySize);
		Schema.Type[] schema=((TestDataStream)dataSource).getSchema();
		Schema.Type[] bucketschema={Schema.Type.INT};
		ManagedFile hashfile=fm.open(path);
		hashfile.resize(directorySize);
		Datum[] keydatTemp=new Datum[key.keySchema().length];
		int keylen=keydatTemp.length;
		int len=schema.length;
		int hash=-1;
		String bin=null;
		schemaSize=len;
		int bitsToConsider=10;
		
		//System.out.println("bits to consider "+bitsToConsider+" dir size "+directorySize);
		System.out.println("please wait while creating index...this might take a few minutes!!");
		for(int i=0;i<directorySize;i++){
		    (hashfile.safeGetBuffer(i)).putInt(0, directorySize+i);
			hashfile.dirty(i);
		}

		DatumBuffer[] datbuffarr=new DatumBuffer[directorySize];
		for(int i=0;i<directorySize;i++){
			datbuffarr[i]=new DatumBuffer(hashfile.safeGetBuffer(directorySize+i),schema);
			datbuffarr[i].initialize(40);
			hashfile.getBuffer(directorySize+i).putInt(5, 0);
			hashfile.dirty(directorySize+i);
		}
		filesize=hashfile.size();
		System.out.println("Buckets created...writing data into buckets..please wait!!");
		//System.out.println("filesize after initialization: "+filesize);
		 
		File file = new File("data.txt");
		if (!file.exists()) {
			file.createNewFile();
		}
		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);

		bin="0";
		for(int i=0;i<bitsToConsider-1;i++){
			bin +="0";
		}
		String temp=bin;
		
		while(dataSource.hasNext()){
			Datum[] datarr=dataSource.next();
			Datum[] keydat=key.createKey(datarr);
			String arr[]=new String[key.keySchema().length];
			bin=temp;
			int pagenum=0;
			ByteBuffer bufftemp=ByteBuffer.allocate(1024);
			for(int j=0;j<key.keySchema().length;j++){
				String a="key"+Integer.toString(j)+" is "+keydat[j].toInt()+"   ";
				bw.write(a);
				//System.out.print("key"+j+" is "+keydat[j].toInt()+"   ");
				arr[j]=Integer.toBinaryString(keydat[j].toInt());
				if(arr[j].length()<bitsToConsider){
					int lentemp=arr[j].length();
					for(int l=0;l<bitsToConsider-lentemp+1;l++){
						arr[j]="0"+arr[j];
					}
					//System.out.println("key: "+arr[j]);
				}
				//System.out.println(bin);
				bin=XOR(bin,arr[j].substring(arr[j].length()-bitsToConsider));
			}
			//bw.newLine();
			//System.out.println("   bin   "+bin);
			hash=(Integer.parseInt(bin, 2))%directorySize;
			bw.write(" "+"hash "+hash);
			bw.newLine();
			ByteBuffer bufftemp1=hashfile.getBuffer(hash);
			bufftemp1.position(0);
			pagenum=bufftemp1.getInt();
			//System.out.println("first page: "+pagenum+" hash : "+hash);
			bufftemp=hashfile.getBuffer(pagenum);
			boolean write=true;
			int next=-1;
			int curr=pagenum;
			int r=0;
			while(write){
				if((bufftemp.getInt(5)!=0)){
					next=bufftemp.getInt(0);
					//System.out.println("page -> "+next);
					bufftemp=hashfile.getBuffer(next);
					curr=next;//r++;if(r>10){break;}
				}
				else{
					datbuffarr[hash]=new DatumBuffer(hashfile.getBuffer(curr),schema);
					if((len*4)<datbuffarr[hash].remaining()-8){
						//System.out.println("writing on page: "+curr+" for hash: "+hash);
						//System.out.println("remaining space: "+(datbuffarr[hash].remaining()-8)+" page: "+curr);
						int idx=datbuffarr[hash].write(datarr);
						hashfile.getBuffer(curr).putInt(10, idx);
						//System.out.println("no. of records written on page: "+idx);
						hashfile.dirty(curr);
						write=false;
					}
					else{
						next=filesize+1;
						filesize++;
						//System.out.println("creating overflow bucket..on page  "+next);
						bufftemp.putInt(0, next);
						bufftemp.putInt(5, 1);
						hashfile.dirty(curr);
						bufftemp=hashfile.safeGetBuffer(next);
						datbuffarr[hash]=new DatumBuffer(bufftemp,schema);
						datbuffarr[hash].initialize(40);
						curr=next;
						next=-1;
						int idx=datbuffarr[hash].write(datarr);
						bufftemp.putInt(5, 0);
						bufftemp.putInt(10, idx);
						hashfile.dirty(curr);
						write=false;
					}
				}
			}
			//System.out.println("the pagunum in hashfile: "+ hashfile.getBuffer(hash).getInt(0));

		}
		
		System.out.println("Created index successfuly!!");
		System.out.println("time to create is : "+((double)(System.currentTimeMillis()-time)/1000)+" secs.");

		hashfile.resize(hashfile.size()+1);
		hashfile.getBuffer(hashfile.size()-1).putInt(0, directorySize);
		hashfile.getBuffer(hashfile.size()-1).putInt(4, bitsToConsider);
		hashfile.getBuffer(hashfile.size()-1).putInt(8, len);
		hashfile.dirty(hashfile.size()-1);
		hashfile.flush();
		fm.close(path);
		bw.close();
		return null;
					}

	public IndexIterator scan() 
			throws SqlException, IOException
			{
		return null;
			}

	public IndexIterator rangeScanTo(Datum[] toKey)
			throws SqlException, IOException
			{
		throw new SqlException("not supported");
			}

	public IndexIterator rangeScanFrom(Datum[] fromKey)
			throws SqlException, IOException
			{
		throw new SqlException("not supported");
			}

	public IndexIterator rangeScan(Datum[] start, Datum[] end)
			throws SqlException, IOException
			{
		throw new SqlException("not supported");
			}

	public Datum[] get(Datum[] key)
			throws SqlException, IOException
			{
		schemaSize=file.getBuffer(file.size()-1).getInt(8);
		dirsize=file.getBuffer(file.size()-1).getInt(0);
		Schema.Type[] scheme=new Schema.Type[schemaSize];
		for(int i=0;i<schemaSize;i++){
			scheme[i]=Schema.Type.INT;
		}
		Datum ret[]=new Datum[schemaSize];
		//System.out.println("Schema size: "+schemaSize+" Dir size: "+dirsize);
		//System.out.println("key1 length: "+key1.length);
		
		/*System.out.println("key diff: "+(keySpec.keySchema().length-key.length));
		if(key.length<=keySpec.keySchema().length){
			for(int i=0;i<=keySpec.keySchema().length-key.length-1;i++){
				key1[i+key.length]=new Datum.Int(0);
				System.out.print("key1"+i+" "+key1[i].toInt()+" ");
			}
		}*/
		//System.out.println();
		int bitsToConsider=file.getBuffer(file.size()-1).getInt(4);
		//System.out.println("bitstoconsider: "+bitsToConsider);
		String arr[]=new String[key.length];
		String bin;
		int pagenum=0;
		
		bin="0";
		for(int i=0;i<bitsToConsider-1;i++){
			bin +="0";
		}
	
		for(int j=0;j<key.length;j++){
			//System.out.print("key"+j+" is "+key1[j].toInt()+"   ");
			arr[j]=Integer.toBinaryString(key[j].toInt());
			if(arr[j].length()<bitsToConsider){
				int lentemp=arr[j].length();
				for(int l=0;l<bitsToConsider-lentemp+1;l++){
					arr[j]="0"+arr[j];
				}
			}
			bin=XOR(bin,arr[j].substring(arr[j].length()-bitsToConsider));
		}
		//System.out.println();
		//System.out.println("   bin   "+bin.substring(1));
		int hash=(Integer.parseInt(bin, 2))%dirsize;
		pagenum=file.getBuffer(hash).getInt(0);
		/*int num1=new DatumBuffer(file.getBuffer(pagenum),scheme).read(0)[0].toInt();
		int num2=new DatumBuffer(file.getBuffer(pagenum),scheme).read(0)[1].toInt();
		int num3=new DatumBuffer(file.getBuffer(pagenum),scheme).read(0)[2].toInt();
		int num4=new DatumBuffer(file.getBuffer(pagenum),scheme).read(0)[3].toInt();*/
		System.out.println("calc hash: "+hash+" page: "+pagenum);
		//System.out.println("<"+num1+" "+num2+" "+num3+" "+num4+">");
		boolean found=false;
		boolean last=false;
		while((found==false)||(last==false)){
			int curr=pagenum;
			int maxrecord=file.getBuffer(pagenum).getInt(10);
			System.out.println("no. records: "+maxrecord);
			for(int i=0;i<=maxrecord;i++){
				int flag=0;
				for(int j=0;j<key.length;j++){
					DatumBuffer datbuff=new DatumBuffer(file.getBuffer(pagenum),scheme);
					//System.out.println("search key: "+key[j].toInt()+" found key: "+
							//(keySpec.createKey(datbuff.read(i))[j]).toInt());
					if(key[j].equals(keySpec.createKey(datbuff.read(i))[j])){
						flag++;
					}
					
				}
				//System.out.println("flag val: "+flag);
				if(key.length==flag){
					DatumBuffer datbuff=new DatumBuffer(file.getBuffer(pagenum),scheme);
					ret=datbuff.read(i);
					found=true;
					last=true;
					break;
				}
				if(last==true){
					found=true;
				}
				
			}
			
			pagenum=file.getBuffer(curr).getInt(0);
			System.out.println("next page: "+pagenum);
			if(file.getBuffer(pagenum).getInt(5)==0){
				//System.out.println(pagenum+" is last page");
				last=true;
			}
			if((last==true)&&(found==false)){
				ret=null;
			}
		}
		
		return ret;
			}
	
	public static String XOR(String a,String b){
		//System.out.println("a= "+a+" b= "+b);
		int length=a.length();
		String ret="*";
		for(int i=0;i<length;i++){
			if(a.charAt(i)!=b.charAt(i)){
				ret +="1";
			}
			else{
				ret +=0;
			}
		}
		ret=ret.substring(1);
		//System.out.println("ret after XOR: "+ret);
		return ret;
	}

}