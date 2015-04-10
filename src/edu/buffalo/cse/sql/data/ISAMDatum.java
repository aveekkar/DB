package edu.buffalo.cse.sql.data;

import java.nio.ByteBuffer;

import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.Schema.Type;
import edu.buffalo.cse.sql.data.Datum.CastError;

public class ISAMDatum  {
	
	protected ByteBuffer buffer;
	protected int position;
	protected int numvalue;
	protected int ptrvalue;
	protected int keyposition=1012;
	protected int ptrposition=1016;
	protected int inreserve;
	protected int length;
	
	public ISAMDatum(ByteBuffer buffer,Schema.Type[] keyschema){
		this.buffer=buffer;
		length=keyschema.length;
		numvalue=buffer.getInt(1020);
	}
	public void initialize(){
		initialize(0);
	}
	
	public void initialize(int reserve){
		this.position=reserve;
		this.inreserve=reserve;
		buffer.position(1012);
		buffer.putInt(1020, 0);
		numvalue=0;
	}
	
	public void createPointer(int pointer) throws Exception{
		int temp=position;
		//System.out.println("writin pointer @: "+position);
		buffer.putInt(position,pointer);
		position+=4;
		buffer.putInt(ptrposition,temp);
		//System.out.println("pointer "+temp+" -> "+ptrposition);
		ptrposition -=8;
		ptrvalue++;
		if(ptrvalue-numvalue>1){
			throw new Exception("invalid pointer insertion: ptr = "+ptrvalue+" num = "+numvalue);
		}
	}
	
	public int putKey(Datum[] key) throws Exception{
		if((key.length*4)+12>remaining()){
			throw new Exception("ran out of space on index page!!");
		}
		int temp=position;
		for(int i=0;i<key.length;i++){
			//System.out.println("writin key @: "+position+" index of datum: "+i);
			//System.out.println("putting key: "+Datum.stringOfRow(key));
			buffer.putFloat(position,key[i].toFloat());
			position+=4;
		}
		//System.out.println("key no. "+numvalue);
		buffer.putInt(keyposition, temp);
		keyposition -=8;
		buffer.putInt(1020, numvalue+1);
		return ++numvalue;
	}
	
	public int remaining(){
		return ptrposition<keyposition?ptrposition-position:keyposition-position;
	}
	
	
	public Datum[] getKey(int num){
		//System.out.println(num);
		if(num>numvalue||num==0){
			try {
				throw new Exception("accesing illegal value!!");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		Datum[] dat=new Datum[length];
		int temp=1012;
		for(int i=1;i<num;i++){
			temp -= 8;
		}
		temp=buffer.getInt(temp);
		for(int i=0;i<length;i++){
			dat[i]=new Datum.Flt(buffer.getFloat(temp));
			temp +=4;
		}
		return dat;
		
	}
	
	public int lesserPtr(int num){
		int temp=1012;
		for(int i=1;i<num;i++){
			temp -= 8;
		}
		return buffer.getInt(buffer.getInt(temp+4));
	}
	
	public int greaterPtr(int num){
		int temp=1012;
		for(int i=1;i<num;i++){
			temp -= 8;
		}
		return buffer.getInt(buffer.getInt(temp-4));
	}
	
	public void showPage(){
		int i;
		for(i=1;i<numvalue+1;i++){
			System.out.print("|->"+buffer.getInt(buffer.getInt(1016-(i-1)*8))+"|  ");
			System.out.print("key0: "+buffer.getFloat(buffer.getInt(1012-(i-1)*8))+"  ");
		}
		System.out.print("|"+buffer.getInt(buffer.getInt(1016-(i-1)*8))+" |");
		System.out.println();
	}
	
	public void delete(){
		position -=4;
		for(int i=0;i<length;i++){
			position -=4;
		}
		buffer.putInt(1020, --numvalue);
		keyposition +=8;
		ptrposition +=8;
		
	}
	
	public boolean hasPrevious(){
		return keyposition<1004?true:false;
	}
	
	public int getPrevious(){
		return numvalue-1;
	}
	
	public int getNumRows(){
		return buffer.getInt(1020);
	}
	
	public int pointer(Datum[] key){
		int pointer=-1;
		if(Datum.Flt.compareRows(key, getKey(1))==-1||Datum.Flt.compareRows(key, getKey(1))==0){
			return lesserPtr(1);
		}
		else if(Datum.Flt.compareRows(key, getKey(getNumRows()))==1||
				Datum.Flt.compareRows(key, getKey(getNumRows()))==0){
			return greaterPtr(getNumRows());
		}
		else{
			for(int i=1;i<getNumRows();i++){
				
				//System.out.println("trying to locate interval for "+Datum.stringOfRow(key));
				if((Datum.Flt.compareRows(key, getKey(i))==1) && (Datum.Flt.compareRows(key, getKey(i+1))==-1)||(Datum.Flt.compareRows(key, getKey(i+1))==0)){
						pointer= lesserPtr(i+1);
						//System.out.println("pointer is:"+pointer);
						break;
					
				}
			
			}
		}
		
		return pointer;
	}
	
}
