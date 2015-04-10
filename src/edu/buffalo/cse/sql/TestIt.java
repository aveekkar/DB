package edu.buffalo.cse.sql;

import java.awt.List;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

import edu.buffalo.cse.sql.Schema.TableFromFile;
import edu.buffalo.cse.sql.Schema.Type;
import edu.buffalo.cse.sql.data.Datum;

public class TestIt  implements Iterator{
	ArrayList<Schema.Var> var=new ArrayList<Schema.Var>();
	File file;
	FileInputStream fhandle;
	DataInputStream finp;
	BufferedReader buf;
	String key;
	String str;

	
	int count=0;
	boolean isnull=false;

	HashMap<String,Schema.TableFromFile> tab=new HashMap<String,Schema.TableFromFile>();
	TestIt(HashMap<String,Schema.TableFromFile> tab){
		this.tab= tab;
	}
	
	public TestIt(HashMap<String,Schema.TableFromFile> tab, String key){
		this.tab=tab;
		this.key=key;
		if(key.equals("]")){
			isnull=true;
		}else{
		var.addAll(((Schema.Table)tab.get(key)).names());
		file=tab.get(key).getFile();
		if(!file.toString().startsWith("test")){
			file=new File("test/"+file.toString());
		}
		try {
			fhandle=new FileInputStream(file);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		finp=new DataInputStream(fhandle);
		buf=new BufferedReader(new InputStreamReader(finp));
		}
	}

	public ArrayList<Datum[]> tableDetails(String key) throws IOException{
		ArrayList<Datum[]> list = new ArrayList<Datum[]>();
		ArrayList<Schema.Column> col=new ArrayList<Schema.Column>();
		File file;

		if(key.equals("]")){
			Datum[] tmp=new Datum[1];
			tmp[0]=new Datum.Str("null");
			list.add(tmp);
			return list;
		}



		ArrayList<Schema.Var> var=new ArrayList<Schema.Var>();

		var.addAll(((Schema.Table)tab.get(key)).names());
		System.out.println(var.toString()+var.size());
		file=tab.get(key).getFile();
		if(!file.toString().startsWith("test/")){
			file=new File("test/"+file.toString());
		}

		try {
			//System.out.println(file.toString());
			String str;
			System.out.println("Outside the while loop");

			while((str=buf.readLine())!=null){
				//System.out.println("inside the while loop");
		        //System.out.println("String is:"+ buf.readLine());
				//System.out.println("to1:"+str);
				StringTokenizer stk=null;
				if(file.toString().length()==10){
					stk=new StringTokenizer(str,",");
				}
				else{
					stk=new StringTokenizer(str,"|");
				}
	
	
				int tcnt=var.size();
				//stk.countTokens();
				Datum[] dat=new Datum[tcnt];
				for(int i=0;i<tcnt;i++){
					//System.out.println("schema is:"+ tab.get(key).lookup(var.get(i)).type);
					if(tab.get(key).lookup(var.get(i)).type==Schema.Type.INT){
						String tok=stk.nextToken().toString();
					//	System.out.println("integer token: "+tok);
						if((tok.length()>9)&&(tok.substring(4,5).equals("-"))){
							String arr[]=tok.split("-");
							tok=arr[0]+arr[1]+arr[2];
						}
				//		System.out.println("token is:"+tok);
						dat[i]=new Datum.Int(Integer.parseInt(tok));
					}
					if(tab.get(key).lookup(var.get(i)).type==Schema.Type.FLOAT){
						dat[i]=new Datum.Flt(Float.parseFloat(stk.nextToken().toString()));
					}
					if(tab.get(key).lookup(var.get(i)).type==Schema.Type.STRING){
						
						dat[i]=new Datum.Str(stk.nextToken().toString());
				//		System.out.println("String token: "+dat[i]);
					}
					if(tab.get(key).lookup(var.get(i)).type==Schema.Type.BOOL){
						dat[i]=new Datum.Bool(Boolean.parseBoolean(stk.nextToken().toString()));
					}
					//System.out.println("String class "+ dat[i]);
				}
				list.add(dat);

			}
			finp.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		for(int k=0;k<var.size();k++){
			col.add(tab.get(key).lookup(var.get(k)));
		}

		//System.out.println("done parsing file");
		return list;
	}

	@Override
	public boolean hasNext() {
		if(isnull){
			if(count==1){
				return false;
			}
			else{
				return true;
			}
		}
		else{
			try {
				if((str=buf.readLine())!=null){
					return true;
				}
				else 
					return false;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return false;
	}

	@Override
	public Datum[] next() {
		if(isnull){
			Datum[] tmp=new Datum[1];
			tmp[0]=new Datum.Str("null");
			count++;
			return tmp;
		}
		ArrayList<Schema.Column> col=new ArrayList<Schema.Column>();
			
			int tcnt=var.size();
			Datum[] dat=new Datum[tcnt];

				//System.out.println("inside if");
				//System.out.println("to1:"+str);
				StringTokenizer stk=null;
				if(file.toString().length()==10){
					stk=new StringTokenizer(str,",");
				}
				else{
					stk=new StringTokenizer(str,"|");
				}
				for(int i=0;i<tcnt;i++){
					if(tab.get(key).lookup(var.get(i)).type==Schema.Type.INT){
						String tok=stk.nextToken().toString();

						if((tok.length()>9)&&(tok.substring(4,5).equals("-"))){
							String arr[]=tok.split("-");
							tok=arr[0]+arr[1]+arr[2];
						}
						dat[i]=new Datum.Int(Integer.parseInt(tok));
					}
					if(tab.get(key).lookup(var.get(i)).type==Schema.Type.FLOAT){
						dat[i]=new Datum.Flt(Float.parseFloat(stk.nextToken().toString()));
					}
					if(tab.get(key).lookup(var.get(i)).type==Schema.Type.STRING){
						
						dat[i]=new Datum.Str(stk.nextToken().toString());
					}
					if(tab.get(key).lookup(var.get(i)).type==Schema.Type.BOOL){
						dat[i]=new Datum.Bool(Boolean.parseBoolean(stk.nextToken().toString()));
					}
					//System.out.println("String class "+ dat[i]);
				}

				for(int k=0;k<var.size();k++){
			col.add(tab.get(key).lookup(var.get(k)));
		}

		return dat;
	}

	@Override
	public void remove() {
		// TODO Auto-generated method stub
		
	}

}
