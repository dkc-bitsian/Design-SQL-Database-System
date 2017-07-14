package database;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

public class Read_all {

	
	private Column[] col;
	RandomAccessFile table_file; 
	 int location=0 ;
	
	
	public Read_all(Column[] cols, String tn) {
		// TODO Auto-generated constructor stub
		try {
			this.table_file=new RandomAccessFile(tn, "r");
			this.col=cols;

		} 
		catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
	}

	
	List<List<String>> readall()
	{
		List<List<String>> ll=new ArrayList<List<String>>();
		
		
		try {
			while(location<(int)table_file.length())
			{
				
				List<String> l;
				
				l= get_record();
				
				ll.add(l);
				
				
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return ll;
		
	}	
	List<String> get_record()
	{
		
		List<String> l= new ArrayList<String>();
		
		
		for(int i=0; i< col.length ; i++)
		{
			//parse that particular column
			String_position sp;
			
			//returns the string as well the number of bytes read
			sp=parse_col(location,col[i].column_type);
			
			l.add(sp.s);
			
			location=location+sp.i;
			
		}
		
		
		return l;
	}
	
	String_position parse_col(int location, String col_type)
	{
		
		String_position sp= null;
		String s;
		
		
		
		
		try 
		{
			table_file.seek(location);
			
			
			int varLength = Integer.parseInt(Byte.toString(table_file.readByte()));
			int pos=1;
			String ss="";
			if(varLength==4)
			{
			for(int i = 0; i < varLength; i++)
			{
				ss=ss+(char)table_file.readByte();
				pos=pos+1;
			}
			}
			
			
			if(ss.equalsIgnoreCase("null"))
			{
				sp= new String_position(ss,pos);
				return sp;
				
			}
			
			table_file.seek(location);
			
			
			if(col_type=="byte")
			{
				
				s=Byte.toString(table_file.readByte());
				sp= new String_position(s,1);
				 
			}
			else if(col_type=="int")
			{
				int k=table_file.readInt();
				s=Integer.toString(k);
				sp= new String_position(s,4);
			}
			else if(col_type=="float")
			{
				
				s=Float.toString(table_file.readFloat());
				sp= new String_position(s,4);
				
				 
			}
			else if(col_type=="double")
			{
				
				s=Double.toString(table_file.readDouble());
				sp= new String_position(s,8);
				
				 
			}
			else if(col_type=="long")
			{

				s=Long.toString(table_file.readLong());
				sp= new String_position(s,8);
				 
			}
			else if(col_type=="short")
			{
				s=Short.toString(table_file.readShort());
				sp= new String_position(s,2);

			}
			else if(col_type=="varchar")
			{
				int varcharLength = Integer.parseInt(Byte.toString(table_file.readByte()));
				int po=1;
				s="";
				for(int i = 0; i < varcharLength; i++)
				{
					s=s+(char)table_file.readByte();
					po=po+1;
				}
				sp= new String_position(s,po);
			
			}
			else if(col_type=="char")
			{
				int varcharLength = Integer.parseInt(Byte.toString(table_file.readByte()));
				int po=1;
				s="";
				for(int i = 0; i < varcharLength; i++)
				{
					s=s+(char)table_file.readByte();
					po=po+1;
				}
				sp= new String_position(s,po);
				 
			}
			else if(col_type=="date")
			{
				int varcharLength = Integer.parseInt(Byte.toString(table_file.readByte()));
				int po=1;
				s="";
				for(int i = 0; i < varcharLength; i++)
				{
					s=s+(char)table_file.readByte();
					po=po+1;
				}
				sp= new String_position(s,po);
				 
			}
			else if(col_type=="datetime")
			{
				int varcharLength = Integer.parseInt(Byte.toString(table_file.readByte()));
				int po=1;
				s="";
				for(int i = 0; i < varcharLength; i++)
				{
					s=s+(char)table_file.readByte();
					po=po+1;
				}
				sp= new String_position(s,po);
			}
		

			
			
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return sp;
		
	}
	
}
