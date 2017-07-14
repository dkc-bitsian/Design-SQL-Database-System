package database;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Read_class 
{
	
	RandomAccessFile table_file; 
	RandomAccessFile index_table;
	Column[] col;
	
	
	
	public Read_class(String table_name,String id,Column[] c)
	{
		try {
			this.table_file=new RandomAccessFile(table_name, "r");
			this.index_table=new RandomAccessFile(id, "r");
			this.col=c;

		} 
		catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
			
			
	List<List<String>> reader( String value ,String col_name,String col_type, String operator)
	{
		List<List<String>> ll=new ArrayList<List<String>>();
		try 
		{			
			
			int position=0;
		//	int no_of_records=0;
			//int[] offsets_array =null;
			int location;
			
			boolean record_found=false;
			while(position<(int)index_table.length())
			{
				index_table.seek(position);
				String v="";
				int varcharLength = Integer.parseInt(Byte.toString(index_table.readByte()));
				position=position+1;
				for(int i = 0; i < varcharLength; i++)
				{
					v=v+(char)index_table.readByte();
					position=position+1;
				}
				
				index_table.seek(position);
				
				int records=index_table.readInt();
				
				
				position=position+4;
				
				boolean expression ;
				if( operator.contains(">")||operator.contains("<"))
					expression=value_checker(operator,v,value,col_type);
				
				else expression=value.equalsIgnoreCase(v);
					
					
					
				if(expression)
				{
					
					List<String> l;
					
					for(int i=0 ; i<records ; i++)
					{
						index_table.seek(position);
						
						
						location=index_table.readInt();
						
						l= get_record(location);
						
						ll.add(l);
						
						position=position+4;
						
						
						
					}
					
					
					record_found=true;
					
				}
				else
				{
					position=position+4*records;
					
				}
			}
			
		if(record_found==false)
			System.out.println("No such records found");
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return ll;
	}
	boolean value_checker(String operator,String v,String value,String col_type)
	{
		//v is the actual value read from table and value is given value
		
		boolean expression=false;
		
		if(col_type=="byte")
		{
			if(operator.equals(">"))
			{
				expression=Byte.parseByte(v) > Byte.parseByte(value);			
				
			}
			if(operator.equals("<"))
			{
				expression=Byte.parseByte(v) < Byte.parseByte(value);			
				
			}
			 
		}
		else if(col_type=="int")
		{
			if(operator.equals(">"))
			{
				expression=Integer.parseInt(v) > Integer.parseInt(value);			
				
			}
			if(operator.equals("<"))
			{
				expression=Integer.parseInt(v) < Integer.parseInt(value);			
				
			}
		}
		else if(col_type=="float")
		{
			if(operator.equals(">"))
			{
				expression=Float.parseFloat(v) > Float.parseFloat(value);			
				
			}
			if(operator.equals("<"))
			{
				expression=Float.parseFloat(v) < Float.parseFloat(value);			
				
			}
		}
		else if(col_type=="double")
		{

			if(operator.equals(">"))
			{
				expression=Double.parseDouble(v) > Double.parseDouble(value);			
				
			}
			if(operator.equals("<"))
			{
				expression=Double.parseDouble(v) < Double.parseDouble(value);			
				
			}
		
			 
		}
		else if(col_type=="long")
		{

			if(operator.equals(">"))
			{
				expression=Long.parseLong(v) > Long.parseLong(value);			
				
			}
			if(operator.equals("<"))
			{
				expression=Long.parseLong(v) < Long.parseLong(value);			
				
			}
			 
		}
		else if(col_type=="short")
		{

			if(operator.equals(">"))
			{
				expression=Short.parseShort(v) > Short.parseShort(value);			
				
			}
			if(operator.equals("<"))
			{
				expression=Short.parseShort(v) < Short.parseShort(value);			
				
			}
		

		}
		
		else if(col_type.contains("date"))
		{


			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
			try {
				Date value1 =  df.parse(value);
				Date v1 =  df.parse(v);
				
				if(operator.equals(">"))
				{
					if(v1.compareTo(value1)>0)
					expression=true;
					else
						expression= false;
					
					
					
				}
				if(operator.equals("<"))
				{
					if(v1.compareTo(value1)<0)
					expression=true;
					else
						expression= false;
					
					
					
				}
				
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		 
		}
		else if(col_type.contains("datetime"))
		{
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
			try {
				Date value1 =  df.parse(value);
				Date v1 =  df.parse(v);
				
				if(operator.equals(">"))
				{
					if(v1.compareTo(value1)>0)
					expression=true;
					else
						expression= false;
					
					
					
				}
				if(operator.equals("<"))
				{
					if(v1.compareTo(value1)<0)
					expression=true;
					else
						expression= false;
					
					
					
				}
				
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
		
		
		
		return expression;
		
		
		
	}

	List<String> get_record(int location)
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
			else if(col_type=="varchar" )
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
			else if(col_type.contains("date"))
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
 class String_position
{
	
	int i;
	String s;
	
	String_position(String ss,int ii)
	{
		
		this.i=ii;
		this.s=ss;
		
	}
	
	
}


