package database;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Write_class 

{
	String table_name;
	
	public Write_class(String tab_name)
	{
		this.table_name=tab_name;
	}
	
	boolean unique(String value, Column col, String schema_used)
	{
		
		boolean check=true;
		try 
		{
			String ind_name= schema_used+"."+table_name+"."+col.column_name+".ndx";
			
			RandomAccessFile index_table= new RandomAccessFile(ind_name,"rw");
			
			int position=0;
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
				
				if(value.equalsIgnoreCase(v))
				{
					return false;
					
					
				}
				else
				{
					position=position+4*records;
					
				}
			}
			
			index_table.close();
			
		}
		
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		return check;
	}
	
	boolean add_table(RandomAccessFile table, String[] values, Column[] cols, String schema_used)
	{
		//creating a new values array to include defaults if not specified in the input
		try 
		{

			String[] vals= new String[cols.length];
			for(int i=0; i<cols.length ; i++)
			{
				if(i<values.length)
				{
					if(cols[i].primary_key)
					{
						if(!unique(values[i],cols[i],schema_used)) return false ;
						
						else if(values[i].contains("null"))
						{
							System.out.println("primary key cant be a null value");
							return false;
						}
						
						else
						vals[i]=values[i];
						
					}
					else
						vals[i]=values[i];
					
						
					
				}
				else
				{
					if(cols[i].primary_key)
						return false;
					else
						vals[i]="null";
				}
				
			}
			
			long location=table.length();
			table.seek(location);
			List<String> l;
			
			for(int i=0; i<vals.length;i++)
			{
				//adds the record for each value
				table.seek(table.length());

				add_record(table,vals[i],cols[i],table.length());
				
				//updates the index table of each value
				
				l= new ArrayList<String>();
				
				l.add(vals[i]);
				l.add("1");
				l.add(Long.toString(location));
				
				String ind_name= schema_used+"."+table_name+"."+cols[i].column_name+".ndx";
				
				
				RandomAccessFile index_file= new RandomAccessFile(ind_name,"rw");
				
				
				//changing the entries in the corresponding index file
				
				create_new_record(index_file,l);
				
			}
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		return true;
	}
	void add_record(RandomAccessFile table_file,String value, Column c,long locat)
	{
		try 
		{
			if(value.equalsIgnoreCase("null"))
			{
				table_file.writeByte(value.length());
				table_file.writeBytes(value);
				return;
				
			}
			
			else if(c.column_type=="byte")
			{
				
				table_file.writeByte(Byte.parseByte(value));
				 
			}
			else if(c.column_type=="int")
			{
				
				table_file.writeInt(Integer.parseInt(value));
				
				 
			}
			else if(c.column_type=="float")
			{
					table_file.writeFloat(Float.parseFloat(value));
				
			}
			else if(c.column_type=="double")
			{
				
				table_file.writeDouble(Double.parseDouble(value));
				 
			}
			else if(c.column_type=="long")
			{
				
				table_file.writeLong(Long.parseLong(value));
				 
			}
			else if(c.column_type=="short")
			{
				
				table_file.writeShort(Short.parseShort(value));
				//long ssdsd=table_file.length();

				//table_file.seek(locat);

				//short recc = table_file.readShort();
				//table_file.seek(locat);

				//short rec =0;
				 
			}
			else if(c.column_type=="varchar")
			{
				
				table_file.writeByte(value.length());
				table_file.writeBytes(value);
				 
			}
			else if(c.column_type=="char")
			{
				table_file.writeByte(value.length());
				table_file.writeBytes(value);
				 
			}
			else if( c.column_type.contains("date"))
			{
				table_file.writeByte(value.length());
				table_file.writeBytes(value);
				
				 
			}
			else if(c.column_type=="datetime")
			{
				table_file.writeByte(value.length());
				table_file.writeBytes(value);
			}
		
	
		}
		catch (NumberFormatException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
	}
	void create_new_record(RandomAccessFile index_file,List<String> l)
	{
		
		List<List<String>> ll= parse_index_file(index_file);
		
		if(ll==null)
		{
			ll=new ArrayList<List<String>>();
		}
		
		boolean record_found=false;
		
		for(int i=0; i<ll.size();i++)
		{
			if(ll.get(i).get(0).equalsIgnoreCase(l.get(0)))
			{
				//record found
				ll.get(i).add(l.get(2));  //i am adding location stored in 3rd index of l to the record in the index file
				
				ll.get(i).set(1, Integer.toString(Integer.parseInt(ll.get(i).get(1))+1));
				record_found=true;
			}
		}
		
		if(record_found==false)
		{
			ll.add(l);
		}
		
		//ll is updated. now this new ll must be transfered to the index file
		
		
		update_index_file(index_file,ll);
		
	}
	
	
	void update_index_file(RandomAccessFile index_file,List<List<String>> ll)
	{
		
		//delete contents of file
		try 
		{
			index_file.setLength(0);


			for(int i=0; i<ll.size();i++)
			{
			
				//write value,no of records and locations of the records
		
				 
				for(int j=0;j<ll.get(i).size();j++)
				{
					if(j==0 ) //writing the value in string format
					{
						index_file.writeByte(ll.get(i).get(j).length());
						
						index_file.writeBytes(ll.get(i).get(j));
					
						
					}
					
					else //writing the number of locations and offsets
					{
						index_file.writeInt(Integer.parseInt(ll.get(i).get(j)));
										
					}
				}
							
			}
			
			
		} 
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	List<List<String>> parse_index_file (RandomAccessFile index_file)
	{
		//aassuming value of index is of type int
		
		List<List<String>> ll=new ArrayList<List<String>>();
		try 
		{
			int position=0;
			index_file.seek(position);
			
			List<String> l;
		//int chh= (int)index_file.length();
			
			while(position<(int)index_file.length())
			{
				l=new ArrayList<String>();
				
				
				
				String value="";
				int varcharLength = Integer.parseInt(Byte.toString(index_file.readByte()));
				position=position+1;
				for(int i = 0; i < varcharLength; i++)
				{
					value=value+(char)index_file.readByte();
					position=position+1;
				}
					
				l.add(value);
				
				index_file.seek(position);
				
				int no_of_records=index_file.readInt();
				l.add(Integer.toString(no_of_records));
				position=position+4;
				

				
				for(int i=0; i< no_of_records ; i++)
				{
					index_file.seek(position); //the record position
					
					int offset= index_file.readInt();
					position=position+4;//increment position after reading the record
					
					l.add(Integer.toString(offset));
					
						
				}
				ll.add(l);
			
			}
		
		
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return ll;
		
	}

}
