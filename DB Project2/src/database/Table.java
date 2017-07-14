package database;


import java.io.RandomAccessFile;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;




public class Table 
{
	String schema_used;
	
	
	
	public Table(String s)
	{
		this.schema_used=s;
	}
	
	void create_table(String s, RandomAccessFile tables)
	{
		try 
		{

			String table_name = table_name_parser(s,3);
			//System.out.println(table_name);
			String table_name_schema=table_name+" schema";
			String tn= schema_used+"."+table_name+".tbl";
			
			RandomAccessFile table_info = new RandomAccessFile(table_name_schema, "rw") ;
			RandomAccessFile table = new RandomAccessFile(tn, "rw") ;
			
			
			table_info.write(s.getBytes());
			table_info.seek(0);

			Parser p =new Parser();
			Column[] cols= p.parser(table_info.readLine());
			
			
			update_tables(tables,table_name,schema_used,"0");
			
			for(int i=0; i<cols.length;i++)
			{
				String add=cols[i].column_name;
				String index_name=schema_used+"."+table_name+".";
				index_name=index_name +add+".ndx";
				
				RandomAccessFile table_id = new RandomAccessFile(index_name, "rw") ;
				table_id.close();
				
			}
			
			
			table_info.close();
			
		} 
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
		
	}
	
	private void update_tables(RandomAccessFile tables, String table_name, String schema_used2, String string) {
		// TODO Auto-generated method stub
		try
		{
			tables.seek(tables.length());
			
			tables.writeByte(schema_used2.length());
			tables.writeBytes(schema_used2);
			

			tables.writeByte(table_name.length());
			tables.writeBytes(table_name);

			tables.writeByte(string.length());
			tables.writeBytes(string);
			
			long asd=tables.length();
				long as=tables.length();
		}
		catch ( IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

	void insert_table(String s, RandomAccessFile tables, RandomAccessFile columns) 
	{
		try 
		{
			
			String table_name= table_name_parser(s,4);
			String path_schema=table_name+" schema";
			String[] values= values_insert_parser(s);
			//System.out.println(path_schema);

			String tn=schema_used+"."+table_name+".tbl";
			
			RandomAccessFile table_info = new RandomAccessFile(path_schema, "r");
			RandomAccessFile table_file = new RandomAccessFile(tn, "rw");
			table_info.seek(0);
			
			Parser p =new Parser();
			Column[] cols= p.parser(table_info.readLine());
			
			table_info.close();
			
			boolean check = false; 
			//inserting the values into the table
			
			Write_class write_table= new Write_class(table_name);
			check=write_table.add_table(table_file, values, cols,schema_used);
			
			if(check==false)
			{
				System.out.println("record cant be added because primary key is not unique");
				return;
			}
			else		
			{
				System.out.println("Record has been successfully inserted into the table");
				//modify columns table;
				
				
				
				//modify tables table..increment row
				
				modify_mainTables(tables,table_name,schema_used);
				
			}
			
			
			table_file.close();
		//	tables.close();
			
			
		}
		catch ( IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	

	private void modify_mainTables(RandomAccessFile tables, String table_name, String schema_used2) {
		// TODO Auto-generated method stub
		
		try 
		{
			tables.seek(0);
			List<List<String>> ll=new ArrayList<List<String>>();
				
				int position=0;
				Long asads= tables.length();
				
				while(position<(int)tables.length())
				{
					List<String> l= new ArrayList<String>();
					tables.seek(position);
					String sname="";
					String tname="";
					String rows="";
					int varcharLength = Integer.parseInt(Byte.toString(tables.readByte()));
					position=position+1;
					for(int i = 0; i < varcharLength; i++)
					{
						sname=sname+(char)tables.readByte();
						position=position+1;
					}
					
					tables.seek(position);
					
					varcharLength = Integer.parseInt(Byte.toString(tables.readByte()));
					position=position+1;
					for(int i = 0; i < varcharLength; i++)
					{
						tname=tname+(char)tables.readByte();
						position=position+1;
					}
					
					tables.seek(position);
					
					varcharLength = Integer.parseInt(Byte.toString(tables.readByte()));
					position=position+1;
					for(int i = 0; i < varcharLength; i++)
					{
						rows=rows+(char)tables.readByte();
						position=position+1;
					}
					
					
					
					l.add(sname);
					l.add(tname);
					
					if(tname.equalsIgnoreCase(table_name)&&sname.equalsIgnoreCase(schema_used2))
					{
						
						String r= Integer.toString(Integer.parseInt(rows)+1);
						
						l.add(r);
						
						
					}
					else
						l.add(rows);
					
					
					ll.add(l);
					
					
				}
				
				
				adding_list(ll,tables);
			
		}
		catch ( IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

	private void adding_list(List<List<String>> ll, RandomAccessFile tables) {
		// TODO Auto-generated method stub
		try 
		{
			tables.setLength(0);


			for(int i=0; i<ll.size();i++)
			{
			
				//write value,no of records and locations of the records
		
					
				for(int j=0;j<ll.get(i).size();j++)
				{
					
						tables.writeByte(ll.get(i).get(j).length());
						
						tables.writeBytes(ll.get(i).get(j));
					
						
					
				}			
			
			}
		
		}
	catch ( IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	
	}

	String[] values_insert_parser (String s)
	{
		String s1;
		s1=s.substring(s.indexOf("(") + 1,s.indexOf(")"));
		String[] ss= s1.split(",");
		
		for(int i=0 ; i<ss.length ; i++)
		{
			
			if(ss[i].contains("'"))
					ss[i]=ss[i].substring(1,ss[i].length()-1);
			
		}
		
		
		return ss;
		

	}
	


	String table_name_parser(String s,int t)
	{
		
		String[] aa =s.split(" ");
		String r="no_id";
		Pattern p = Pattern.compile("[a-zA-Z]");
		Matcher m ;
		int k=0;
		
		for(int i=0; i<aa.length; i++)
		{
			m=p.matcher(aa[i]);
			if(m.find())
			{
				r=aa[i];
				k++;
			}
			if(k==t) break;
			
		}
		return r;
		
	}
	String query_parser(String s,int t)
	{
		//exactly one space //query semi colon space 
		String[] aa =s.split(" ");
		String r="no_id";
		int k=0;
		
		for(int i=0; i<aa.length; i++)
		{
			
				r=aa[i];
				k++;
			
			if(k==t) break;
			
		}
		return r;
		
	}
	
	String query (String s)
	{
		try
		{
		String table_name=query_parser(s,4);
		String col_name;
		String value;
		String operator=null;
		if(!s.toLowerCase().contains("where"))
		{
			value=null;
			col_name=null;
		}
		else
		{
			
				String combo= query_parser(s,6); 
				
				if(combo.contains(">"))
					operator=">";
				else if(combo.contains("<"))
					operator="<";
				else if(combo.contains("="))
					operator="=";
				
				
				String[] aa=combo.split(operator);
				
			
				col_name=aa[0];

				value=aa[1];
				if(value.contains("'"))
				{
					value=value.substring(1,value.length()-1);		
				}
				if(value.contains("'"))
				{
					value=value.substring(0,value.length()-1);		
				}
				

		}
			
		
		String output ;
		
		String path_schema=table_name+" schema";
		String col_id= schema_used+"."+table_name +"."+col_name+".ndx";
		
		String tn= schema_used+"."+table_name +".tbl";
		//System.out.println(path_schema);
		
		RandomAccessFile table_info = new RandomAccessFile(path_schema, "r");
		table_info.seek(0);
		
		Parser p =new Parser();
		Column[] cols= p.parser(table_info.readLine());
		
		table_info.close();	
		
		
		String col_type=null;
		
		for(int i =0 ; i< cols.length; i++)
		{
			if(cols[i].column_name.equalsIgnoreCase(col_name))
				col_type=cols[i].column_type;
			
			
		}
		
		if(value==null)
		{
			
			//read all records in table column wise
			
			Read_all r = new Read_all(cols,tn);
			
			List<List<String>> ll=r.readall();
			if(ll.isEmpty())
				System.out.println("no records to display");
			else
				display(ll,cols);
			
			
		}
		
		else
		{
			//read only records corresponding col_name=value
			
			
			
			Read_class read= new Read_class(tn,col_id,cols);
			List<List<String>> ll=read.reader( value, col_name,col_type,operator);
			
			if(ll.isEmpty())
				System.out.println("no matching records to display");
			else
				display(ll,cols);
			
			
		}
		
		
		
		
		
		}
		catch ( IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		
		
		return s;
		
	}

	private void display(List<List<String>> ll, Column[] cols) {
		// TODO Auto-generated method stub
		
		System.out.println(line("*",50));

		for(int i=0; i<cols.length ; i++)
		{
			System.out.print(cols[i].column_name + " -  " );
		}
		
		System.out.println();
		
		System.out.println(line("*",50));

		for(int i=0; i <ll.size();i++)
		{
			
			for(int j=0; j<ll.get(i).size(); j++)
			{
				
				System.out.print(ll.get(i).get(j)+"  -   ");
				
				
			}
			System.out.println("");
		
		
		}
		System.out.println(line("*",50));

		
	}

	public void create_schema(String userCommand, RandomAccessFile schemata) 
	{
		// TODO Auto-generated method stub
		
		String schema_name=query_parser(userCommand,3);
		try {
			schemata.seek(schemata.length());
			schemata.writeByte(schema_name.length());
			schemata.writeBytes(schema_name);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		
	}

	public String use_schema(String userCommand) 
	{
		
		String schema_name=query_parser(userCommand,2);
		schema_used=schema_name;
		
		return schema_name;
		
		// TODO Auto-generated method stub
		
	}

	public void show_schema(String userCommand, RandomAccessFile schemata) 
	{
		// TODO Auto-generated method stub
		
		try {
			
			System.out.println(line("*",40));
			System.out.println("SCHEMA_NAME");
			System.out.println(line("*",40));


			schemata.seek(0);
			int position=0;
			while(position<(int)schemata.length())
			{
				schemata.seek(position);
				String v="";
				int varcharLength = Integer.parseInt(Byte.toString(schemata.readByte()));
				position=position+1;
				for(int i = 0; i < varcharLength; i++)
				{
					v=v+(char)schemata.readByte();
					position=position+1;
				}
				
				
				System.out.println("|   "+v+"               ");


			}
			
			System.out.println(line("*",40));

			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		
		
		
	}
	
	String line(String s,int num) {
		String a = "";
		for(int i=0;i<num;i++) {
			a += s;
		}
		return a;
	}

	public void show_tables(String userCommand, RandomAccessFile tables) {
		// TODO Auto-generated method stub
try {
			
			System.out.println(line("*",60));
			System.out.println(" TABLE_SCHEMA     -    TABLE_NAME   -    TABLE_ROWS");
			System.out.println(line("*",60));


			tables.seek(0);
			int position=0;
			long ll=tables.length();
			int j=1;
			while(position<(int)tables.length())
			{
				
				tables.seek(position);
				String v="";
				int varcharLength = Integer.parseInt(Byte.toString(tables.readByte()));
				position=position+1;
				for(int i = 0; i < varcharLength; i++)
				{
					v=v+(char)tables.readByte();
					position=position+1;
				}
				
				
				System.out.print(" "+v+"        -");
				
				if(j==3)
				{
					j=0;
				System.out.println("");
				}
				j++;

			}
			
			System.out.println(line("*",60));
			
		} 
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
	}

	public void drop_table(String userCommand, RandomAccessFile tables) 
	{
		// TODO Auto-generated method stub
		
		
		try 
		{		
			
			String table_name = query_parser(userCommand,3);

			//System.out.println(table_name);
			String table_name_schema=table_name+" schema";
			String tn= schema_used+"."+table_name+".tbl";
			
			RandomAccessFile table_info = new RandomAccessFile(table_name_schema, "rw") ;
			RandomAccessFile table = new RandomAccessFile(tn, "rw") ;
			
			table_info.seek(0);
			
			Parser p =new Parser();
			Column[] cols= p.parser(table_info.readLine());
			
			table_info.close();
			
			File f = new File(table_name_schema);
			f.delete();
			
			File ff=new File(tn);
			ff.delete();
			
			for(int i=0 ; i<cols.length;i++)
			{
			String ind_name= schema_used+"."+table_name+"."+cols[i].column_name+".ndx";
			File fff=new File(ind_name);
			fff.delete();
			
			
			}
			
			
			
			delete_from_tables(tables,table_name,schema_used);
			
			
			System.out.println("Table has been dropped successfully");
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
	}

	private void delete_from_tables(RandomAccessFile tables, String table_name, String schema_used2) {
		// TODO Auto-generated method stub
		try 
		{
			tables.seek(0);
			List<List<String>> ll=new ArrayList<List<String>>();
				
				int position=0;
				Long asads= tables.length();
				
				while(position<(int)tables.length())
				{
					List<String> l= new ArrayList<String>();
					tables.seek(position);
					String sname="";
					String tname="";
					String rows="";
					int varcharLength = Integer.parseInt(Byte.toString(tables.readByte()));
					position=position+1;
					for(int i = 0; i < varcharLength; i++)
					{
						sname=sname+(char)tables.readByte();
						position=position+1;
					}
					
					tables.seek(position);
					
					varcharLength = Integer.parseInt(Byte.toString(tables.readByte()));
					position=position+1;
					for(int i = 0; i < varcharLength; i++)
					{
						tname=tname+(char)tables.readByte();
						position=position+1;
					}
					
					tables.seek(position);
					
					varcharLength = Integer.parseInt(Byte.toString(tables.readByte()));
					position=position+1;
					for(int i = 0; i < varcharLength; i++)
					{
						rows=rows+(char)tables.readByte();
						position=position+1;
					}
					
					
					
					
					if(!tname.equalsIgnoreCase(table_name)|| !sname.equalsIgnoreCase(schema_used2))
					{
						

						l.add(sname);
						l.add(tname);
						l.add(rows);
						
						ll.add(l);

					}
					
					
					
					
				}
				
				
				adding_list(ll,tables);
			
		}
		catch ( IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	
}
