package database;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Parser 
{
	Column[] parser(String s)
	{
		String s1;
		s1=s.substring(s.indexOf("(") + 1);
		String[] aa=s1.split(",");
		int l=aa.length;
		
		Column[] cols = new Column[l];
		
		for(int i=0; i<l ;i++)
		{
			int column_no=i+1;

			String column_type= find_type(aa[i]);
			
			boolean primary_key;
			if(aa[i].toLowerCase().contains("primary") || aa[i].toLowerCase().contains("notnull") ||aa[i].toLowerCase().contains("not null") )
				 primary_key=true;
			else
				 primary_key=false;
			
			String column_name=name(aa[i]);
			
			cols[i]= new Column(column_no,column_name,column_type,primary_key);
			
			
		}
		return cols;

		//coulumn_no,column_name,column_type,primary_key

		
	}
	public String name(String s)
	{
		
		String[] aa =s.split(" ");
		String r="no_id";
		Pattern p = Pattern.compile("[a-zA-Z]");
		Matcher m ;
		
		for(int i=0; i<aa.length; i++)
		{
			m=p.matcher(aa[i]);
			if(m.find())
			{
				r=aa[i];
				break;
			}
			
			
		}
		return r;
		
		
		
	}
	public  String find_type(String s)
	{
		if(s.toLowerCase().contains("byte"))
				return "byte";
		else if(s.toLowerCase().contains("short"))
			return "short";
		else if(s.toLowerCase().contains("long"))
			return "long";
		else if(s.toLowerCase().contains("int"))
			return "int";
		else if(s.toLowerCase().contains("varchar"))
			return "varchar";
		else if(s.toLowerCase().contains("char"))
			return "char";
		else if(s.toLowerCase().contains("float"))
			return "float";
		else if(s.toLowerCase().contains("double"))
			return "double";
		else if(s.toLowerCase().contains("date"))
			return "date";
		else 
			return "datetime";
		
	
		
		
	}

}
class Column
{
	 int column_no;
	 String column_name;
	 String column_type;
	 boolean primary_key;
	
	
	public Column(int a, String b, String c, boolean t)
	{
		this.column_no=a;
		this.column_name=b;
		this.column_type=c;
		this.primary_key=t;
	}
	
	
	
}
