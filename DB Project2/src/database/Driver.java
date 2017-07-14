package database;


import java.io.RandomAccessFile;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;
import java.util.SortedMap;







public class Driver 
{
	
	 public  RandomAccessFile schemata;
	 public  RandomAccessFile tables;
	 public  RandomAccessFile columns;
	 
	 public static String default_schema="information_schema";
	
	
	
	public Driver()
	{
		try {
			this.schemata= new RandomAccessFile("information_schema.schemata.tbl","rw");
			this.tables= new RandomAccessFile("information_schema.tables.tbl","rw");
			this.columns= new RandomAccessFile("information_schema.columns.tbl","rw");
			
			schemata.writeByte("information_schema".length());
			schemata.writeBytes("information_schema");
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	void start()
	{
		
		Scanner scanner = new Scanner(System.in).useDelimiter(";");
		
		
		
		String userCommand; // Variable to collect user input from the prompt
		
		
		
		//userCommand="use schema";
		
		
		
		do {  // do-while !exit
			System.out.print("kcSql>>");
			userCommand = scanner.next().trim();
			
			//userCommand="show schemas";
			//userCommand="show tables";
			//userCommand="use zoo";
			//userCommand="create schema zoo";
			//userCommand="CREATE TABLE table (id int primary key, name varchar(25),quantity date,  probability float );";
			//userCommand="INSERT INTO TABLE table VALUES (1,'krishna','20-21-2016',33); ";
			//userCommand="select * from table where id=1 ;";
			//userCommand="drop table tasaable ;";

			//SELECT * FROM table WHERE quantity<'1995-01-01' ;


			
			
			if(userCommand.toLowerCase().contains("help"))
				help();
			
			else if(userCommand.toLowerCase().contains("use"))
			{
				Table t=new Table(default_schema);
				default_schema=t.use_schema(userCommand);
				System.out.println("A new Schema is in use");
			}
			else if(userCommand.toLowerCase().contains("show")&& userCommand.toLowerCase().contains("schemas"))
			{
				Table t=new Table(default_schema);
				t.show_schema(userCommand,schemata);
				//System.out.println("A new Schema has been created");
			}
			else if(userCommand.toLowerCase().contains("show")&& userCommand.toLowerCase().contains("tables"))
			{
				try {
					if(tables.length()==0)
					{
						System.out.println("No Tables records exist");
					}
					else
					{
						Table t=new Table(default_schema);
						t.show_tables(userCommand,tables);
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				//
			}
			
			else if(userCommand.toLowerCase().contains("drop")&& userCommand.toLowerCase().contains("table"))
			{
				Table t=new Table(default_schema);
				t.drop_table(userCommand,tables);
				
				
				
			}

			else if(userCommand.toLowerCase().contains("create")&& userCommand.toLowerCase().contains("schema"))
			{
				Table t=new Table(default_schema);
				t.create_schema(userCommand,schemata);
				System.out.println("A new Schema has been created");
			}


			else if(userCommand.toLowerCase().contains("create table"))
			{
				Table t=new Table(default_schema);
				t.create_table(userCommand,tables);
				System.out.println("A new empty Table has been created");
			
			}
			else if (userCommand.toLowerCase().contains("insert into table"))
			{
				Table t=new Table(default_schema);
				t.insert_table(userCommand,tables,columns);
				
				//Test t= new Test();
				//t.parse_index_file();
				
				//System.out.println("Record has been successfully inserted");
			}
			else if (userCommand.toLowerCase().contains("select"))
			{
				Table t=new Table(default_schema);
				t.query(userCommand);

				//Test t= new Test();
				//t.parse_index_file();
				
			
			}
			else
			{
				if(!userCommand.toLowerCase().contains("exit"))
				{
				System.out.println("Invalid command. Press 'help' for correct syntax support.");
				}
			}
		} 
	while(!userCommand.equalsIgnoreCase("exit"));
		System.out.println("Exiting...");
		
	}
	
	
	
	 void help() 
	{
		System.out.println(line("*",80));
		System.out.println();
		System.out.println("\tSHOW SCHEMAS;       Display all schemas defined in the database");
		System.out.println("\tUSE;     		      Chooses a schema");
		System.out.println("\tSHOW TABLES;        Show the program version.");
		System.out.println("\tCREATE SCHEMA;      Creates a new schema to hold tables");
		System.out.println("\tCREATE TABLE table_name;       Creates a new table schema,i.e, a new empty table");
		System.out.println("\tINSERT INTO TABLE table_name VALUES (value1,'string_value','date_value',value2) ;  Inserts a row/record into a table");
		System.out.println("\tDROP TABLE table_name;         Remove a table schema, and all of its contained data");
		System.out.println("\tSELECT FROM WHERE;  Style query");
		System.out.println("\texit;               Exit the program and saves all table & index information");
		System.out.println();
		System.out.println();
		System.out.println(line("*",80));
	}
		
	 String line(String s,int num) {
			String a = "";
			for(int i=0;i<num;i++) {
				a += s;
			}
			return a;
		}
		
	
	
	

}
