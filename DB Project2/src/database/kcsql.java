package database;

import java.io.RandomAccessFile;
import java.io.File;
import java.io.FileReader;
import java.util.Scanner;
import java.util.SortedMap;





public class kcsql 
{
	public static void main(String[] args) {
		/* Display the welcome splash screen */
		splashScreen();
		
		
		Driver d = new Driver();
		d.start();

    } /* End main() method */
	
	
	
	static void splashScreen()
	{
		System.out.println(line("*",80));
        System.out.println("Welcome to KrishnaBase"); // Display the string.
        System.out.println("KrishnaBase v1.0"); // Display the string.
		System.out.println("Type \"help;\" to display supported commands.");
		System.out.println(line("*",80));
	}
	static String line(String s,int num) {
		String a = "";
		for(int i=0;i<num;i++) {
			a += s;
		}
		return a;
	}
	
	

}
