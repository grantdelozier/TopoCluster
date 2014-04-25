import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;
import edu.stanford.nlp.ie.crf.CRFClassifier;

public class Parser {

	/**
	 * @param args
	 */
	public static Map<Long, String> id_coord = new HashMap<Long, String>();
	public static BufferedWriter writer = null;
	public static final String[] ARGUMENTS = new String[]{"-loadClassifier", 
        "C:\\Users\\Lori\\Documents\\CS388\\SemesterProject\\stanford-ner-2014-01-04\\classifiers\\english.conll.4class.distsim.crf.ser.gz",
        "-textFile",
        //"temp_file.txt",
        "C:\\Users\\Lori\\Documents\\CS388\\SemesterProject\\TopCluster\\txt_file\\Puzhuthivakkam.txt",
        "-inputEncoding",
        "utf-8",
        "-outputEncoding",
        "utf-8",
        "-outputFormat",
        "slashTags"
        };
	static int proportion = 0;
	public static void main(String[] args) throws Exception {
		writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("C:\\Users\\" +
				"Lori\\Documents\\CS388\\SemesterProject\\TopCluster\\" +
				"wiki_training_file.txt"), "UTF-8"));
		
		init_coord();
		addFile("Lori", (long)78);
		writer.close();
//		String encoding = "UTF-8";
//		BufferedReader reader = null;
//		reader = new BufferedReader(new InputStreamReader(new FileInputStream( "C:\\Users\\Lori\\" +
//				"Documents\\CS388\\SemesterProject\\TopCluster\\Corpora" +
//				"\\enwiki-20130102\\enwiki-20130102-permuted-text-only-coord-documents.txt"), encoding));
//		String article_title = "";
//		Long id = (long)-1;
//		BufferedWriter temp_file = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("temp_file.txt"), encoding));
//		int count = 0;
//		int articles = 0;
//		long time = System.currentTimeMillis();
//		try{
//			while(true){
//			String line = reader.readLine();
//			if(line.equals("\n"))
//			{
//				System.err.println("****** newline new File?");
//			}
//			String[] arr = line.split(":");
//			if(arr.length > 1)
//			{
//				if(arr[0].equals("Article title"))
//				{
//					if(article_title.isEmpty())
//						article_title = line.split(":")[1].trim();
//					else
//					{
//						temp_file.close();
//						addFile(article_title, id);
//						
//						temp_file = new BufferedWriter(new OutputStreamWriter(
//								new FileOutputStream("temp_file.txt"), encoding));
//						article_title = line.split(":")[1].trim();
//						System.err.println("----------- Count: "+count);
//					}
//				}
//				else if(arr[0].equals("Article ID"))
//				{
//					id = Long.parseLong(line.split(":")[1].trim());
//				}
//			}
//			else
//			{
//				temp_file.write(new String(line.getBytes(), "UTF-8"));
//				temp_file.write("\n");
//			}
//			articles++;
//			count++;
//			//if(count > 5000)
//				//break;
//			}
//		}
//		catch(Exception e)
//		{
//			
//		}
//		temp_file.close();
//		addFile(article_title, id);
//		reader.close();
//		writer.close();
//		System.err.println("===================Time: " +  (System.currentTimeMillis()-time)/1000.0 + " Proportion of non-label: " + proportion*1.0/(id_coord.size()*1.0));
	}
	@SuppressWarnings("rawtypes")
	public static void addFile(String name, Long id) throws Exception
	{
//		if(id_coord.get(id) == null)
//		{
//			System.out.println("Grant... not all of the data has coordinates :(");
//			System.out.println("Name: " + name + ", ID: " + id);
//			System.out.println();
//			//throw new Exception("Grant... not all of the data has coordinates :, no record of id is in there...." + "Name: " + name + ", ID: " + id);
//			System.err.println("**********************Grant... not all of the data has coordinates :, no record of id is in there...." + "Name: " + name + ", ID: " + id);
//			proportion++;
//			return;
//		}
		writer.append(id.toString());
		writer.append("\t");
		writer.append(name);
		writer.append("\t");
		System.err.println(name + " " + id);
		writer.append(id_coord.get(id));
		writer.append("\t?\t?\t?\t?\t?\t");
		File file = new File("output_temp.txt");
		FileOutputStream fos = new FileOutputStream(file);
		//BufferedOutputStream x = new BufferedOutputStream(x, proportion);//(new FileOutputStream(file), "UTF-8");
		PrintStream ps = new PrintStream(fos, false, "utf-8");
		System.setOut(ps);
		CRFClassifier.main(ARGUMENTS);
		ps.close();
		//file.close();
		create_dictionary();
		
	}
	public static void create_dictionary() throws IOException
	{
		boolean found = false;
		StringBuffer entity = new StringBuffer();
		Map<String, Integer> dictionary = new HashMap<String, Integer>();
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("output_temp.txt"), "UTF-8"));
		try{
			while(true){
				
			String line = reader.readLine();
		    String[] token = line.trim().split(" ");
		    for(String item :token){ 
		    	String[] arr = item.split("/");
		        if(arr.length > 1 && !arr[1].equals("O")){
		        	String UTF = arr[0];
		        	if( found == true)
		        		entity = entity.append("|").append(UTF);
		        	else{
		        		entity = new StringBuffer(UTF);
		        		found = true;
		        	}
		        }
		        else if( arr.length > 1){
		        	if(found == true){
		        		Integer val = dictionary.get(entity);
		        		if(val != null)
		        			dictionary.put(new String(entity.toString().getBytes(), "UTF-8"), val+1);
		        		else
		        			dictionary.put(new String(entity.toString().getBytes(), "UTF-8"), 1);
		        		found = false;
		        	}
		        	String UTF = arr[0];
		        	Integer val = dictionary.get(UTF);
		        	if(val != null)
		        		dictionary.put(UTF, val + 1);
		        	else
		        		dictionary.put(UTF, 1);
		        }
		    }
		    }
		}
		catch(Exception e)
		{
			PrintStream console = System.out;
			System.setOut(console);
			console.close();
			System.out.println("\n\n\nComplete");
			
		}
		reader.close();
		for(Map.Entry<String, Integer>entry : dictionary.entrySet())
		{
			
			writer.append(entry.getKey());
			writer.append(":");
			writer.append(entry.getValue().toString());
			writer.append(" ");
		}
		writer.append("\n");
	}
	public static void init_coord() throws IOException
	{
		String encoding = "UTF-8";
		BufferedReader reader = null;
		reader = new BufferedReader(new InputStreamReader(new FileInputStream( "C:\\Users\\Lori\\" +
				"Documents\\CS388\\SemesterProject\\TopCluster\\Corpora" +
				"\\enwiki-20130102\\enwiki-20130102-permuted-coords.txt"), encoding));
		try{
			while(true)
			{
				reader.readLine();
				
				Long id = Long.parseLong(reader.readLine().split(":")[1].trim());
				String coordinate = reader.readLine().split(":")[1].trim();
				id_coord.put(id, coordinate);
			}
		}
		catch(Exception e)
		{
			
		}
		reader.close();
		System.out.println(id_coord.size());
		System.out.println("Success on loading cordinates...");
	}
}
