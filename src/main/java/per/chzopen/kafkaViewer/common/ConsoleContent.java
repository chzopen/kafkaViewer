package per.chzopen.kafkaViewer.common;

import java.util.LinkedList;

public class ConsoleContent
{

	private int rows;
	private LinkedList<String> lines = new LinkedList<>();
	
	public ConsoleContent()
	{
		this.rows = 1000;
	}
	
	public ConsoleContent(int rows)
	{
		this.rows = rows;
	}
	
	public void addLine(String line)
	{
		if( line==null )
		{
			return ;
		}
		int begin = 0;
		int index = line.indexOf("\r\n");
		while( index>=0 )
		{
			String str = line.substring(begin, index);
			lines.add(str);
			begin = index + 2;
			index = line.indexOf("\r\n", begin);
		}
		lines.add(line.substring(begin));
		while( lines.size()>rows )
		{
			lines.removeFirst();
		}
	}
	
	public void clear()
	{
		lines.clear();
	}
	
	public StringBuilder toStringBuilder()
	{
		StringBuilder sb = new StringBuilder();
		for( String line : lines )
		{
			sb.append(line).append("\r\n");
		}
		return sb;
	}
	
	public String toString()
	{
		return toStringBuilder().toString();
	}
	
	public static void main(String[] args)
	{
		ConsoleContent cc = new ConsoleContent(100);
		for( int i=0; i<100; i++ )
		{
			cc.addLine(""+i);
		}
		cc.addLine("1aaaaaa1\r\n2bbbbbbbbb2\r\n\r\n3cccccccc3");
		System.out.println(cc.toStringBuilder());
	}
}
