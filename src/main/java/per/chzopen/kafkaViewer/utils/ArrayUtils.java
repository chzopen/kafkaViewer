package per.chzopen.kafkaViewer.utils;

import java.util.Arrays;
import java.util.List;

public class ArrayUtils
{

	public static String join(Object[] ary, String sep, String beginning, String ending)
	{
		return join(Arrays.asList(ary), sep, beginning, ending);
	}
	
	public static String join(List<? extends Object> list, String sep, String beginning, String ending)
	{
		StringBuilder sb = new StringBuilder();
		if( beginning!=null )
		{
			sb.append(beginning);
		}
		int count = 0;
		for( Object obj : list )
		{
			if( obj==null )
			{
				continue;
			}
			if( count>0 )
			{
				sb.append(sep);
			}
			count++;
			sb.append(obj);
		}
		if( ending!=null )
		{
			sb.append(ending);
		}
		return sb.toString();
	}
	
	public static String join(Object[] ary)
	{
		return join(ary, ",", null, null);
	}

	public static String join(List<? extends Object> list)
	{
		return join(list, ",", null, null);
	}
	
	public static void main(String[] args)
	{
		System.out.println(join(new Object[]{1,2,3,4}));
	}
	
}
