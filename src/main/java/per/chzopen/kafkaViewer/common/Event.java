package per.chzopen.kafkaViewer.common;

public class Event
{

	private boolean flag = false;
	
	private boolean waitIfFalse = true;
	
	public Event()
	{
		this.flag = false;
	}
	
	public Event(Boolean flag)
	{
		this.flag = flag;
	}
	
	
	public synchronized boolean isSet()
	{
		return flag;
	}
	

	public synchronized boolean doWait(long timeout)
	{
		return waitIfFalse(timeout);
	}
	
	public synchronized boolean waitIfFalse(long timeout)
	{
		waitIfFalse = true;
		if( !flag )
		{
			justWait(timeout);
		}
		return flag;
	}

	public synchronized boolean waitIfTrue(long timeout)
	{
		waitIfFalse = false;
		if( flag )
		{
			justWait(timeout);
		}
		return flag;
	}
	
	private synchronized void justWait(long timeout)
	{
		try
		{
			if( timeout>0 )
			{
				this.wait(timeout);
			}
			else
			{
				this.wait();	// wait for ever
			}
		}
		catch (InterruptedException e)
		{
		}
	}
	
	public synchronized void clear()
	{
		flag = false;
		ifNotify();
	}
	
	public synchronized void set(boolean clear)
	{
		flag = clear ? false : true;
		ifNotify();
	}
	
	private synchronized void ifNotify()
	{
		if( waitIfFalse && flag )
		{
			this.notifyAll();
		}
		else if( !waitIfFalse && !flag )
		{
			this.notifyAll();
		}
	}
	
	public synchronized void set()
	{
		set(false);
	}
	
}
