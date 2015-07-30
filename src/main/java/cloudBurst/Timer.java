package cloudBurst;

public class Timer {
	
	public long starttime;
	public long endtime;
	
	public Timer()
	{
		starttime = System.currentTimeMillis();
	}
	
	public double get()
	{
		endtime = System.currentTimeMillis();
		return (endtime - starttime) / 1000.0;
	}

}
