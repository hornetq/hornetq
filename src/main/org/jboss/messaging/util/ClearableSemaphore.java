package org.jboss.messaging.util;

import java.util.concurrent.Semaphore;

import org.jboss.logging.Logger;

/**
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: $</tt>20 Oct 2007
 *
 * $Id: $
 *
 */
public class ClearableSemaphore
{
   protected Logger log = Logger.getLogger(ClearableSemaphore.class);
	
	private volatile Semaphore semaphore;
	
	private int permits;
	
	private void createSemaphore()
	{
		semaphore = new Semaphore(permits, true);
	}
	
	public ClearableSemaphore(int permits)
	{				
		this.permits = permits;
		
		createSemaphore();
	}
	
	public void acquire() throws InterruptedException
	{
		Semaphore sem = semaphore;
		
		if (sem != null)
		{
			sem.acquire();
		}
	}
	
	public void release()
	{
		Semaphore sem = semaphore;
		
		if (sem != null)
		{
			sem.release();
		}
	}
	
	public synchronized void reset()
	{
		if (semaphore != null)
		{
			Semaphore oldSem = semaphore;
			
			createSemaphore();
			
			oldSem.release(permits);
		}
	}
}
