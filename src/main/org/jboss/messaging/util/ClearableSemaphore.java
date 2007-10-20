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
	
	private Semaphore semaphore;
	
	private boolean enabled;
	
	private int permits;
	
	private void createSemaphore()
	{
		semaphore = new Semaphore(permits, true);
		
		enabled = true;
	}
	
	public ClearableSemaphore(int permits)
	{				
		this.permits = permits;
		
		createSemaphore();
	}
	
	public synchronized void acquire() throws InterruptedException
	{
		if (enabled)
		{
			semaphore.acquire();
		}
	}
	
	public synchronized void release()
	{
		if (enabled)
		{
			semaphore.release();
		}
	}
	
	public synchronized void enable()
	{
		if (!enabled)
		{
			createSemaphore();
		}
	}
	
	// We need to be able to disable the semaphore since during failover requests may be sent but responses
	// may not come back (node is dead or hasn't loaded it's queues yet)
	// In which case we don't want to acquire a token since we might end up locking and using up all permits	
	public synchronized void disable()
	{
		if (enabled)
		{
			semaphore.release(permits);
			
			semaphore = null;
			
			enabled = false;
		}
	}
}
