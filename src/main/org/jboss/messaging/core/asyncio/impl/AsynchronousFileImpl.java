/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.messaging.core.asyncio.impl;

import java.nio.ByteBuffer;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.jboss.messaging.core.asyncio.AIOCallback;
import org.jboss.messaging.core.asyncio.AsynchronousFile;
import org.jboss.messaging.core.logging.Logger;

/**
 * 
 * @author clebert.suconic@jboss.com
 * Warning: Case you refactor the name or the package of this class
 *          You need to make sure you also rename the C++ native calls
 */
public class AsynchronousFileImpl implements AsynchronousFile
{
	private static Logger log = Logger.getLogger(AsynchronousFileImpl.class);
	private boolean opened = false;
	private String fileName;
	private Thread poller;
	private static boolean loaded = false;
	private int maxIO;
	
	Semaphore writeSemaphore;
	
	ReadWriteLock lock = new ReentrantReadWriteLock();
	Lock writeLock = lock.writeLock();
	
	Semaphore pollerSemaphore = new Semaphore(1);
	
	/**
	 *  Warning: Beware of the C++ pointer! It will bite you! :-)
	 */ 
	private long handler;
	
	private static boolean loadLibrary(String name) 
	{
		try
		{
			log.trace(name + " being loaded");
			System.loadLibrary(name);
			return isNativeLoaded();
		}
		catch (Throwable e)
		{
			log.trace(name + " -> error loading it", e);
			return false;
		}
		
	}
	
	static
	{
		String libraries[] = new String[] {"JBMLibAIO", "JBMLibAIO32", "JBMLibAIO64"};
		
		
		for (String library: libraries)
		{
			if (loadLibrary(library))
			{
				loaded = true;
				break;
			}
			else
			{
				log.debug("Library " + library + " not found!");
			}
		}
		
		if (!loaded)
		{
			log.debug("Couldn't locate LibAIO Wrapper");
		}
	}
	
	public static boolean isLoaded()
	{
		return loaded;
	}
	
	
	
	
	public void open(String fileName, int maxIO)
	{
		try
		{
			writeLock.lock();
			this.maxIO = maxIO;
			
			this.writeSemaphore = new Semaphore(maxIO);
			
			if (opened)
			{
				throw new IllegalStateException("AsynchronousFile is already opened");
			}
			opened = true;
			this.fileName=fileName;
			handler = init (fileName, maxIO, log);
			startPoller();
		}
		finally
		{
			writeLock.unlock();
		}
	}
	
	class PollerThread extends Thread
	{
		PollerThread ()
		{
			super("NativePoller for " + fileName);
		}
		public void run()
		{
			// informing caller that this thread already has the lock
			try
			{
				pollEvents();
			}
			finally
			{
				pollerSemaphore.release();
			}
		}
	}
	
	public void close() throws Exception
	{
		checkOpened();
		
		writeLock.lock();
		writeSemaphore.acquire(maxIO);
		stopPoller(handler);
		// We need to make sure we won't call close until Poller is completely done, or we might get beautiful GPFs
		try
		{
			pollerSemaphore.acquire();
			closeInternal(handler);
			opened = false;
			handler = 0;
		}
		finally
		{
			writeLock.unlock();
			pollerSemaphore.release();
		}
	}
	
	
	public void write(long position, long size, ByteBuffer directByteBuffer, AIOCallback aioPackage)
	{
		checkOpened();
		this.writeSemaphore.acquireUninterruptibly();
		try
		{
			write (handler, position, size, directByteBuffer, aioPackage);
		}
		catch (RuntimeException e)
		{
			writeSemaphore.release();
			throw e;
		}
		
	}
	
	public void read(long position, long size, ByteBuffer directByteBuffer, AIOCallback aioPackage)
	{
		checkOpened();
		this.writeSemaphore.acquireUninterruptibly();
		try
		{
			read (handler, position, size, directByteBuffer, aioPackage);
		}
		catch (RuntimeException e)
		{
			writeSemaphore.release();
			throw e;
		}
		
	}
	
	public long size()
	{
		checkOpened();
		// TODO: wire this method to ftell
		return 0;
	}
	
	public void fill(long position, int blocks, long size, byte fillChar)
	{
		checkOpened();
		fill(handler, position, blocks, size, fillChar);
	}
	
	public int getBlockSize()
	{
		return 512;
	}
	
	
	/** The JNI layer will call this method, so we could use it to unlock readWriteLocks held in the java layer */
	@SuppressWarnings("unused") // Called by the JNI layer.. just ignore the warning
	private void callbackDone(AIOCallback callback)
	{
		writeSemaphore.release();
		callback.done();
	}
	
	@SuppressWarnings("unused") // Called by the JNI layer.. just ignore the warning
	private void callbackError(AIOCallback callback, int errorCode, String errorMessage)
	{
		writeSemaphore.release();
		callback.onError(errorCode, errorMessage);
	}
	
	private void pollEvents()
	{
		if (!opened)
		{
			return;
		}
		internalPollEvents(handler);
	}
	
	private synchronized void  startPoller()
	{
		checkOpened();
		
		poller = new PollerThread(); 
		try
		{
			this.pollerSemaphore.acquire();
			poller.start();
		}
		catch (Exception ex)
		{
			log.error(ex.getMessage(), ex);
		}
	}
	
	
	
	private void checkOpened() 
	{
		if (!opened)
		{
			throw new RuntimeException("File is not opened");
		}
	}
	
	/** 
	 * I'm sending aioPackageClazz here, as you could have multiple classLoaders with the same class, and I don't want the hassle of doing classLoading in the Native layer
	 */
	@SuppressWarnings("unchecked")
	private static native long init(String fileName, int maxIO, Logger logger);
	
	private native void write(long handle, long position, long size, ByteBuffer buffer, AIOCallback aioPackage);
	
	private native void read(long handle, long position, long size, ByteBuffer buffer, AIOCallback aioPackage);
	
	private static native void fill(long handle, long position, int blocks, long size, byte fillChar);
	
	private static native void closeInternal(long handler);
	
	private static native void stopPoller(long handler);
	
	/** A native method that does nothing, and just validate if the ELF dependencies are loaded and on the correct platform as this binary format */
	private static native boolean isNativeLoaded();
	
	/** Poll asynchrounous events from internal queues */
	private static native void internalPollEvents(long handler);
	
	// Should we make this method static?
	public native void destroyBuffer(ByteBuffer buffer);
	
	// Should we make this method static?
	public native ByteBuffer newBuffer(long size);
	
	
	
	
	
}
