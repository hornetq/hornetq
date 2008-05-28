/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.messaging.core.journal.impl;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.TimeUnit;

import org.jboss.messaging.core.asyncio.AsynchronousFile;
import org.jboss.messaging.core.asyncio.impl.AsynchronousFileImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.journal.IOCallback;
import org.jboss.messaging.core.journal.SequentialFile;
import org.jboss.messaging.core.logging.Logger;

/**
 * 
 * A AIOSequentialFile
 * 
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class AIOSequentialFile implements SequentialFile
{
   private static final Logger log = Logger.getLogger(AIOSequentialFile.class);

   private final String journalDir;
   
	private final String fileName;
	
	private boolean opened = false;
	
	private final int maxIO;
	
	private final long timeout;
	
	private AsynchronousFile aioFile;
	
	private AtomicLong position = new AtomicLong(0);

	// A context switch on AIO would make it to synchronize the disk before switching to the new thread, what would cause
	// serious performance problems. Because of that we make all the writes on AIO using a single thread.
	private ExecutorService executor;
	
	public AIOSequentialFile(final String journalDir, final String fileName, final int maxIO, final long timeout) throws Exception
	{
		this.journalDir = journalDir;		
		this.fileName = fileName;
		this.maxIO = maxIO;
		this.timeout = timeout;
	}
	
	public int getAlignment() throws Exception
	{
		checkOpened();
		
		return aioFile.getBlockSize();
	}
	
	public int calculateBlockStart(int position) throws Exception
	{
		int alignment = getAlignment();
		
		int pos = ((position / alignment) + (position % alignment != 0 ? 1 : 0)) * alignment;
		
		return pos;
	}
			
	public synchronized void close() throws Exception
	{
		checkOpened();
		opened = false;
      executor.shutdown();
      if (!executor.awaitTermination(timeout, TimeUnit.MILLISECONDS))
      {
         try
         {
            aioFile.close();
         }
         catch (Exception ignored)
         {
         }
         throw new Exception("Timeout!");
      }
		aioFile.close();
		aioFile = null;		
	}
	
	public void delete() throws Exception
	{
		if (aioFile != null)
		{
			aioFile.close();
			aioFile = null;
		}
		
		File file = new File(journalDir + "/" +  fileName);
		file.delete();
	}
	
	public void fill(int position, final int size, final byte fillCharacter) throws Exception
	{
		checkOpened();
		
		int blockSize = aioFile.getBlockSize();
		
      if (size % (100*1024*1024) == 0)
      {
         blockSize = 100*1024*1024;
      }
      if (size % (10*1024*1024) == 0)
      {
         blockSize = 10*1024*1024;
      }
		else if (size % (1024*1024) == 0)
		{
			blockSize = 1024*1024;
		}
		else if (size % (10*1024) == 0)
		{
			blockSize = 10*1024;
		}
		else
		{
			blockSize = aioFile.getBlockSize();
		}
		
		int blocks = size / blockSize;
		
		if (size % blockSize != 0)
		{
			blocks++;
		}
		
		if (position % aioFile.getBlockSize() != 0)
		{
			position = ((position / aioFile.getBlockSize()) + 1) * aioFile.getBlockSize();
		}
		
		aioFile.fill((long)position, blocks, blockSize, (byte)fillCharacter);		
	}
	
	public String getFileName()
	{
		return fileName;
	}
	
	public synchronized void open() throws Exception
	{
	   opened = true;
	   executor = Executors.newSingleThreadExecutor();
		aioFile = new AsynchronousFileImpl();
		aioFile.open(journalDir + "/" + fileName, maxIO, timeout);
		position.set(0);
		
	}
	
	public void position(final int pos) throws Exception
	{
		position.set(pos);		
	}
	
	public int read(final ByteBuffer bytes, final IOCallback callback) throws Exception
	{
		int bytesToRead = bytes.limit();
		
		long positionToRead = position.getAndAdd(bytesToRead);
		
		bytes.rewind();
		
		aioFile.read(positionToRead, bytesToRead, bytes, callback);
		
		return bytesToRead;
	}
	
	public int read(final ByteBuffer bytes) throws Exception
	{
		WaitCompletion waitCompletion = new WaitCompletion();
		
		int bytesRead = read (bytes, waitCompletion);
		
		waitCompletion.waitLatch();
		
		if (waitCompletion.errorMessage != null)
		{
			throw new MessagingException(waitCompletion.errorCode, waitCompletion.errorMessage);
		}
		
		return bytesRead;
	}
	
	
	public int write(final ByteBuffer bytes, boolean sync, final IOCallback callback) throws Exception
	{
		final int bytesToWrite = bytes.limit();
		
		final long positionToWrite = position.getAndAdd(bytesToWrite);
		
		execWrite(bytes, callback, bytesToWrite, positionToWrite);
		
		return bytesToWrite;
	}

   private void execWrite(final ByteBuffer bytes, final IOCallback callback,
                          final int bytesToWrite, final long positionToWrite)
   {
      executor.execute(new Runnable()
		{
		   public void run()
		   {
		      try
		      {
		         aioFile.write(positionToWrite, bytesToWrite, bytes, callback);
		      }
		      catch (Exception e)
		      {
		         log.warn (e.getMessage(), e);
		         if (callback != null)
		         {
		            callback.onError(-1, e.getMessage());
		         }
		      }
		   }
		});      
   }
	
	public int write(final ByteBuffer bytes, final boolean sync) throws Exception
	{
		return write (bytes, sync, DummyCallback.instance);
	}
	
	private void checkOpened() throws Exception
	{
		if (aioFile == null || !opened)
		{
			throw new IllegalStateException ("File not opened");
		}
	}

	private static class DummyCallback implements IOCallback
	{	   
	   static DummyCallback instance = new DummyCallback();

      public void done()
      {
      }

      public void onError(int errorCode, String errorMessage)
      {
      }	   
	}
	
	private static class WaitCompletion implements IOCallback
	{		
		CountDownLatch latch = new CountDownLatch(1);
		
		String errorMessage;
		
		int errorCode = 0;
		
		public void done()
		{
			latch.countDown();
		}
		
		public void onError(final int errorCode, final String errorMessage)
		{
			this.errorCode = errorCode;
			
			this.errorMessage = errorMessage;
			
			latch.countDown();			
		}
		
		public void waitLatch() throws Exception
		{
			latch.await();
		}		
	}	
}
