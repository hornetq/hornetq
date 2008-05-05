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
import java.util.concurrent.atomic.AtomicLong;

import org.jboss.messaging.core.asyncio.AsynchronousFile;
import org.jboss.messaging.core.asyncio.impl.AsynchronousFileImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.journal.IOCallback;
import org.jboss.messaging.core.journal.SequentialFile;

public class AIOSequentialFile implements SequentialFile
{
	
	String journalDir;
	String fileName;
	
	AsynchronousFile aioFile;
	
	AtomicLong position = new AtomicLong(0);
	
	public AIOSequentialFile(String journalDir, String fileName) throws Exception
	{
		this.journalDir = journalDir;
		this.fileName = fileName;
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
	
	
	
	public void close() throws Exception
	{
		checkOpened();
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
	
	public void fill(int position, int size, byte fillCharacter)
	throws Exception
	{
		checkOpened();
		
		int blockSize = aioFile.getBlockSize();
		
		if (size % (10*1024*1024) == 0)
		{
			blockSize = 10*1024*1024;
		}
		else
			if (size % (1024*1024) == 0)
			{
				blockSize = 1024*1024;
			}
			else
				if (size % (10*1024) == 0)
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
		//System.out.println("filling " + blocks + " blocks with blockSize=" + blockSize + " on file=" + this.getFileName());
		aioFile.fill((long)position, blocks, blockSize, (byte)fillCharacter);
		
	}
	
	public String getFileName()
	{
		return fileName;
	}
	
	public void open() throws Exception
	{
		aioFile = new AsynchronousFileImpl();
		aioFile.open(journalDir + "/" + fileName, 1000);
		position.set(0);
		
	}
	
	public void position(int pos) throws Exception
	{
		position.set(pos);
		
	}
	
	public int read(ByteBuffer bytes, IOCallback callback) throws Exception
	{
		int bytesToRead = bytes.limit();
		long positionToRead = position.getAndAdd(bytesToRead);
		
		bytes.rewind();
		aioFile.read(positionToRead, bytesToRead, bytes, callback);
		
		return bytesToRead;
	}
	
	public int read(ByteBuffer bytes) throws Exception
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
	
	public int write(ByteBuffer bytes, boolean sync, IOCallback callback)
	throws Exception
	{
		int bytesToWrite = bytes.limit();
		long positionToWrite = position.getAndAdd(bytesToWrite);
		
		aioFile.write(positionToWrite, bytesToWrite, bytes, callback);
		return bytesToWrite;
	}
	
	public int write(ByteBuffer bytes, boolean sync) throws Exception
	{
		WaitCompletion waitCompletion = new WaitCompletion();
		int bytesWritten = write (bytes, sync, waitCompletion);
		
		waitCompletion.waitLatch();
		
		if (waitCompletion.errorMessage != null)
		{
			throw new MessagingException(waitCompletion.errorCode, waitCompletion.errorMessage);
		}
		
		return bytesWritten;
	}
	
	private void checkOpened() throws Exception
	{
		if (aioFile == null)
		{
			throw new IllegalStateException ("File not opened");
		}
	}
	
	class WaitCompletion implements IOCallback
	{
		
		CountDownLatch latch = new CountDownLatch(1);
		
		String errorMessage;
		int errorCode = 0;
		
		public void done()
		{
			latch.countDown();
		}
		
		public void onError(int errorCode, String errorMessage)
		{
			System.out.println("OK Error!");
			this.errorCode = errorCode;
			this.errorMessage = errorMessage;
			
			latch.countDown();
			
		}
		
		public void waitLatch() throws Exception
		{
			latch.await();
		}
		
	}
	
	public ByteBuffer newBuffer(int size)
	{
		if (size % aioFile.getBlockSize() != 0)
		{
			size = ((size / aioFile.getBlockSize()) + 1) * aioFile.getBlockSize();
		}
		return ByteBuffer.allocateDirect(size);
	}
	
	public ByteBuffer wrapBuffer(byte[] bytes)
	{
		ByteBuffer newbuffer = newBuffer(bytes.length);
		newbuffer.put(bytes);
		return newbuffer;
	};
	
}
