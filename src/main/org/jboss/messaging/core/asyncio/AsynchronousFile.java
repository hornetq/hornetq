/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.messaging.core.asyncio;

import java.nio.ByteBuffer;

/**
 * 
 * @author clebert.suconic@jboss.com
 *
 */
public interface AsynchronousFile
{	
	void close() throws Exception;
	
	/**
	 * 
	 * Note: If you are using a native Linux implementation, maxIO can't be higher than what's defined on /proc/sys/fs/aio-max-nr, or you would get an error 
	 * @param fileName
	 * @param maxIO The number of max concurrent asynchrnous IO operations. It has to be balanced between the size of your writes and the capacity of your disk.
	 */
	void open(String fileName, int maxIO);
	
	/** 
	 * Warning: This function will perform a synchronous IO, probably translating to a fstat call
	 * */
	long size();
	
	void write(long position, long size, ByteBuffer directByteBuffer, AIOCallback aioPackage);
	
	void read(long position, long size, ByteBuffer directByteBuffer,  AIOCallback aioPackage);
	
	void fill(long position, int blocks, long size, byte fillChar);
	
	ByteBuffer newBuffer(long size);
	
	void destroyBuffer(ByteBuffer buffer);
	
	int getBlockSize();
	
}
