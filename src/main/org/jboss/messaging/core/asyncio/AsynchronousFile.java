/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
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
	
	ByteBuffer newBuffer(int size);
	
	void setBufferCallback(BufferCallback callback);
	
	int getBlockSize();
	
	String getFileName();
	
}
