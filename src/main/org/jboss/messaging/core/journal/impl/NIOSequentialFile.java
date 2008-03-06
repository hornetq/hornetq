/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
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
package org.jboss.messaging.core.journal.impl;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.jboss.messaging.core.journal.SequentialFile;

/**
 * 
 * A NIOSequentialFile
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class NIOSequentialFile implements SequentialFile
{
	private static final int LONG_LENGTH = 8;
		
	private final String fileName;
	
	private final boolean sync;
		
	private File file;
	
	private FileChannel channel;
	
	private long orderingID;
		
	public NIOSequentialFile(String fileName, boolean sync) throws Exception
	{
		this.fileName = fileName;
		
		this.sync = sync;			  
	}
		
	public void create() throws Exception
	{
		file = new File(fileName);

		RandomAccessFile rfile = new RandomAccessFile(file, "rw");

		channel = rfile.getChannel();		
	}
	
	public void load() throws Exception
	{
		ByteBuffer bb = ByteBuffer.wrap(new byte[LONG_LENGTH]);
		
		channel.read(bb);
		
		orderingID = bb.getLong();
	}
	
	public long getOrderingID()
	{
		return orderingID;
	}
	
	public void initialise(long orderingID, int size) throws Exception
	{
		ByteBuffer bb = ByteBuffer.allocateDirect(size);
		
		this.orderingID = orderingID;

		//First 8 bytes contain the orderingID - used to order the files when loading
		
		bb.putLong(orderingID);
		
		//for debug only
		for (int i = 0; i < size - LONG_LENGTH; i++)
		{
			if (i % 100 == 0)
			{
				bb.put((byte)'\n');
			}
			else
			{
				bb.put((byte)'X');
			}
		}
		//end debug

		bb.flip();

		channel.position(0);

		channel.write(bb);

		channel.force(false);
		
		channel.position(LONG_LENGTH);		
	}

	public void close() throws Exception
	{
		channel.close();
	}

	public void delete() throws Exception
	{
		close();
		
		file.delete();
	}

	public void read(ByteBuffer bytes) throws Exception
	{
		channel.read(bytes);
	}

	public void write(ByteBuffer bytes) throws Exception
	{
		channel.write(bytes);
		
		if (sync)
		{
			channel.force(false);
		}
	}

}
