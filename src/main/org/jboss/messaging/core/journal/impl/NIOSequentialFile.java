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
import org.jboss.messaging.core.logging.Logger;

/**
 * 
 * A NIOSequentialFile
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class NIOSequentialFile implements SequentialFile
{
	private static final Logger log = Logger.getLogger(NIOSequentialFile.class);
			
	private String journalDir;
	
	private String fileName;
	
	private boolean sync;
	
	private File file;
	
	private FileChannel channel;
	
	private RandomAccessFile rfile;
		
	public NIOSequentialFile(final String journalDir, final String fileName, final boolean sync)
	{
		this.journalDir = journalDir;
		
		this.fileName = fileName;
		
		this.sync = sync;		
	}
	
	public String getFileName()
	{
		return fileName;
	}
		
	public void open() throws Exception
	{		
		file = new File(journalDir + "/" + fileName);

		rfile = new RandomAccessFile(file, "rw");

		channel = rfile.getChannel();		
		
		//log.info("Opened file");
	}
	
	public void fill(final int position, final int size, final byte fillCharacter) throws Exception
	{
		ByteBuffer bb = ByteBuffer.allocateDirect(size);
		
		for (int i = 0; i < size; i++)
		{
			bb.put(fillCharacter);			
		}
		
		bb.flip();

		channel.position(position);

		channel.write(bb);

		channel.force(false);	
		
		channel.position(0);
	}
	
	public void close() throws Exception
	{
		channel.close();
		
		rfile.close();
						
		channel = null;
		
		rfile = null;
		
		file = null;
		
		//log.info("Closed file");
	}

	public void delete() throws Exception
	{		
		file.delete();
		
		close();		
	}

	public int read(ByteBuffer bytes) throws Exception
	{
		//log.info("reading, position is " + channel.position());
		
		int bytesRead = channel.read(bytes);
		
		//log.info("Read " + bytesRead + " bytes");
		
		return bytesRead;
	}

	public int write(ByteBuffer bytes, boolean sync) throws Exception
	{
		int bytesRead = channel.write(bytes);
		
		if (sync && this.sync)
		{
			channel.force(false);
		}
		
		return bytesRead;
	}

	public void position(final int pos) throws Exception
	{
		//log.info("Positioning to " + pos);
		channel.position(pos);
	}
}
