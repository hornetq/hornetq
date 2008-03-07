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
package org.jboss.messaging.core.journal.impl.test.unit.fakes;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.jboss.messaging.core.journal.SequentialFile;
import org.jboss.messaging.core.journal.SequentialFileFactory;
import org.jboss.messaging.core.logging.Logger;

/**
 * 
 * A FakeSequentialFileFactory
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class FakeSequentialFileFactory implements SequentialFileFactory
{
	private static final Logger log = Logger.getLogger(FakeSequentialFileFactory.class);
		
	private Map<String, FakeSequentialFile> fileMap = new ConcurrentHashMap<String, FakeSequentialFile>();
	
	public SequentialFile createSequentialFile(final String fileName, final boolean sync) throws Exception
	{
		FakeSequentialFile sf = fileMap.get(fileName);
		
		if (sf == null)
		{						
		   sf = new FakeSequentialFile(fileName, sync);
		   
		   fileMap.put(fileName, sf);
		}
		else
		{		
			sf.data.position(0);
			
			log.info("positioning data to 0");
		}
						
		return sf;
	}
	
	public List<String> listFiles(String journalDir, String extension)
	{
		return new ArrayList<String>(fileMap.keySet());
	}
	
	public Map<String, FakeSequentialFile> getFileMap()
	{
		return fileMap;
	}
	
	public void clear()
	{
		fileMap.clear();
	}
	
	public class FakeSequentialFile implements SequentialFile
	{
		private volatile boolean open;
		
		private final String fileName;
		
		private final boolean sync;
		
		private ByteBuffer data;
		
		public ByteBuffer getData()
		{
			return data;
		}
		
		public boolean isSync()
		{
			return sync;
		}
		
		public boolean isOpen()
		{
			log.info("is open" + System.identityHashCode(this) +" open is now " + open);
			return open;
		}
		
		public FakeSequentialFile(final String fileName, final boolean sync)
		{
			this.fileName = fileName;
			
			this.sync = sync;		
		}

		public void close() throws Exception
		{
			open = false;
			
			log.info("Calling close " + System.identityHashCode(this) +" open is now " + open);
		}

		public void delete() throws Exception
		{
			if (!open)
			{
				throw new IllegalStateException("Is closed");
			}
			close();
			
			fileMap.remove(fileName);
		}

		public String getFileName()
		{
			if (!open)
			{
				throw new IllegalStateException("Is closed");
			}
			return fileName;
		}

		public void open() throws Exception
		{
			log.info("open called");
			
			if (open)
			{
				throw new IllegalStateException("Is already open");
			}		

			open = true;
		}

		public void preAllocate(int size, byte fillCharacter) throws Exception
		{		
			if (!open)
			{
				throw new IllegalStateException("Is closed");
			}
			
			log.info("pre-allocate called " + size +" , " + fillCharacter);
			
			byte[] bytes = new byte[size];
			
			for (int i = 0; i < size; i++)
			{
				bytes[i] = fillCharacter;
			}
			
			data = ByteBuffer.wrap(bytes);		
		}

		public void read(ByteBuffer bytes) throws Exception
		{
			if (!open)
			{
				throw new IllegalStateException("Is closed");
			}
			
			log.info("read called " + bytes.array().length);
			
			byte[] bytesRead = new byte[bytes.array().length];
			
			log.info("reading, data pos is " + data.position() + " data size is " + data.array().length);
			
			data.get(bytesRead);
			
			bytes.put(bytesRead);
		}

		public void reset() throws Exception
		{
			if (!open)
			{
				throw new IllegalStateException("Is closed");
			}
			
			log.info("reset called");
			
			data.position(0);
		}

		public void write(ByteBuffer bytes) throws Exception
		{
			if (!open)
			{
				throw new IllegalStateException("Is closed");
			}
			
			log.info("write called, position is " + data.position() + " bytes is " + bytes.array().length);
			
			data.put(bytes);
		}

	}

}
