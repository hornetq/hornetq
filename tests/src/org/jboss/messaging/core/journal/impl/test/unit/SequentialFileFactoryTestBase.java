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
package org.jboss.messaging.core.journal.impl.test.unit;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.jboss.messaging.core.journal.SequentialFile;
import org.jboss.messaging.core.journal.SequentialFileFactory;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.test.unit.UnitTestCase;

/**
 * 
 * A SequentialFileFactoryTestBase
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public abstract class SequentialFileFactoryTestBase extends UnitTestCase
{
	private static final Logger log = Logger.getLogger(SequentialFileFactoryTestBase.class);
	
	
	protected void setUp() throws Exception
	{
		super.setUp();
		
		factory = createFactory();
	}
	
	protected abstract SequentialFileFactory createFactory();
	
	private SequentialFileFactory factory;
		
	public void testCreateAndListFiles() throws Exception
	{
		List<String> expectedFiles = new ArrayList<String>();
		
		final int numFiles = 10;
		
		for (int i = 0; i < numFiles; i++)
		{
			String fileName = UUID.randomUUID().toString() + ".jbm";
			
			expectedFiles.add(fileName);
			
			SequentialFile sf = factory.createSequentialFile(fileName, false);
			
			sf.open();
			
			assertEquals(fileName, sf.getFileName());
		}				
		
		//Create a couple with a different extension - they shouldn't be picked up
		
		SequentialFile sf1 = factory.createSequentialFile("different.file", false);
		sf1.open();
		
		SequentialFile sf2 = factory.createSequentialFile("different.cheese", false);
		sf2.open();
						
		List<String> fileNames = factory.listFiles("jbm");
		
		assertEquals(expectedFiles.size(), fileNames.size());
		
		for (String fileName: expectedFiles)
		{
			assertTrue(fileNames.contains(fileName));
		}
		
		fileNames = factory.listFiles("file");
		
		assertEquals(1, fileNames.size());
		
		assertTrue(fileNames.contains("different.file"));	
		
		fileNames = factory.listFiles("cheese");
		
		assertEquals(1, fileNames.size());
		
		assertTrue(fileNames.contains("different.cheese"));	
	}
	
	
	public void testFill() throws Exception
	{
		SequentialFile sf = factory.createSequentialFile("fill.jbm", true);
		
		sf.open();
		
		checkFill(sf, 0, 100, (byte)'X');
		
		checkFill(sf, 13, 300, (byte)'Y');
		
		checkFill(sf, 0, 1, (byte)'Z');
		
		checkFill(sf, 100, 1, (byte)'A');
		
		checkFill(sf, 1000, 10000, (byte)'B');
	}
	
	public void testDelete() throws Exception
	{
		SequentialFile sf = factory.createSequentialFile("delete-me.jbm", true);
		
		sf.open();
		
		SequentialFile sf2 = factory.createSequentialFile("delete-me2.jbm", true);
		
		sf2.open();
		
		List<String> fileNames = factory.listFiles("jbm");
		
		assertEquals(2, fileNames.size());
		
		assertTrue(fileNames.contains("delete-me.jbm"));
		
		assertTrue(fileNames.contains("delete-me2.jbm"));
		
		sf.delete();
		
		fileNames = factory.listFiles("jbm");
		
		assertEquals(1, fileNames.size());
		
		assertTrue(fileNames.contains("delete-me2.jbm"));
		
	}
	
	public void testWriteandRead() throws Exception
	{
		SequentialFile sf = factory.createSequentialFile("write.jbm", true);
		
		sf.open();
		
		String s1 = "aardvark";
		byte[] bytes1 = s1.getBytes("UTF-8");
		ByteBuffer bb1 = ByteBuffer.wrap(bytes1);
		
		String s2 = "hippopotamus";
		byte[] bytes2 = s2.getBytes("UTF-8");
		ByteBuffer bb2 = ByteBuffer.wrap(bytes2);
		
		String s3 = "echidna";
		byte[] bytes3 = s3.getBytes("UTF-8");
		ByteBuffer bb3 = ByteBuffer.wrap(bytes3);
		
		int bytesWritten = sf.write(bb1, true);
		
		assertEquals(bytes1.length, bytesWritten);
		
		bytesWritten = sf.write(bb2, true);
		
		assertEquals(bytes2.length, bytesWritten);
		
		bytesWritten = sf.write(bb3, true);
		
		assertEquals(bytes3.length, bytesWritten);
		
		sf.position(0);
		
		byte[] rbytes1 = new byte[bytes1.length];
		
		byte[] rbytes2 = new byte[bytes2.length];
		
		byte[] rbytes3 = new byte[bytes3.length];
		
		ByteBuffer rb1 = ByteBuffer.wrap(rbytes1);
		ByteBuffer rb2 = ByteBuffer.wrap(rbytes2);
		ByteBuffer rb3 = ByteBuffer.wrap(rbytes3);

		int bytesRead = sf.read(rb1);
		assertEquals(rbytes1.length, bytesRead);		
		assertByteArraysEquivalent(bytes1, rbytes1);
		
		bytesRead = sf.read(rb2);
		assertEquals(rbytes2.length, bytesRead);		
		assertByteArraysEquivalent(bytes2, rbytes2);
		
		bytesRead = sf.read(rb3);
		assertEquals(rbytes3.length, bytesRead);		
		assertByteArraysEquivalent(bytes3, rbytes3);				
	}
	
	public void testPosition() throws Exception
	{
		SequentialFile sf = factory.createSequentialFile("position.jbm", true);
		
		sf.open();
		
		String s1 = "orange";
		byte[] bytes1 = s1.getBytes("UTF-8");
		ByteBuffer bb1 = ByteBuffer.wrap(bytes1);
		
		String s2 = "grapefruit";
		byte[] bytes2 = s2.getBytes("UTF-8");
		ByteBuffer bb2 = ByteBuffer.wrap(bytes2);
		
		String s3 = "lemon";
		byte[] bytes3 = s3.getBytes("UTF-8");
		ByteBuffer bb3 = ByteBuffer.wrap(bytes3);
		
		int bytesWritten = sf.write(bb1, true);
		
		assertEquals(bytes1.length, bytesWritten);
		
		bytesWritten = sf.write(bb2, true);
		
		assertEquals(bytes2.length, bytesWritten);
		
		bytesWritten = sf.write(bb3, true);
		
		assertEquals(bytes3.length, bytesWritten);
		
		byte[] rbytes1 = new byte[bytes1.length];
		
		byte[] rbytes2 = new byte[bytes2.length];
		
		byte[] rbytes3 = new byte[bytes3.length];
		
		ByteBuffer rb1 = ByteBuffer.wrap(rbytes1);
		ByteBuffer rb2 = ByteBuffer.wrap(rbytes2);
		ByteBuffer rb3 = ByteBuffer.wrap(rbytes3);
		
		sf.position(bytes1.length + bytes2.length);
		
		int bytesRead = sf.read(rb3);
		assertEquals(rbytes3.length, bytesRead);		
		assertByteArraysEquivalent(bytes3, rbytes3);		
		
		sf.position(bytes1.length);
		
		bytesRead = sf.read(rb2);
		assertEquals(rbytes2.length, bytesRead);		
		assertByteArraysEquivalent(bytes2, rbytes2);
		
		sf.position(0);
		
		bytesRead = sf.read(rb1);
		assertEquals(rbytes1.length, bytesRead);		
		assertByteArraysEquivalent(bytes1, rbytes1);		
	}
	
	public void testOpenClose() throws Exception
	{
		SequentialFile sf = factory.createSequentialFile("openclose.jbm", true);
		
		sf.open();
		
		String s1 = "cheesecake";
		byte[] bytes1 = s1.getBytes("UTF-8");
		ByteBuffer bb1 = ByteBuffer.wrap(bytes1);
		
		int bytesWritten = sf.write(bb1, true);
		
		assertEquals(bytes1.length, bytesWritten);
		
		sf.close();
		
		try
		{
			sf.write(bb1, true);
			
			fail("Should throw exception");
		}
		catch (Exception e)
		{
			//OK
		}
		
		sf.open();
		
		sf.write(bb1, true);
	}
	
	// Private ---------------------------------
	
	private void checkFill(SequentialFile file, int pos, int size, byte fillChar) throws Exception
	{
		file.fill(pos, size, fillChar);
		
		file.close();
		
		file.open();
		
		file.position(pos);
		
		byte[] bytes = new byte[size];
		
		ByteBuffer bb = ByteBuffer.wrap(bytes);
		
		int bytesRead = file.read(bb);
		
		assertEquals(size, bytesRead);
		
		for (int i = 0; i < size; i++)
		{
			//log.info(" i is " + i);
			assertEquals(fillChar, bytes[i]);
		}
				
	}
	
}
