/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.messaging.core.journal.impl;

import java.nio.ByteBuffer;

import org.jboss.messaging.core.journal.SequentialFile;
import org.jboss.messaging.core.asyncio.impl.AsynchronousFileImpl;

/**
 * 
 * A AIOSequentialFileFactory
 * 
 * @author clebert.suconic@jboss.com
 *
 */
public class AIOSequentialFileFactory extends AbstractSequentialFactory
{	
	public AIOSequentialFileFactory(final String journalDir)
	{
		super(journalDir);
	}
	
	public SequentialFile createSequentialFile(final String fileName, final boolean sync, final int maxIO, final int timeout) throws Exception
	{
		return new AIOSequentialFile(journalDir, fileName, maxIO, timeout);
	}
	
   public boolean supportsCallbacks()
   {
      return true;
   }
   
	public static boolean isSupported()
	{
		return AsynchronousFileImpl.isLoaded();
	}
	
   public ByteBuffer newBuffer(int size)
   {
      if (size % 512 != 0)
      {
         size = (size / 512 + 1) * 512;
      }
      return ByteBuffer.allocateDirect(size);
   }
   
   // For tests only
   public ByteBuffer wrapBuffer(final byte[] bytes)
   {
      ByteBuffer newbuffer = newBuffer(bytes.length);
      newbuffer.put(bytes);
      return newbuffer;
   };
   }
