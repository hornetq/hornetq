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


package org.jboss.messaging.tests.unit.core.journal.impl;

import java.nio.ByteBuffer;

import org.jboss.messaging.core.asyncio.impl.AsynchronousFileImpl;
import org.jboss.messaging.core.journal.SequentialFileFactory;
import org.jboss.messaging.core.journal.impl.AIOSequentialFileFactory;
import org.jboss.messaging.core.journal.impl.NIOSequentialFileFactory;
import org.jboss.messaging.tests.unit.core.journal.impl.fakes.FakeSequentialFileFactory;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * 
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class CleanBufferTest extends UnitTestCase
{
   
   // Constants -----------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------
   
   
   public void testCleanOnNIO()
   {
      SequentialFileFactory factory = new NIOSequentialFileFactory("Whatever");

      testBuffer(factory);
   }

   public void testCleanOnAIO()
   {
      if (AsynchronousFileImpl.isLoaded())
      {
         SequentialFileFactory factory = new AIOSequentialFileFactory("Whatever");
   
         testBuffer(factory);
      }
   }

   public void testCleanOnFake()
   {
      SequentialFileFactory factory = new FakeSequentialFileFactory();

      testBuffer(factory);
   }

   private void testBuffer(SequentialFileFactory factory)
   {
      ByteBuffer buffer = factory.newBuffer(100);
      for (byte b = 0; b < 100; b++)
      {
         buffer.put(b);
      }
      
      buffer.rewind();
      
      for (byte b = 0; b < 100; b++)
      {
         assertEquals(b, buffer.get());
      }
      
      

      buffer.limit(10);
      factory.clearBuffer(buffer);
      buffer.limit(100);
      
      buffer.rewind();
      
      for (byte b = 0; b < 100; b++)
      {
         if (b < 10)
         {
            assertEquals(0, buffer.get());
         }
         else
         {
            assertEquals(b, buffer.get());
         }
      }
   }
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
   
}
