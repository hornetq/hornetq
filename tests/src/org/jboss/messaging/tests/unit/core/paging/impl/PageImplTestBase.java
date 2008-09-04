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


package org.jboss.messaging.tests.unit.core.paging.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.jboss.messaging.core.journal.SequentialFile;
import org.jboss.messaging.core.journal.SequentialFileFactory;
import org.jboss.messaging.core.paging.PageMessage;
import org.jboss.messaging.core.paging.impl.PageImpl;
import org.jboss.messaging.core.paging.impl.PageMessageImpl;
import org.jboss.messaging.core.remoting.impl.ByteBufferWrapper;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.server.impl.ServerMessageImpl;
import org.jboss.messaging.tests.util.RandomUtil;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.util.SimpleString;

/**
 * 
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public abstract class PageImplTestBase extends UnitTestCase
{
   
   // Constants -----------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   /** Validate if everything we add is recovered */
   protected void testAdd(SequentialFileFactory factory, int numberOfElements) throws Exception
   {
      
      SequentialFile file = factory.createSequentialFile("00010.page", 1);
      
      PageImpl impl = new PageImpl(factory, file, 10);
      
      assertEquals(10, impl.getPageId());
      
      impl.open();

      assertEquals(1, factory.listFiles("page").size());

      ArrayList<ByteBuffer> buffers = new ArrayList<ByteBuffer>();
      
      SimpleString simpleDestination = new SimpleString("Test");
      
      for (int i = 0; i < numberOfElements; i++)
      {
         ByteBuffer buffer = ByteBuffer.allocate(10);
         
         for (int j = 0; j < buffer.limit(); j++)
         {
            buffer.put(RandomUtil.randomByte());
         }
         
         buffers.add(buffer);

         ServerMessage msg = new ServerMessageImpl((byte)1, true, 0,
               System.currentTimeMillis(), (byte)0, new ByteBufferWrapper(buffer));
         
         msg.setMessageID((long)i);
         
         msg.setDestination(simpleDestination);
         
         impl.write(new PageMessageImpl(msg));
         
         assertEquals(i + 1, impl.getNumberOfMessages());
      }
      
      impl.sync();
      impl.close();
      
      file = factory.createSequentialFile("00010.page", 1);
      file.open();
      impl = new PageImpl(factory, file, 10);
      
      PageMessage msgs[] = impl.read();
      
      assertEquals(numberOfElements, msgs.length);

      assertEquals(numberOfElements, impl.getNumberOfMessages());
      
      for (int i = 0; i < msgs.length; i++)
      {
         assertEquals((long)0, msgs[i].getMessage().getMessageID());
         
         assertEquals(simpleDestination, msgs[i].getMessage().getDestination());
         
         assertEqualsByteArrays(buffers.get(i).array(), msgs[i].getMessage().getBody().array());
      }

      impl.delete();
      
      
      assertEquals(0, factory.listFiles(".page").size());
      
   }
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
   
}
