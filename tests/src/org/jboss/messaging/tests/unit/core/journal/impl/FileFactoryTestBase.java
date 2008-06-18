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

import org.jboss.messaging.core.journal.SequentialFile;
import org.jboss.messaging.core.journal.SequentialFileFactory;
import org.jboss.messaging.tests.util.UnitTestCase;
/**
 * 
 * @author clebert.suconic@jboss.org
 *
 */
public abstract class FileFactoryTestBase extends UnitTestCase
{
   protected abstract SequentialFileFactory createFactory();
   
   protected SequentialFileFactory factory;

   protected void setUp() throws Exception
   {
      super.setUp();
      
      factory = createFactory();
   }
   

   
   // Protected ---------------------------------
   
   protected void checkFill(SequentialFile file, int pos, int size, byte fillChar) throws Exception
   {
      file.fill(pos, size, fillChar);
      
      file.close();
      
      file.open();
      
      file.position(pos);
      
      
      
      ByteBuffer bb = ByteBuffer.allocateDirect(size);
      
      int bytesRead = file.read(bb);
      
      assertEquals(size, bytesRead);
      
      bb.rewind();
      
      byte bytes[] = new byte[size];
      
      bb.get(bytes);
      
      for (int i = 0; i < size; i++)
      {
         //log.info(" i is " + i);
         assertEquals(fillChar, bytes[i]);
      }
            
   }
   
   

}
