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

package org.jboss.messaging.tests.unit.core.journal.impl;

import java.io.File;

import org.jboss.messaging.core.asyncio.impl.AsynchronousFileImpl;
import org.jboss.messaging.core.journal.SequentialFileFactory;
import org.jboss.messaging.core.journal.impl.AIOSequentialFileFactory;
import org.jboss.messaging.core.logging.Logger;

/**
 * 
 * A RealJournalImplTest
 * you need to define -Djava.library.path=${project-root}/native/src/.libs when calling the JVM
 * If you are running this test in eclipse you should do:
 *   I - Run->Open Run Dialog
 *   II - Find the class on the list (you will find it if you already tried running this testcase before)  
 *   III - Add -Djava.library.path=<your project place>/native/src/.libs
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class RealAIOJournalImplTest extends JournalImplTestUnit
{
   private static final Logger log = Logger.getLogger(RealAIOJournalImplTest.class);
   
   // Need to run the test over a local disk (no NFS)
   protected String journalDir = "/tmp/journal-test";
     
   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      if (!AsynchronousFileImpl.isLoaded())
      {
         fail(String.format("libAIO is not loaded on %s %s %s", 
               System.getProperty("os.name"), 
               System.getProperty("os.arch"), 
               System.getProperty("os.version")));
      }
   }
   
   protected void tearDown() throws Exception
   {
      super.tearDown();
      deleteDirectory(new File(journalDir));
   }
   
   protected SequentialFileFactory getFileFactory() throws Exception
   {
      File file = new File(journalDir);
      
      deleteDirectory(file);
      
      file.mkdir();     
      
      return new AIOSequentialFileFactory(journalDir);
   }  

   protected int getAlignment()
   {
      return 512;
   }
   
}
