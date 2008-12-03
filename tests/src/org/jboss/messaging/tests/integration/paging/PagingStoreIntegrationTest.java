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

package org.jboss.messaging.tests.integration.paging;

import java.io.File;

import org.jboss.messaging.core.asyncio.impl.AsynchronousFileImpl;
import org.jboss.messaging.core.journal.impl.AIOSequentialFileFactory;
import org.jboss.messaging.core.journal.impl.NIOSequentialFileFactory;
import org.jboss.messaging.tests.unit.core.paging.impl.PagingStoreTestBase;

/**
 * 
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class PagingStoreIntegrationTest extends PagingStoreTestBase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testPageStoreWithAIO() throws Exception
   {
      if (!AsynchronousFileImpl.isLoaded())
      {
         fail(String.format("libAIO is not loaded on %s %s %s",
                            System.getProperty("os.name"),
                            System.getProperty("os.arch"),
                            System.getProperty("os.version")));
      }
      testConcurrentPaging(new AIOSequentialFileFactory(getTestDir()), 10);
   }

   public void testPageWithNIO() throws Exception
   {
      // This integration test could fail 1 in 100 due to race conditions.
      for (int i = 0; i < 100; i++)
      {
         recreateDirectory();
         System.out.println("Test " + i);
         testConcurrentPaging(new NIOSequentialFileFactory(getTestDir()), 1);
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      recreateDirectory();
   }

   @Override
   protected void tearDown() throws Exception
   {
      super.tearDown();
      // deleteDirectory(new File(journalDir));
   }

   // Private -------------------------------------------------------

   private void recreateDirectory()
   {
      File fileJournalDir = new File(getTestDir());
      deleteDirectory(fileJournalDir);
      fileJournalDir.mkdirs();
   }

   // Inner classes -------------------------------------------------

}
