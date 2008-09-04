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
import org.jboss.messaging.tests.unit.core.paging.impl.PageImplTestBase;

public class PagingIntegrationTest extends PageImplTestBase
{
   
   // Constants -----------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected String journalDir = System.getProperty("java.io.tmpdir", "/tmp") +  "/journal-test";
   
   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------
   
   public void testPageWithAIO() throws Exception
   {
      if (!AsynchronousFileImpl.isLoaded())
      {
         fail(String.format("libAIO is not loaded on %s %s %s", 
               System.getProperty("os.name"), 
               System.getProperty("os.arch"), 
               System.getProperty("os.version")));
      }
      testAdd(new AIOSequentialFileFactory(journalDir), 1000);
   }
   
   public void testPageWithNIO() throws Exception
   {
      testAdd(new NIOSequentialFileFactory(journalDir), 1000);
   }
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      File fileJournalDir = new File(journalDir);
      deleteDirectory(fileJournalDir);
      fileJournalDir.mkdirs();
   }
   
   protected void tearDown() throws Exception
   {
      super.tearDown();
      deleteDirectory(new File(journalDir));
   }

   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
   
}
