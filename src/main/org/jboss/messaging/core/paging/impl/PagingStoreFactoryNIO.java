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

package org.jboss.messaging.core.paging.impl;

import java.io.File;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.jboss.messaging.core.journal.SequentialFileFactory;
import org.jboss.messaging.core.journal.impl.NIOSequentialFileFactory;
import org.jboss.messaging.core.paging.PagingManager;
import org.jboss.messaging.core.paging.PagingStore;
import org.jboss.messaging.core.paging.PagingStoreFactory;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.util.JBMThreadFactory;
import org.jboss.messaging.util.SimpleString;

/**
 * 
 * Integration point between Paging and NIO
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class PagingStoreFactoryNIO implements PagingStoreFactory
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final String directory;

   private final ExecutorService executor;

   private PagingManager pagingManager;
   
   private StorageManager storageManager;
   
   private PostOffice postOffice;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public PagingStoreFactoryNIO(final String directory)
   {
      this.directory = directory;

      executor = Executors.newCachedThreadPool(new JBMThreadFactory("JBM-depaging-threads"));
   }

   // Public --------------------------------------------------------

   public Executor getPagingExecutor()
   {
      return executor;
   }

   public void stop() throws InterruptedException
   {
      executor.shutdown();

      executor.awaitTermination(30, TimeUnit.SECONDS);
   }

   public PagingStore newStore(final SimpleString destinationName, final QueueSettings settings)
   {
      // FIXME: This directory creation should be done inside PagingStoreImpl::start, or this method should be made
      // synchornized
      final String destinationDirectory = directory + "/" + destinationName.toString();

      File destinationFile = new File(destinationDirectory);

      destinationFile.mkdirs();

      return new PagingStoreImpl(pagingManager,
                                 storageManager,
                                 postOffice,
                                 newFileFactory(destinationDirectory),
                                 destinationName,
                                 settings,
                                 executor);
   }

   public void setPagingManager(final PagingManager pagingManager)
   {
      this.pagingManager = pagingManager;
   }
   
   public void setStorageManager(final StorageManager storageManager)
   {
      this.storageManager = storageManager; 
   }
   
   public void setPostOffice(final PostOffice postOffice)
   {
      this.postOffice = postOffice;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected SequentialFileFactory newFileFactory(final String destinationDirectory)
   {
      return new NIOSequentialFileFactory(destinationDirectory);
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
