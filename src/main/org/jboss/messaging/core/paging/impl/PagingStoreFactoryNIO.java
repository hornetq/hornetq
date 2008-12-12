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

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.jboss.messaging.core.journal.SequentialFileFactory;
import org.jboss.messaging.core.journal.impl.NIOSequentialFileFactory;
import org.jboss.messaging.core.paging.PagingManager;
import org.jboss.messaging.core.paging.PagingStore;
import org.jboss.messaging.core.paging.PagingStoreFactory;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.util.Base64;
import org.jboss.messaging.util.JBMThreadFactory;
import org.jboss.messaging.util.OrderedExecutorFactory;
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

   private final ExecutorService parentExecutor;
   
   private final OrderedExecutorFactory executorFactory;
   
   private final Executor globalDepagerExecutor;

   private PagingManager pagingManager;
   
   private StorageManager storageManager;
   
   private PostOffice postOffice;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public PagingStoreFactoryNIO(final String directory, final int maxThreads)
   {
      System.out.println("maxThreads = " + maxThreads);
      this.directory = directory;

      parentExecutor = new ThreadPoolExecutor(0, maxThreads,
                             60L, TimeUnit.SECONDS,
                             new SynchronousQueue<Runnable>(),
                             new JBMThreadFactory("JBM-depaging-threads"));
      
      executorFactory = new OrderedExecutorFactory(parentExecutor);
      
      globalDepagerExecutor = executorFactory.getExecutor();
   }

   // Public --------------------------------------------------------

   public Executor getGlobalDepagerExecutor()
   {
      return globalDepagerExecutor;
   }

   public void stop() throws InterruptedException
   {
      parentExecutor.shutdown();

      parentExecutor.awaitTermination(30, TimeUnit.SECONDS);
   }

   public PagingStore newStore(final SimpleString destinationName, final QueueSettings settings)
   {
      
      final String destinationDirectory = directory + "/" + Base64.encodeBytes(destinationName.getData(), Base64.URL_SAFE);

      return new PagingStoreImpl(pagingManager,
                                 storageManager,
                                 postOffice,
                                 newFileFactory(destinationDirectory),
                                 destinationName,
                                 settings,
                                 executorFactory.getExecutor());
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
