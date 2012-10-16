/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.core.paging.impl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.journal.IOCriticalErrorListener;
import org.hornetq.core.journal.SequentialFileFactory;
import org.hornetq.core.journal.impl.NIOSequentialFileFactory;
import org.hornetq.core.paging.PagingManager;
import org.hornetq.core.paging.PagingStore;
import org.hornetq.core.paging.PagingStoreFactory;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.server.HornetQServerLogger;
import org.hornetq.core.settings.HierarchicalRepository;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.utils.ExecutorFactory;
import org.hornetq.utils.UUIDGenerator;

/**
 * 
 * Integration point between Paging and NIO
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class PagingStoreFactoryNIO implements PagingStoreFactory
{

   // Constants -----------------------------------------------------

   private static final String ADDRESS_FILE = "address.txt";

   // Attributes ----------------------------------------------------

   private final String directory;

   private final ExecutorFactory executorFactory;

   protected final boolean syncNonTransactional;

   private PagingManager pagingManager;

   private final ScheduledExecutorService scheduledExecutor;

   private final long syncTimeout;

   private StorageManager storageManager;

   private PostOffice postOffice;

   private final IOCriticalErrorListener critialErrorListener;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public PagingStoreFactoryNIO(final String directory,
                                final long syncTimeout,
                                final ScheduledExecutorService scheduledExecutor,
                                final ExecutorFactory executorFactory,
                                final boolean syncNonTransactional)
   {
      this(directory, syncTimeout, scheduledExecutor, executorFactory, syncNonTransactional, null);
   }

   public PagingStoreFactoryNIO(final String directory,
                                final long syncTimeout,
                                final ScheduledExecutorService scheduledExecutor,
                                final ExecutorFactory executorFactory,
                                final boolean syncNonTransactional,
                                final IOCriticalErrorListener critialErrorListener)
   {
      this.directory = directory;

      this.executorFactory = executorFactory;

      this.syncNonTransactional = syncNonTransactional;

      this.scheduledExecutor = scheduledExecutor;

      this.syncTimeout = syncTimeout;

      this.critialErrorListener = critialErrorListener;
   }

   // Public --------------------------------------------------------

   public void stop()
   {
   }

   public synchronized PagingStore newStore(final SimpleString address, final AddressSettings settings)
   {

      return new PagingStoreImpl(address,
                                 scheduledExecutor,
                                 syncTimeout,
                                 pagingManager,
                                 storageManager,
                                 null,
                                 this,
                                 address,
                                 settings,
                                 executorFactory.getExecutor(),
                                 syncNonTransactional);
   }

   public synchronized SequentialFileFactory newFileFactory(final SimpleString address) throws Exception
   {

      String guid = UUIDGenerator.getInstance().generateStringUUID();

      SequentialFileFactory factory = newFileFactory(guid);

      factory.createDirs();

      File fileWithID = new File(directory + File.separatorChar +
                                 guid +
                                 File.separatorChar +
                                 PagingStoreFactoryNIO.ADDRESS_FILE);

      BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileWithID)));

      try
      {
         writer.write(address.toString());
         writer.newLine();
      }
      finally
      {
         writer.close();
      }

      return factory;
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

   public List<PagingStore> reloadStores(final HierarchicalRepository<AddressSettings> addressSettingsRepository) throws Exception
   {
      File pageDirectory = new File(directory);

      File[] files = pageDirectory.listFiles();

      if (files == null)
      {
         return Collections.<PagingStore> emptyList();
      }
      else
      {
         ArrayList<PagingStore> storesReturn = new ArrayList<PagingStore>(files.length);

         for (File file : files)
         {

            final String guid = file.getName();

            final File addressFile = new File(file, PagingStoreFactoryNIO.ADDRESS_FILE);

            if (!addressFile.exists())
            {
               HornetQServerLogger.LOGGER.pageStoreFactoryNoIdFile(file.toString(), PagingStoreFactoryNIO.ADDRESS_FILE);
               continue;
            }

            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(addressFile)));

            String addressString;

            try
            {
               addressString = reader.readLine();
            }
            finally
            {
               reader.close();
            }

            SimpleString address = new SimpleString(addressString);

            SequentialFileFactory factory = newFileFactory(guid);

            AddressSettings settings = addressSettingsRepository.getMatch(address.toString());

            PagingStore store = new PagingStoreImpl(address,
                                                    scheduledExecutor,
                                                    syncTimeout,
                                                    pagingManager,
                                                    storageManager,
                                                    factory,
                                                    this,
                                                    address,
                                                    settings,
                                                    executorFactory.getExecutor(),
                                                    syncNonTransactional);

            storesReturn.add(store);
         }

         return storesReturn;
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected SequentialFileFactory newFileFactory(final String directoryName)
   {
      return new NIOSequentialFileFactory(directory + File.separatorChar + directoryName, false, critialErrorListener);
   }

   protected PagingManager getPagingManager()
   {
      return pagingManager;
   }

   protected StorageManager getStorageManager()
   {
      return storageManager;
   }

   protected PostOffice getPostOffice()
   {
      return postOffice;
   }

   protected ExecutorFactory getExecutorFactory()
   {
      return executorFactory;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
