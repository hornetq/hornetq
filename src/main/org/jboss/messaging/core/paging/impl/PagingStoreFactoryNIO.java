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
import java.util.concurrent.Executor;

import org.jboss.messaging.core.journal.SequentialFileFactory;
import org.jboss.messaging.core.journal.impl.NIOSequentialFileFactory;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.paging.PagingManager;
import org.jboss.messaging.core.paging.PagingStore;
import org.jboss.messaging.core.paging.PagingStoreFactory;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.AddressSettings;
import org.jboss.messaging.utils.ExecutorFactory;
import org.jboss.messaging.utils.SimpleString;
import org.jboss.messaging.utils.UUIDGenerator;

/**
 * 
 * Integration point between Paging and NIO
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class PagingStoreFactoryNIO implements PagingStoreFactory
{

   // Constants -----------------------------------------------------
   private static final Logger log = Logger.getLogger(PagingStoreFactoryNIO.class);

   private static final String ADDRESS_FILE = "address.txt";

   // Attributes ----------------------------------------------------

   private final String directory;

   private final ExecutorFactory executorFactory;

   private final Executor globalDepagerExecutor;

   private PagingManager pagingManager;

   private StorageManager storageManager;

   private PostOffice postOffice;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public PagingStoreFactoryNIO(final String directory, final ExecutorFactory executorFactory)
   {
      this.directory = directory;

      this.executorFactory = executorFactory;

      globalDepagerExecutor = executorFactory.getExecutor();
   }

   // Public --------------------------------------------------------

   public Executor getGlobalDepagerExecutor()
   {
      return globalDepagerExecutor;
   }

   public void stop()
   {
   }

   public synchronized PagingStore newStore(final SimpleString destinationName, final AddressSettings settings) throws Exception
   {

      return new PagingStoreImpl(pagingManager,
                                 storageManager,
                                 postOffice,
                                 null,
                                 this,
                                 destinationName,
                                 settings,
                                 executorFactory.getExecutor());
   }

   /**
    * @param storeName
    * @return
    */
   public synchronized SequentialFileFactory newFileFactory(final SimpleString destinationName) throws Exception
   {

      String guid = UUIDGenerator.getInstance().generateStringUUID();

      SequentialFileFactory factory = newFileFactory(guid);

      factory.createDirs();

      File fileWithID = new File(directory + File.separatorChar + guid + File.separatorChar + ADDRESS_FILE);

      BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileWithID)));

      try
      {
         writer.write(destinationName.toString());
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
            
            final File addressFile = new File(file, ADDRESS_FILE);

            if (!addressFile.exists())
            {
               log.warn("Directory " + file.toString() + " didn't have an identification file " + ADDRESS_FILE);
               continue;
            }

            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(addressFile)));

            String destination;
            
            try
            {
               destination = reader.readLine();
            }
            finally
            {
               reader.close();
            }

            SimpleString destinationName = new SimpleString(destination);

            SequentialFileFactory factory = newFileFactory(guid);

            AddressSettings settings = addressSettingsRepository.getMatch(destinationName.toString());

            PagingStore store = new PagingStoreImpl(pagingManager,
                                                    storageManager,
                                                    postOffice,
                                                    factory,
                                                    this,
                                                    destinationName,
                                                    settings,
                                                    executorFactory.getExecutor());

            storesReturn.add(store);
         }

         return storesReturn;
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected SequentialFileFactory newFileFactory(final String directoryName)
   {
      return new NIOSequentialFileFactory(directory + File.separatorChar + directoryName);
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
