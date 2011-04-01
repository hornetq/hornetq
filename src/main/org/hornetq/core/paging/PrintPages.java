/*
 * Copyright 2010 Red Hat, Inc.
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

package org.hornetq.core.paging;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.paging.impl.PagingManagerImpl;
import org.hornetq.core.paging.impl.PagingStoreFactoryNIO;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.persistence.impl.nullpm.NullStorageManager;
import org.hornetq.core.settings.HierarchicalRepository;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.core.settings.impl.HierarchicalObjectRepository;
import org.hornetq.utils.ExecutorFactory;

/**
 * A PrintPage
 *
 * @author clebertsuconic
 *
 *
 */
public class PrintPages
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------
   
   public static void main(String arg[])
   {
      try
      {
         ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(1);
         final ExecutorService executor = Executors.newFixedThreadPool(10);
         ExecutorFactory execfactory = new ExecutorFactory()
         {
            
            public Executor getExecutor()
            {
               return executor;
            }
         };
         PagingStoreFactory pageStoreFactory = new PagingStoreFactoryNIO(arg[0], 1000l, scheduled, execfactory, false);
         HierarchicalRepository<AddressSettings> addressSettingsRepository = new HierarchicalObjectRepository<AddressSettings>(); 
         addressSettingsRepository.setDefault(new AddressSettings());
         StorageManager sm = new NullStorageManager();
         PagingManager manager = new PagingManagerImpl(pageStoreFactory, sm, addressSettingsRepository);
         
         manager.start();
         
         SimpleString stores[] = manager.getStoreNames();

         for (SimpleString store : stores)
         {
            System.out.println("####################################################################################################");
            System.out.println("Exploring store " + store);
            PagingStore pgStore = manager.getPageStore(store);
            int pgid = (int)pgStore.getFirstPage();
            for (int pg = 0 ; pg < pgStore.getNumberOfPages(); pg++)
            {
               System.out.println("*******   Page " + pgid);
               Page page = pgStore.createPage(pgid);
               page.open();
               List<PagedMessage> msgs = page.read();
               page.close();
               
               int msgID = 0;
               
               for (PagedMessage msg : msgs)
               {
                  msg.initMessage(sm);
                  System.out.println("pg=" + pg + ", msg=" + msgID + "=" + msg.getMessage());
                  msgID++;
               }
               
               pgid ++;
               
            }
         }
         
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
