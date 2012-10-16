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

package org.hornetq.core.server.impl;

import java.util.concurrent.ScheduledExecutorService;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.filter.Filter;
import org.hornetq.core.paging.cursor.PageSubscription;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.QueueFactory;
import org.hornetq.core.settings.HierarchicalRepository;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.utils.ExecutorFactory;

/**
 *
 * A QueueFactoryImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 *
 */
public class QueueFactoryImpl implements QueueFactory
{
   private final HierarchicalRepository<AddressSettings> addressSettingsRepository;

   private final ScheduledExecutorService scheduledExecutor;

   /** This is required for delete-all-reference to work correctly with paging, and controlling global-size */
   private PostOffice postOffice;

   private final StorageManager storageManager;
   
   private final ExecutorFactory executorFactory;

   public QueueFactoryImpl(final ExecutorFactory executorFactory,
                           final ScheduledExecutorService scheduledExecutor,
                           final HierarchicalRepository<AddressSettings> addressSettingsRepository,
                           final StorageManager storageManager)
   {
      this.addressSettingsRepository = addressSettingsRepository;

      this.scheduledExecutor = scheduledExecutor;

      this.storageManager = storageManager;
      
      this.executorFactory = executorFactory;
   }

   public void setPostOffice(final PostOffice postOffice)
   {
      this.postOffice = postOffice;
   }

   public Queue createQueue(final long persistenceID,
                            final SimpleString address,
                            final SimpleString name,
                            final Filter filter,
                            final PageSubscription pageSubscription,
                            final boolean durable,
                            final boolean temporary)
   {
      AddressSettings addressSettings = addressSettingsRepository.getMatch(address.toString());

      Queue queue;
      if (addressSettings.isLastValueQueue())
      {
         queue = new LastValueQueue(persistenceID,
                                    address,
                                    name,
                                    filter,
                                    pageSubscription,
                                    durable,
                                    temporary,
                                    scheduledExecutor,
                                    postOffice,
                                    storageManager,
                                    addressSettingsRepository,
                                    executorFactory.getExecutor());
      }
      else
      {
         queue = new QueueImpl(persistenceID,
                               address,
                               name,
                               filter,
                               pageSubscription,
                               durable,
                               temporary,
                               scheduledExecutor,
                               postOffice,
                               storageManager,
                               addressSettingsRepository,
                               executorFactory.getExecutor());
      }

      return queue;
   }
}
