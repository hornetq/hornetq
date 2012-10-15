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

package org.hornetq.tests.unit.core.server.impl.fakes;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.filter.Filter;
import org.hornetq.core.paging.cursor.PageSubscription;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.QueueFactory;
import org.hornetq.core.server.impl.QueueImpl;

/**
 *
 * A FakeQueueFactory
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class FakeQueueFactory implements QueueFactory
{
   private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();

   private final ExecutorService executor = Executors.newSingleThreadExecutor();

   private PostOffice postOffice;

   public Queue createQueue(final long persistenceID,
                            final SimpleString address,
                            final SimpleString name,
                            final Filter filter,
                            final PageSubscription subscription,
                            final boolean durable,
                            final boolean temporary)
   {
      return new QueueImpl(persistenceID,
                           address,
                           name,
                           filter,
                           subscription,
                           durable,
                           temporary,
                           scheduledExecutor,
                           postOffice,
                           null,
                           null,
                           executor);
   }

   public void setPostOffice(final PostOffice postOffice)
   {
      this.postOffice = postOffice;

   }

   public void stop() throws Exception
   {
      scheduledExecutor.shutdown();

      executor.shutdown();
   }

}
