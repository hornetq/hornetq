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

import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.Consumer;

/**
 * A RoundRobinDistributor
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class RoundRobinDistributor extends DistributorImpl
{
   private static final Logger log = Logger.getLogger(RoundRobinDistributor.class);

   protected int pos = 0;

   @Override
   public synchronized void addConsumer(final Consumer consumer)
   {
      pos = 0;
      super.addConsumer(consumer);
   }

   @Override
   public synchronized boolean removeConsumer(final Consumer consumer)
   {
      pos = 0;
      return super.removeConsumer(consumer);
   }

   @Override
   public synchronized int getConsumerCount()
   {
      return super.getConsumerCount();
   }

   public synchronized Consumer getNextConsumer()
   {
      Consumer consumer = consumers.get(pos);
      incrementPosition();
      return consumer;
   }

   private synchronized void incrementPosition()
   {
      pos++;

      if (pos == consumers.size())
      {
         pos = 0;
      }
   }
}
