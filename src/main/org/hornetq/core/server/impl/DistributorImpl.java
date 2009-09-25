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

import java.util.ArrayList;
import java.util.List;

import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.Consumer;
import org.hornetq.core.server.Distributor;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public abstract class DistributorImpl implements Distributor
{
   private static final Logger log = Logger.getLogger(DistributorImpl.class);

   protected final List<Consumer> consumers = new ArrayList<Consumer>();

   public void addConsumer(Consumer consumer)
   {
      consumers.add(consumer);
   }

   public boolean removeConsumer(Consumer consumer)
   {
      return consumers.remove(consumer);
   }

   public int getConsumerCount()
   {
      return consumers.size();
   }

   public List<Consumer> getConsumers()
   {
      return consumers;
   }
}
