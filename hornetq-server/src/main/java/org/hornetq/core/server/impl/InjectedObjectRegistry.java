/*
 * Copyright 2005-2014 Red Hat, Inc.
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import org.hornetq.api.core.Interceptor;
import org.hornetq.core.server.ConnectorServiceFactory;

/**
 * @author <a href="mailto:mtaylor@redhat.com">Martyn Taylor</a>
 */

public class InjectedObjectRegistry
{
   private ExecutorService executorService;

   private ScheduledExecutorService scheduledExecutorService;

   /* We are using a List rather than HashMap here as HornetQ allows multiple instances of the same class to be added
   * to the interceptor list
   */
   private List<Interceptor> incomingInterceptors;

   private List<Interceptor> outgoingInterceptors;

   private Map<String, ConnectorServiceFactory> connectorServiceFactories;

   public InjectedObjectRegistry()
   {
      this.incomingInterceptors = Collections.synchronizedList(new ArrayList<Interceptor>());
      this.outgoingInterceptors = Collections.synchronizedList(new ArrayList<Interceptor>());
      this.connectorServiceFactories = new ConcurrentHashMap<String, ConnectorServiceFactory>();
   }

   public ExecutorService getExecutorService()
   {
      return executorService;
   }

   public void setExecutorService(ExecutorService executorService)
   {
      this.executorService = executorService;
   }

   public ScheduledExecutorService getScheduledExecutorService()
   {
      return scheduledExecutorService;
   }

   public void setScheduledExecutorService(ScheduledExecutorService scheduledExecutorService)
   {
      this.scheduledExecutorService = scheduledExecutorService;
   }

   public void addConnectorServiceFactory(ConnectorServiceFactory connectorServiceFactory)
   {
      connectorServiceFactories.put(connectorServiceFactory.getClass().getCanonicalName(), connectorServiceFactory);
   }

   public void removeConnectorServiceFactory(String className)
   {
      connectorServiceFactories.remove(className);
   }

   public ConnectorServiceFactory getConnectorServiceFactory(String className)
   {
      return connectorServiceFactories.get(className);
   }

   public Map<String, ConnectorServiceFactory> getConnectorServiceFactories()
   {
      return Collections.unmodifiableMap(connectorServiceFactories);
   }

   public void addIncomingInterceptor(Interceptor interceptor)
   {
      incomingInterceptors.add(interceptor);
   }

   public void removeIncomingInterceptor(Interceptor interceptor)
   {
      incomingInterceptors.remove(interceptor);
   }

   public List<Interceptor> getIncomingInterceptors()
   {
      return Collections.unmodifiableList(incomingInterceptors);
   }

   public void addOutgoingInterceptor(Interceptor interceptor)
   {
      outgoingInterceptors.add(interceptor);
   }

   public void removeOutgoingInterceptor(Interceptor interceptor)
   {
      outgoingInterceptors.remove(interceptor);
   }

   public List<Interceptor> getOutgoingInterceptors()
   {
      return Collections.unmodifiableList(outgoingInterceptors);
   }
}
