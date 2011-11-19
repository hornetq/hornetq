/*
 * Copyright 2009 Red Hat, Inc.
 *  Red Hat licenses this file to you under the Apache License, version
 *  2.0 (the "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied.  See the License for the specific language governing
 *  permissions and limitations under the License.
 */
package org.hornetq.tests.integration.ra;

import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.integration.jboss.recovery.AS7RecoveryRegistry;
import org.hornetq.jms.client.HornetQMessage;
import org.hornetq.tests.util.ServiceTestBase;
import org.jboss.msc.service.*;
import org.jboss.msc.value.Value;
import org.jboss.tm.XAResourceRecovery;
import org.jboss.tm.XAResourceRecoveryRegistry;

import javax.jms.Message;
import javax.jms.MessageListener;
import javax.resource.ResourceException;
import javax.resource.spi.BootstrapContext;
import javax.resource.spi.UnavailableException;
import javax.resource.spi.XATerminator;
import javax.resource.spi.endpoint.MessageEndpoint;
import javax.resource.spi.endpoint.MessageEndpointFactory;
import javax.resource.spi.work.*;
import javax.transaction.xa.XAResource;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         Created Jul 6, 2010
 */
public abstract class HornetQRATestBase  extends ServiceTestBase
{
   protected Configuration configuration;

   protected HornetQServer server;
   
   protected ServerLocator locator;

   protected static final String MDBQUEUE = "mdbQueue";
   protected static final String MDBQUEUEPREFIXED = "jms.queue.mdbQueue";
   protected static final SimpleString MDBQUEUEPREFIXEDSIMPLE = new SimpleString("jms.queue.mdbQueue");

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      clearData();
      locator = createInVMNonHALocator();
      configuration = createDefaultConfig(true);
      configuration.setSecurityEnabled(isSecure());
      server = createServer(true, configuration);
      server.start();
      server.createQueue(MDBQUEUEPREFIXEDSIMPLE, MDBQUEUEPREFIXEDSIMPLE, null, true, false);
      AS7RecoveryRegistry.container = new DummyServiceContainer();
   }

   @Override
   protected void tearDown() throws Exception
   {
      locator.close();
      
      locator = null;
      if (server != null)
      {
         try
         {
            server.stop();
            server = null;
         }
         catch (Exception e)
         {
            // ignore
         }
      }
      super.tearDown();
   }
    public abstract boolean isSecure();


   class DummyMessageEndpointFactory implements MessageEndpointFactory
   {
      private DummyMessageEndpoint endpoint;

      private final boolean isDeliveryTransacted;

      public DummyMessageEndpointFactory(DummyMessageEndpoint endpoint, boolean deliveryTransacted)
      {
         this.endpoint = endpoint;
         isDeliveryTransacted = deliveryTransacted;
      }

      public MessageEndpoint createEndpoint(XAResource xaResource) throws UnavailableException
      {
         if(xaResource != null)
         {
            endpoint.setXAResource(xaResource);
         }
         return endpoint;
      }

      public boolean isDeliveryTransacted(Method method) throws NoSuchMethodException
      {
         return isDeliveryTransacted;
      }
   }

   class DummyMessageEndpoint implements MessageEndpoint, MessageListener
   {
      public CountDownLatch latch;

      public HornetQMessage lastMessage;

      public boolean released = false;

      public XAResource xaResource;

      public DummyMessageEndpoint(CountDownLatch latch)
      {
         this.latch = latch;
      }

      public void beforeDelivery(Method method) throws NoSuchMethodException, ResourceException
      {

      }

      public void afterDelivery() throws ResourceException
      {
         if(latch != null)
         {
            latch.countDown();
         }
         System.out.println("lastMessage = " + lastMessage);
      }

      public void release()
      {
         released = true;   
      }

      public void onMessage(Message message)
      {
         lastMessage = (HornetQMessage) message;
      }

      public void reset(CountDownLatch latch)
      {
         this.latch = latch;
         lastMessage = null;
      }

      public void setXAResource(XAResource xaResource)
      {
         this.xaResource = xaResource;
      }
   }

   class MyBootstrapContext implements BootstrapContext
   {
      WorkManager workManager = new DummyWorkManager();

      public Timer createTimer() throws UnavailableException
      {
         return null;
      }

      public WorkManager getWorkManager()
      {
         return workManager;
      }

      public XATerminator getXATerminator()
      {
         return null;
      }

      class DummyWorkManager implements WorkManager
      {
         public void doWork(Work work) throws WorkException
         {
         }

         public void doWork(Work work, long l, ExecutionContext executionContext, WorkListener workListener) throws WorkException
         {
         }

         public long startWork(Work work) throws WorkException
         {
            return 0;
         }

         public long startWork(Work work, long l, ExecutionContext executionContext, WorkListener workListener) throws WorkException
         {
            return 0;
         }

         public void scheduleWork(Work work) throws WorkException
         {
            work.run();
         }

         public void scheduleWork(Work work, long l, ExecutionContext executionContext, WorkListener workListener) throws WorkException
         {
         }
      }
   }


   public static class DummyServiceContainer implements ServiceContainer
   {
      public void shutdown()
      {
         //To change body of implemented methods use File | Settings | File Templates.
      }

      public boolean isShutdownComplete()
      {
         return false;  //To change body of implemented methods use File | Settings | File Templates.
      }

      public void addTerminateListener(TerminateListener terminateListener)
      {
         //To change body of implemented methods use File | Settings | File Templates.
      }

      public void awaitTermination() throws InterruptedException
      {
         //To change body of implemented methods use File | Settings | File Templates.
      }

      public void awaitTermination(long l, TimeUnit timeUnit) throws InterruptedException
      {
         //To change body of implemented methods use File | Settings | File Templates.
      }

      public void dumpServices()
      {
         //To change body of implemented methods use File | Settings | File Templates.
      }

      public void dumpServices(PrintStream printStream)
      {
         //To change body of implemented methods use File | Settings | File Templates.
      }

      public String getName()
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      public ServiceController<?> getRequiredService(ServiceName serviceName) throws ServiceNotFoundException
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      public ServiceController<?> getService(ServiceName serviceName)
      {
         ServiceController<XAResourceRecoveryRegistry> controller = new ServiceController<XAResourceRecoveryRegistry>()
         {
            public ServiceController<?> getParent()
            {
               return null;  //To change body of implemented methods use File | Settings | File Templates.
            }

            public ServiceContainer getServiceContainer()
            {
               return null;  //To change body of implemented methods use File | Settings | File Templates.
            }

            public Mode getMode()
            {
               return null;  //To change body of implemented methods use File | Settings | File Templates.
            }

            public boolean compareAndSetMode(Mode mode, Mode mode1)
            {
               return false;  //To change body of implemented methods use File | Settings | File Templates.
            }

            public void setMode(Mode mode)
            {
               //To change body of implemented methods use File | Settings | File Templates.
            }

            public State getState()
            {
               return null;  //To change body of implemented methods use File | Settings | File Templates.
            }

            public Substate getSubstate()
            {
               return null;  //To change body of implemented methods use File | Settings | File Templates.
            }

            public XAResourceRecoveryRegistry getValue() throws IllegalStateException
            {
               XAResourceRecoveryRegistry registry = new XAResourceRecoveryRegistry()
               {
                  public void addXAResourceRecovery(XAResourceRecovery xaResourceRecovery)
                  {
                     //To change body of implemented methods use File | Settings | File Templates.
                  }

                  public void removeXAResourceRecovery(XAResourceRecovery xaResourceRecovery)
                  {
                     //To change body of implemented methods use File | Settings | File Templates.
                  }
               };
               return registry;  //To change body of implemented methods use File | Settings | File Templates.
            }

            public Service<XAResourceRecoveryRegistry> getService() throws IllegalStateException
            {
               return null;  //To change body of implemented methods use File | Settings | File Templates.
            }

            public ServiceName getName()
            {
               return null;  //To change body of implemented methods use File | Settings | File Templates.
            }

            public ServiceName[] getAliases()
            {
               return new ServiceName[0];  //To change body of implemented methods use File | Settings | File Templates.
            }

            public void addListener(ServiceListener<? super XAResourceRecoveryRegistry> serviceListener)
            {
               //To change body of implemented methods use File | Settings | File Templates.
            }

            public void addListener(ServiceListener.Inheritance inheritance, ServiceListener<Object> objectServiceListener)
            {
               //To change body of implemented methods use File | Settings | File Templates.
            }

            public void removeListener(ServiceListener<? super XAResourceRecoveryRegistry> serviceListener)
            {
               //To change body of implemented methods use File | Settings | File Templates.
            }

            public StartException getStartException()
            {
               return null;  //To change body of implemented methods use File | Settings | File Templates.
            }

            public void retry()
            {
               //To change body of implemented methods use File | Settings | File Templates.
            }

            public Set<ServiceName> getImmediateUnavailableDependencies()
            {
               return null;  //To change body of implemented methods use File | Settings | File Templates.
            }
         };
         return controller;  //To change body of implemented methods use File | Settings | File Templates.
      }

      public List<ServiceName> getServiceNames()
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      public <T> ServiceBuilder<T> addServiceValue(ServiceName serviceName, Value<? extends Service<T>> value)
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      public <T> ServiceBuilder<T> addService(ServiceName serviceName, Service<T> tService)
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      public ServiceTarget addListener(ServiceListener<Object> objectServiceListener)
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      public ServiceTarget addListener(ServiceListener<Object>... serviceListeners)
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      public ServiceTarget addListener(Collection<ServiceListener<Object>> serviceListeners)
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      public ServiceTarget addListener(ServiceListener.Inheritance inheritance, ServiceListener<Object> objectServiceListener)
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      public ServiceTarget addListener(ServiceListener.Inheritance inheritance, ServiceListener<Object>... serviceListeners)
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      public ServiceTarget addListener(ServiceListener.Inheritance inheritance, Collection<ServiceListener<Object>> serviceListeners)
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      public ServiceTarget removeListener(ServiceListener<Object> objectServiceListener)
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      public Set<ServiceListener<Object>> getListeners()
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      public ServiceTarget addDependency(ServiceName serviceName)
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      public ServiceTarget addDependency(ServiceName... serviceNames)
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      public ServiceTarget addDependency(Collection<ServiceName> serviceNames)
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      public ServiceTarget removeDependency(ServiceName serviceName)
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      public Set<ServiceName> getDependencies()
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      public ServiceTarget subTarget()
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      public BatchServiceTarget batchTarget()
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }
   }
}
