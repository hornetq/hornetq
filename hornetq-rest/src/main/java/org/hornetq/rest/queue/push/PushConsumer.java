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
package org.hornetq.rest.queue.push;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.jms.client.ConnectionFactoryOptions;
import org.hornetq.jms.client.SelectorTranslator;
import org.hornetq.rest.HornetQRestLogger;
import org.hornetq.rest.queue.push.xml.PushRegistration;

import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public class PushConsumer
{
   protected PushRegistration registration;
   protected ClientSessionFactory factory;
   protected List<ClientSession> sessions;
   protected List<ClientConsumer> consumers;
   protected String destination;
   protected String id;
   protected PushStrategy strategy;
   protected PushStore store;

   private ConnectionFactoryOptions jmsOptions;

   public PushConsumer(ClientSessionFactory factory, String destination, String id, PushRegistration registration, PushStore store, ConnectionFactoryOptions jmsOptions)
   {
      this.factory = factory;
      this.destination = destination;
      this.id = id;
      this.registration = registration;
      this.store = store;
      this.jmsOptions = jmsOptions;
   }

   public PushStrategy getStrategy()
   {
      return strategy;
   }

   public PushRegistration getRegistration()
   {
      return registration;
   }

   public String getDestination()
   {
      return destination;
   }

   public void start() throws Exception
   {
      if (registration.getTarget().getClassName() != null)
      {
         Class clazz = Thread.currentThread().getContextClassLoader().loadClass(registration.getTarget().getClassName());
         strategy = (PushStrategy) clazz.newInstance();
      }
      else if (registration.getTarget().getRelationship() != null)
      {
         if (registration.getTarget().getRelationship().equals("destination"))
         {
            strategy = new HornetQPushStrategy();
         }
         else if (registration.getTarget().getRelationship().equals("template"))
         {
            strategy = new UriTemplateStrategy();
         }
      }
      if (strategy == null)
      {
         strategy = new UriStrategy();
      }
      strategy.setRegistration(registration);
      strategy.setJmsOptions(jmsOptions);
      strategy.start();

      sessions = new ArrayList<ClientSession>();
      consumers = new ArrayList<ClientConsumer>();

      for (int i = 0; i < registration.getSessionCount(); i++)
      {
         ClientSession session = factory.createSession(false, false, 0);

         ClientConsumer consumer;

         if (registration.getSelector() != null)
         {
            consumer = session.createConsumer(destination, SelectorTranslator.convertToHornetQFilterString(registration.getSelector()));
         }
         else
         {
            consumer = session.createConsumer(destination);
         }
         consumer.setMessageHandler(new PushConsumerMessageHandler(this, session));
         session.start();
         HornetQRestLogger.LOGGER.startingPushConsumer(registration.getTarget());

         consumers.add(consumer);
         sessions.add(session);
      }
   }

   public void stop()
   {
      for (ClientSession session : sessions)
      {
         try
         {
            if (session != null)
            {
               session.close();
            }
         }
         catch (HornetQException e)
         {

         }
      }

      try
      {
         if (strategy != null)
         {
            strategy.stop();
         }
      }
      catch (Exception e)
      {
      }
   }

   public void disableFromFailure()
   {
      registration.setEnabled(false);
      try
      {
         if (registration.isDurable())
         {
            store.update(registration);
         }
      }
      catch (Exception e)
      {
         HornetQRestLogger.LOGGER.errorUpdatingStore(e);
      }
      stop();
   }
}
