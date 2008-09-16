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


package org.jboss.messaging.tests.stress.paging;

import java.util.HashMap;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.tests.integration.base.IntegrationTestBase;
import org.jboss.messaging.util.SimpleString;

/**
 * 
 * This is an integration-tests that will take some time to run.
 * TODO: Maybe this test belongs somewhere else?
 * 
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class MultipleDestinationPagingTest extends IntegrationTestBase
{
   
   // Constants -----------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   MessagingService service;
   
   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------
   
   
   public void testGlobalPage() throws Exception
   {
      testPage(true);
   }
   
   public void testRegularPage() throws Exception
   {
      testPage(false);
   }
   
   public void testPage(boolean globalPage) throws Exception
   {
      Configuration config = createDefaultConfig();
      
      HashMap <String, QueueSettings> settings = new HashMap<String, QueueSettings>();
      
      if (globalPage)
      {
         config.setPagingMaxGlobalSizeBytes(20*1024*1024);
      }
      else
      {
         QueueSettings setting = new QueueSettings();
         setting.setMaxSizeBytes(20*1024*1024);
         settings.put("page-adr", setting);
      }
      
      service = createService(config, settings);
      service.start();

      ClientSessionFactory factory = createFactory();
      ClientSession session = null;
      
      try
      {
   
         session = factory.createSession(false, false, false, -1, false);
         
         SimpleString address = new SimpleString("page-adr");
         SimpleString queue[] = new SimpleString[]{new SimpleString("queue1"), new SimpleString("queue2")};
         
         session.createQueue(address, queue[0], null, true, false);
         session.createQueue(address, queue[1], null, true, false);
         
         ClientProducer prod = session.createProducer(address);
         
         ClientMessage message = createBytesMessage(session, new byte[700], false);
         
         int NUMBER_OF_MESSAGES = 60000;
         
         for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
         {
            if (i % 10000 == 0) System.out.println(i);
            prod.send(message);
         }
         
         session.commit();
         
         session.start();
         
         int counters[] = new int[2];
         
         ClientConsumer consumers[] = new ClientConsumer[] {session.createConsumer(queue[0]), session.createConsumer(queue[1])};
         
         int reads = 0;
         
         while (true)
         {
            int msgs1 = readMessages(session, consumers[0], queue[0]);
            if (reads ++ == 0)
            {
               assertTrue(reads > 0 && reads < NUMBER_OF_MESSAGES);
            }
            int msgs2 = readMessages(session, consumers[1], queue[1]);
            counters[0] += msgs1;
            counters[1] += msgs2;
            
            System.out.println("msgs1 = " + msgs1 + " msgs2 = " + msgs2);
            
            if (msgs1 + msgs2 == 0)
            {
               break;
            }
         }

         consumers[0].close();
         consumers[1].close();
         
         assertEquals(NUMBER_OF_MESSAGES, counters[0]);
         assertEquals(NUMBER_OF_MESSAGES, counters[1]);
      }
      finally
      {
         session.close();
         service.stop();
      }
      
   }



   private int readMessages(ClientSession session, ClientConsumer consumer, SimpleString queue)
         throws MessagingException
   {
      session.start();
      int msgs = 0;
      
      ClientMessage msg = null;
      do
      {
         msg = consumer.receive(1000);
         session.acknowledge();
         if (msg != null)
         {
            if (++msgs % 10000 == 0)
            {
               System.out.println("received " + msgs);
               session.commit();
               
            }
         }
      } while (msg != null);
      
      session.commit();
      
      return msgs;
   }
   
   
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   protected void setUp() throws Exception
   {
      clearData();
   }
   
   
   
   
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
   
}
