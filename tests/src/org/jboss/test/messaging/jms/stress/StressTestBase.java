/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
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
package org.jboss.test.messaging.jms.stress;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.naming.InitialContext;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * 
 * Base class for stress tests
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version 1.1
 *
 * StressTestBase.java,v 1.1 2006/01/17 12:15:33 timfox Exp
 */
public class StressTestBase extends MessagingTestCase
{
   protected static final int NUM_PERSISTENT_MESSAGES = 200;
   
   protected static final int NUM_NON_PERSISTENT_MESSAGES = 1000;
   
   protected ConnectionFactory cf;

   protected Destination topic;

   protected Destination queue1;
   protected Destination queue2;
   protected Destination queue3;
   protected Destination queue4;
   
   protected Destination topic1;
   protected Destination topic2;
   protected Destination topic3;
   protected Destination topic4;



   public StressTestBase(String name)
   {
      super(name);
   }

   public void setUp() throws Exception
   {
      super.setUp();
      
      ServerManagement.start("all");
      
      ServerManagement.deployQueue("Queue1");
      ServerManagement.deployQueue("Queue2");
      ServerManagement.deployQueue("Queue3");
      ServerManagement.deployQueue("Queue4");
      
      ServerManagement.deployTopic("Topic1");
      ServerManagement.deployTopic("Topic2");
      ServerManagement.deployTopic("Topic3");
      ServerManagement.deployTopic("Topic4");
      
      
      InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment());
      cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
      
      
      queue1 = (Destination)ic.lookup("/queue/Queue1");
      queue2 = (Destination)ic.lookup("/queue/Queue2");
      queue3 = (Destination)ic.lookup("/queue/Queue3");
      queue4 = (Destination)ic.lookup("/queue/Queue4");

      topic1 = (Destination)ic.lookup("/topic/Topic1");
      topic2 = (Destination)ic.lookup("/topic/Topic2");
      topic3 = (Destination)ic.lookup("/topic/Topic3");
      topic4 = (Destination)ic.lookup("/topic/Topic4");
            
   }

   public void tearDown() throws Exception
   {
            
      ServerManagement.undeployQueue("Queue1");
      ServerManagement.undeployQueue("Queue2");
      ServerManagement.undeployQueue("Queue3");
      ServerManagement.undeployQueue("Queue4");

      ServerManagement.undeployQueue("Topic1");
      ServerManagement.undeployQueue("Topic2");
      ServerManagement.undeployQueue("Topic3");
      ServerManagement.undeployQueue("Topic4");
      
      super.tearDown();            
   }
   
   protected void runRunners(Runner[] runners) throws Exception
   {
      Thread[] threads = new Thread[runners.length];
      for (int i = 0; i < runners.length; i++)
      {
         threads[i] = new Thread(runners[i]);
         threads[i].start();
      }
      
      for (int i = 0; i < runners.length; i++)
      {
         threads[i].join();
      }
      
      for (int i = 0; i < runners.length; i++)
      {
         if (runners[i].isFailed())
         {
            fail("Runner " + i + " failed");
         }
      }
 
   }
}
