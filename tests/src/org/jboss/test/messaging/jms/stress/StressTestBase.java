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
import javax.jms.Topic;
import javax.naming.InitialContext;

import org.jboss.messaging.util.LockMap;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * 
 * Base class for stress tests
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
*/
public class StressTestBase extends MessagingTestCase
{
   protected static final int NUM_PERSISTENT_MESSAGES = 4000;
   
   protected static final int NUM_NON_PERSISTENT_MESSAGES = 6000;
   
   protected static final int NUM_PERSISTENT_PRESEND = 2000;
   
   protected static final int NUM_NON_PERSISTENT_PRESEND = 3000;
   
   protected ConnectionFactory cf;

   protected Destination topic;

   protected Destination queue1;
   protected Destination queue2;
   protected Destination queue3;
   protected Destination queue4;
   
   protected Topic topic1;
   protected Topic topic2;
   protected Topic topic3;
   protected Topic topic4;

   public StressTestBase(String name)
   {
      super(name);
   }

   public void setUp() throws Exception
   {
      super.setUp();
      
      ServerManagement.start("all");            
      
      //We test with small values for paging params to really stress it
      
      final int fullSize = 1000;
      
      final int pageSize = 100;
      
      final int downCacheSize = 100;
      
      ServerManagement.deployQueue("Queue1", fullSize, pageSize, downCacheSize);
      ServerManagement.deployQueue("Queue2", fullSize, pageSize, downCacheSize);
      ServerManagement.deployQueue("Queue3", fullSize, pageSize, downCacheSize);
      ServerManagement.deployQueue("Queue4", fullSize, pageSize, downCacheSize);
      
      ServerManagement.deployTopic("Topic1", fullSize, pageSize, downCacheSize);
      ServerManagement.deployTopic("Topic2", fullSize, pageSize, downCacheSize);
      ServerManagement.deployTopic("Topic3", fullSize, pageSize, downCacheSize);
      ServerManagement.deployTopic("Topic4", fullSize, pageSize, downCacheSize);
            
      InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment());
      cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
      
      queue1 = (Destination)ic.lookup("/queue/Queue1");
      queue2 = (Destination)ic.lookup("/queue/Queue2");
      queue3 = (Destination)ic.lookup("/queue/Queue3");
      queue4 = (Destination)ic.lookup("/queue/Queue4");

      topic1 = (Topic)ic.lookup("/topic/Topic1");
      topic2 = (Topic)ic.lookup("/topic/Topic2");
      topic3 = (Topic)ic.lookup("/topic/Topic3");
      topic4 = (Topic)ic.lookup("/topic/Topic4");            
   }

   public void tearDown() throws Exception
   {
      assertEquals(0, LockMap.instance.getSize());
            
      ServerManagement.undeployQueue("Queue1");
      ServerManagement.undeployQueue("Queue2");
      ServerManagement.undeployQueue("Queue3");
      ServerManagement.undeployQueue("Queue4");

      ServerManagement.undeployTopic("Topic1");
      ServerManagement.undeployTopic("Topic2");
      ServerManagement.undeployTopic("Topic3");
      ServerManagement.undeployTopic("Topic4");
      
      ServerManagement.stop();
      
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
            log.error("runner failed");
         }
      } 
   }
}
