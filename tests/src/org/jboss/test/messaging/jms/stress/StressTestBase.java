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
import javax.jms.XASession;

import org.jboss.jms.client.JBossSession;
import org.jboss.test.messaging.JBMServerTestCase;

/**
 * 
 * Base class for stress tests
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
*/
public class StressTestBase extends JBMServerTestCase
{
   protected static final int NUM_PERSISTENT_MESSAGES = 4000;
   
   protected static final int NUM_NON_PERSISTENT_MESSAGES = 6000;
   
   protected static final int NUM_PERSISTENT_PRESEND = 5000;
   
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
      
      //We test with small values for paging params to really stress it
      
      final int fullSize = 2000;
      
      final int pageSize = 300;
      
      final int downCacheSize = 300;
      
//      createQueue("Queue1", fullSize, pageSize, downCacheSize);
//      createQueue("Queue2", fullSize, pageSize, downCacheSize);
//      createQueue("Queue3", fullSize, pageSize, downCacheSize);
//      createQueue("Queue4", fullSize, pageSize, downCacheSize);
//      
//      deployTopic("Topic1", fullSize, pageSize, downCacheSize);
//      deployTopic("Topic2", fullSize, pageSize, downCacheSize);
//      deployTopic("Topic3", fullSize, pageSize, downCacheSize);
//      deployTopic("Topic4", fullSize, pageSize, downCacheSize);
//            
//      InitialContext ic = getInitialContext();
//      cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
//      
//      queue1 = (Destination)ic.lookup("/queue/Queue1");
//      queue2 = (Destination)ic.lookup("/queue/Queue2");
//      queue3 = (Destination)ic.lookup("/queue/Queue3");
//      queue4 = (Destination)ic.lookup("/queue/Queue4");
//
//      topic1 = (Topic)ic.lookup("/topic/Topic1");
//      topic2 = (Topic)ic.lookup("/topic/Topic2");
//      topic3 = (Topic)ic.lookup("/topic/Topic3");
//      topic4 = (Topic)ic.lookup("/topic/Topic4");            
   }

   public void tearDown() throws Exception
   {
   	if (checkNoMessageData())
   	{
   		fail("Message data still exists");
   	}
   	
//      undeployQueue("Queue1");
//      undeployQueue("Queue2");
//      undeployQueue("Queue3");
//      undeployQueue("Queue4");
//
//      undeployTopic("Topic1");
//      undeployTopic("Topic2");
//      undeployTopic("Topic3");
//      undeployTopic("Topic4");
      
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
   
   protected void tweakXASession(XASession sess)
   {
     
   }
}
