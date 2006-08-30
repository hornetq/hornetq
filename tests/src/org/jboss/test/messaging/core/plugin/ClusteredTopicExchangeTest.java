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
package org.jboss.test.messaging.core.plugin;

import java.util.List;

import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.local.MessageQueue;
import org.jboss.messaging.core.message.MessageFactory;
import org.jboss.messaging.core.plugin.contract.Exchange;
import org.jboss.messaging.core.plugin.exchange.Binding;
import org.jboss.messaging.core.plugin.exchange.ClusteredTopicExchange;
import org.jgroups.Channel;
import org.jgroups.JChannel;

/**
 * 
 * A ClusteredTopicExchangeTest

 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class ClusteredTopicExchangeTest extends TopicExchangeTest
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------

   public ClusteredTopicExchangeTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();     
            
   }

   public void tearDown() throws Exception
   {           
      super.tearDown();
   }
   
   public void testClusteredTopic() throws Exception
   {
      Exchange exchange1 = null;
      
      Exchange exchange2 = null;
      
      try
      {         
         exchange1 = createExchange("node1", "testgroup");
         
         Binding binding1 = exchange1.bindQueue("sub1", "topic1", null, false, true, ms, pm, 20000, 100, 100);
         
         exchange2 = createExchange("node2", "testgroup");
         
         Binding binding2 = exchange1.bindQueue("sub2", "topic1", null, false, true, ms, pm, 20000, 100, 100);
         
         Binding binding3 = exchange2.bindQueue("sub3", "topic1", null, false, true, ms, pm, 20000, 100, 100);
         
         Message msg1 = MessageFactory.createCoreMessage(1);      
         MessageReference ref1 = ms.reference(msg1);
         
         boolean routed = exchange1.route(ref1, "topic1", null); 
         
         //For non reliable messages may take little while for messages to arrive
         //Since they are just put on the bus and the caller continues - so we sleep
         Thread.sleep(500);
         
         checkQueueContainsRef(binding1, ref1);
         checkQueueContainsRef(binding2, ref1);
         checkQueueContainsRef(binding3, ref1);
         
         log.info("Done test");
      }
      finally
      {
         if (exchange1 != null)
         {
            exchange1.stop();
         }
         
         if (exchange2 != null)
         {
            exchange2.stop();
         }
      }
   }
   
   private void checkQueueContainsRef(Binding binding, MessageReference ref)
   {
      MessageQueue queue = binding.getQueue();
      
      List refs = queue.browse();
      
      assertNotNull(refs);
      
      assertEquals(1, refs.size());
      
      Message msg = (Message)refs.get(0);
      
      assertEquals(ref.getMessageID(), msg.getMessageID());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------
   
   protected Exchange createExchange() throws Exception
   {
      ClusteredTopicExchange exchange = new ClusteredTopicExchange(sc.getDataSource(), sc.getTransactionManager());
      
      Channel controlChannel = new JChannel(JGroupsUtil.getControlStackProperties(50, 1));
      Channel dataChannel = new JChannel(JGroupsUtil.getDataStackProperties(50, 1));
      
      ((ClusteredTopicExchange)exchange).injectAttributes(controlChannel, dataChannel, "group1", "Topic", "node1", ms, im, pool, tr, pm);
      
      exchange.start();
      
      return exchange;
   }
   
   protected Exchange createExchange(String nodeId, String groupName) throws Exception
   {
      ClusteredTopicExchange exchange = new ClusteredTopicExchange(sc.getDataSource(), sc.getTransactionManager());
      
      Channel controlChannel = new JChannel(JGroupsUtil.getControlStackProperties(50, 1));
      Channel dataChannel = new JChannel(JGroupsUtil.getDataStackProperties(50, 1));
      
      ((ClusteredTopicExchange)exchange).injectAttributes(controlChannel, dataChannel, groupName, "Topic", nodeId, ms, im, pool, tr, pm);
      
      exchange.start();
      
      return exchange;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}



