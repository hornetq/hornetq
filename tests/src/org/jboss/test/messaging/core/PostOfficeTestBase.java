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
package org.jboss.test.messaging.core;

import java.util.ArrayList;
import java.util.List;

import org.jboss.messaging.core.contract.ClusterNotifier;
import org.jboss.messaging.core.contract.Condition;
import org.jboss.messaging.core.contract.ConditionFactory;
import org.jboss.messaging.core.contract.FilterFactory;
import org.jboss.messaging.core.contract.JChannelFactory;
import org.jboss.messaging.core.contract.Message;
import org.jboss.messaging.core.contract.MessageReference;
import org.jboss.messaging.core.contract.MessageStore;
import org.jboss.messaging.core.contract.PersistenceManager;
import org.jboss.messaging.core.contract.PostOffice;
import org.jboss.messaging.core.contract.Queue;
import org.jboss.messaging.core.impl.DefaultClusterNotifier;
import org.jboss.messaging.core.impl.IDManager;
import org.jboss.messaging.core.impl.JDBCPersistenceManager;
import org.jboss.messaging.core.impl.message.SimpleMessageStore;
import org.jboss.messaging.core.impl.postoffice.MessagingPostOffice;
import org.jboss.messaging.core.impl.tx.Transaction;
import org.jboss.messaging.core.impl.tx.TransactionRepository;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.core.postoffice.ClusteredPersistenceServiceConfigFileJChannelFactory;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.test.messaging.tools.jmx.ServiceContainer;
import org.jboss.test.messaging.util.CoreMessageFactory;

/**
 * 
 * A PostOfficeTestBase
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public class PostOfficeTestBase extends MessagingTestCase
{
   // Constants ------------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------
	
   protected static PostOffice createClusteredPostOffice(int nodeID,   		
			                                                String groupName,
			                                                long stateTimeout,
			                                                long castTimeout,
			                                                ServiceContainer sc,
			                                                MessageStore ms,			                                         
			                                                TransactionRepository tr,
			                                                PersistenceManager pm)
      throws Exception
   {
      FilterFactory ff = new SimpleFilterFactory();
      ConditionFactory cf = new SimpleConditionFactory();
      IDManager idm = new IDManager("channel_id", 10, pm);
      idm.start();
      ClusterNotifier cn = new DefaultClusterNotifier();

      // we're testing with JGroups stack configurations we're shipping with the release

      String configFilePath = sc.getPersistenceConfigFile(true);

      // TODO (ovidiu) we're temporarily ignoring the multiplex option, it doesn't work well
      boolean ignoreMultiplexer = true;
      JChannelFactory jChannelFactory =
         new ClusteredPersistenceServiceConfigFileJChannelFactory(configFilePath,
                                                                  ignoreMultiplexer,
                                                                  sc.getMBeanServer());

      MessagingPostOffice postOffice =
         new MessagingPostOffice(sc.getDataSource(), sc.getTransactionManager(),
                                 sc.getPostOfficeSQLProperties(), true, nodeID,
                                 "Clustered", ms, pm, tr, ff, cf, idm, cn,
                                 groupName, jChannelFactory,
                                 stateTimeout, castTimeout, true);
      
      postOffice.start();

      return postOffice;
   }
   
   protected static PostOffice createNonClusteredPostOffice(ServiceContainer sc, MessageStore ms, TransactionRepository tr,
   		                                                   PersistenceManager pm)
   	throws Exception
   {
   	FilterFactory ff = new SimpleFilterFactory();
   	ConditionFactory cf = new SimpleConditionFactory();
      IDManager idm = new IDManager("channel_id", 10, pm);
      ClusterNotifier cn = new DefaultClusterNotifier();

   	MessagingPostOffice postOffice =
   		new MessagingPostOffice(sc.getDataSource(), sc.getTransactionManager(),
   				                  sc.getPostOfficeSQLProperties(),
   									   true, 1, "NonClustered", ms, pm, tr, ff, cf, idm, cn);

   	postOffice.start();

   	return postOffice;
   }   

   // Attributes -----------------------------------------------------------------------------------

   protected ServiceContainer sc;

   protected IDManager channelIDManager;
   
   protected IDManager transactionIDManager;
   
   protected PersistenceManager pm;
      
   protected MessageStore ms;
   
   protected TransactionRepository tr;
   
   protected ConditionFactory conditionFactory;
   
   private static long msgCount;
   
   
   // Constructors --------------------------------------------------

   public PostOfficeTestBase(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------
   
   // Protected --------------------------------------------------------
   
	protected PostOffice createNonClusteredPostOffice() throws Exception
	{
		return createNonClusteredPostOffice(sc, ms, tr, pm);
	}
	
	protected PostOffice createClusteredPostOffice(int nodeID, String groupName,
			                                         long stateTimeout, long castTimeout) throws Exception
	{
		return createClusteredPostOffice(nodeID, groupName, stateTimeout, castTimeout, sc, ms, tr, pm);
	}
	
	protected PostOffice createClusteredPostOffice(int nodeID, String groupName) throws Exception
   {
		return createClusteredPostOffice(nodeID, groupName, 5000, 5000, sc, ms, tr, pm);
   }
   
   protected List sendMessages(String conditionText, boolean persistent, PostOffice office, int num, Transaction tx) throws Exception
   {
      List list = new ArrayList();
      
      for (int i = 0; i < num; i++)
      {         
         Message msg = CoreMessageFactory.createCoreMessage(msgCount++, persistent, null);      
         
         MessageReference ref = ms.reference(msg);         
         
         Condition condition = conditionFactory.createCondition(conditionText);
         
         boolean routed = office.route(ref, condition, null);         
         
         assertTrue(routed);
         
         list.add(msg);
      }
      
      Thread.sleep(1000);
      
      return list;
   }
   
   protected void checkContainsAndAcknowledge(Message msg, SimpleReceiver receiver, Queue queue) throws Throwable
   {
      List msgs = receiver.getMessages();
      assertNotNull(msgs);
      assertEquals(1, msgs.size());
      Message msgRec = (Message)msgs.get(0);
      assertEquals(msg.getMessageID(), msgRec.getMessageID());
      receiver.acknowledge(msgRec, null);
      msgs = queue.browse(null);
      assertNotNull(msgs);
      assertTrue(msgs.isEmpty()); 
      receiver.clear();
   }
   
   protected void checkContainsAndAcknowledge(List msgList, SimpleReceiver receiver, Queue queue) throws Throwable
   {
      List msgs = receiver.getMessages();
      assertNotNull(msgs);
      assertEquals(msgList.size(), msgs.size());
      
      for (int i = 0; i < msgList.size(); i++)
      {
         Message msgRec = (Message)msgs.get(i);
         Message msgCheck = (Message)msgList.get(i);
         assertEquals(msgCheck.getMessageID(), msgRec.getMessageID());
         receiver.acknowledge(msgRec, null);
      }
      
      msgs = queue.browse(null);
      assertNotNull(msgs);
      assertTrue(msgs.isEmpty()); 
      receiver.clear();
   }
   
   protected void checkEmpty(SimpleReceiver receiver) throws Throwable
   {
      List msgs = receiver.getMessages();
      assertNotNull(msgs);
      assertTrue(msgs.isEmpty());
   }

   protected void setUp() throws Exception
   {
      super.setUp();

      sc = new ServiceContainer("all");

      sc.start();

      pm =
         new JDBCPersistenceManager(sc.getDataSource(), sc.getTransactionManager(),
                  sc.getPersistenceManagerSQLProperties(),
                  true, true, true, false, 100);
      ((JDBCPersistenceManager)pm).injectNodeID(1);
      pm.start();

      transactionIDManager = new IDManager("TRANSACTION_ID", 10, pm);
      transactionIDManager.start();

      ms = new SimpleMessageStore();
      ms.start();

      tr = new TransactionRepository(pm, ms, transactionIDManager);
      tr.start();

      channelIDManager = new IDManager("CHANNEL_ID", 10, pm);
      channelIDManager.start();

      conditionFactory = new SimpleConditionFactory();

      log.debug("setup done");
   }

   protected void tearDown() throws Exception
   {
      if (!ServerManagement.isRemote())
      {
         sc.stop();
         sc = null;
      }
      pm.stop();
      tr.stop();
      ms.stop();
      transactionIDManager.stop();
      channelIDManager.stop();

      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}


