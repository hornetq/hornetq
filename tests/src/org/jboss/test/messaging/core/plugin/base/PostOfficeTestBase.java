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
package org.jboss.test.messaging.core.plugin.base;

import java.util.ArrayList;
import java.util.List;

import org.jboss.jms.server.QueuedExecutorPool;
import org.jboss.messaging.core.FilterFactory;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.Queue;
import org.jboss.messaging.core.plugin.IDManager;
import org.jboss.messaging.core.plugin.JDBCPersistenceManager;
import org.jboss.messaging.core.plugin.SimpleMessageStore;
import org.jboss.messaging.core.plugin.contract.Condition;
import org.jboss.messaging.core.plugin.contract.ConditionFactory;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.messaging.core.plugin.contract.PostOffice;
import org.jboss.messaging.core.plugin.contract.ClusteredPostOffice;
import org.jboss.messaging.core.plugin.contract.FailoverMapper;
import org.jboss.messaging.core.plugin.postoffice.DefaultPostOffice;
import org.jboss.messaging.core.plugin.postoffice.cluster.MessagePullPolicy;
import org.jboss.messaging.core.plugin.postoffice.cluster.NullMessagePullPolicy;
import org.jboss.messaging.core.plugin.postoffice.cluster.ClusterRouterFactory;
import org.jboss.messaging.core.plugin.postoffice.cluster.DefaultRouterFactory;
import org.jboss.messaging.core.plugin.postoffice.cluster.DefaultFailoverMapper;
import org.jboss.messaging.core.plugin.postoffice.cluster.DefaultClusteredPostOffice;
import org.jboss.messaging.core.plugin.postoffice.cluster.jchannelfactory.JChannelFactory;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.messaging.core.tx.TransactionRepository;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.core.SimpleConditionFactory;
import org.jboss.test.messaging.core.SimpleFilterFactory;
import org.jboss.test.messaging.core.SimpleReceiver;
import org.jboss.test.messaging.core.plugin.postoffice.cluster.ClusteredPersistenceServiceConfigFileJChannelFactory;
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

   protected static ClusteredPostOffice createClusteredPostOffice(int nodeID,
                                                                  String groupName,
                                                                  ServiceContainer sc,
                                                                  MessageStore ms,
                                                                  PersistenceManager pm,
                                                                  TransactionRepository tr,
                                                                  QueuedExecutorPool pool)
      throws Exception
   {
      return createClusteredPostOffice(nodeID, groupName, 5000, 5000, new NullMessagePullPolicy(),
                                       sc, ms, pm, tr, pool);
   }


   protected static ClusteredPostOffice createClusteredPostOffice(int nodeID,
                                                                  String groupName,
                                                                  long stateTimeout,
                                                                  long castTimeout,
                                                                  MessagePullPolicy pullPolicy,
                                                                  ServiceContainer sc,
                                                                  MessageStore ms,
                                                                  PersistenceManager pm,
                                                                  TransactionRepository tr,
                                                                  QueuedExecutorPool pool)
      throws Exception
   {
      FilterFactory ff = new SimpleFilterFactory();
      ClusterRouterFactory rf = new DefaultRouterFactory();
      FailoverMapper mapper = new DefaultFailoverMapper();
      ConditionFactory cf = new SimpleConditionFactory();

      // we're testing with priority JGroups stack configurations we're shipping with the release

      // TODO (ovidiu) we're currently using the mysql configuration file. We could refine this even
      //      further by actually figuring out what database we're currently running on, and use
      //      that file. Useful when we'll run database matrix tests.
      //      See http://jira.jboss.org/jira/browse/JBMESSAGING-793
      String configFilePath = "server/default/deploy/clustered-mysql-persistence-service.xml";

      // TODO (ovidiu) we're temporarily ignoring the multiplex option, it doesn't work well
      boolean ignoreMultiplexer = true;
      JChannelFactory jChannelFactory =
         new ClusteredPersistenceServiceConfigFileJChannelFactory(configFilePath,
                                                                  ignoreMultiplexer,
                                                                  sc.getMBeanServer());

      DefaultClusteredPostOffice postOffice =
         new DefaultClusteredPostOffice(sc.getDataSource(), sc.getTransactionManager(),
                                        sc.getClusteredPostOfficeSQLProperties(), true, nodeID,
                                        "Clustered", ms, pm, tr, ff, cf, pool,
                                        groupName, jChannelFactory,
                                        stateTimeout, castTimeout, pullPolicy, rf, mapper, 1000);
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
   
   protected QueuedExecutorPool pool;
   
   protected ConditionFactory conditionFactory;
   
   // Constructors --------------------------------------------------

   public PostOfficeTestBase(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------
   
   protected PostOffice createPostOffice() throws Exception
   {
      FilterFactory ff = new SimpleFilterFactory();
      
      ConditionFactory cf= new SimpleConditionFactory();
      
      DefaultPostOffice postOffice = 
         new DefaultPostOffice(sc.getDataSource(), sc.getTransactionManager(),
                            sc.getPostOfficeSQLProperties(), true, 1, "Simple", ms, pm, tr, ff, cf, pool);
      
      postOffice.start();      
      
      return postOffice;
   }
   
   
   
   private static long msgCount;
   
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
      msgs = queue.browse();
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
      
      msgs = queue.browse();
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
                  true, true, true, 100);
      pm.start();

      transactionIDManager = new IDManager("TRANSACTION_ID", 10, pm);
      transactionIDManager.start();

      ms = new SimpleMessageStore();
      ms.start();

      tr = new TransactionRepository(pm, ms, transactionIDManager);
      tr.start();

      pool = new QueuedExecutorPool(10);

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


