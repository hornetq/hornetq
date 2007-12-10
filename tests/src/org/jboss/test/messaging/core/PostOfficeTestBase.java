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

import org.jboss.messaging.core.contract.*;
import org.jboss.messaging.core.impl.DefaultClusterNotifier;
import org.jboss.messaging.core.impl.IDManager;
import org.jboss.messaging.core.impl.message.SimpleMessageStore;
import org.jboss.messaging.core.impl.tx.Transaction;
import org.jboss.messaging.core.impl.tx.TransactionRepository;
import org.jboss.test.messaging.JBMServerTestCase;
import org.jboss.test.messaging.tools.container.ServiceContainer;
import org.jboss.test.messaging.util.CoreMessageFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 
 * A PostOfficeTestBase
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public class PostOfficeTestBase extends JBMServerTestCase
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

      /*String configFilePath = sc.getPersistenceConfigFile(true);

      // TODO (ovidiu) we're temporarily ignoring the multiplex option, it doesn't work well
      boolean ignoreMultiplexer = true;
      ChannelFactory jChannelFactory =
         new ClusteredPersistenceServiceConfigFileJChannelFactory(configFilePath,
                                                                  ignoreMultiplexer,
                                                                  sc.getMBeanServer());*/
      
      /*MessagingPostOffice postOffice =
         new MessagingPostOffice(sc.getDataSource(), sc.getTransactionManager(),
                                 sc.getPostOfficeSQLProperties(), true, nodeID,
                                 "Clustered", ms, pm, tr, ff, cf, idm, cn,
                                 groupName, jChannelFactory,
                                 stateTimeout, castTimeout, true, 100);
      
      postOffice.start();
*/
      return null;//postOffice;
   }
   
   protected static PostOffice createNonClusteredPostOffice(ServiceContainer sc, MessageStore ms, TransactionRepository tr,
   		                                                   PersistenceManager pm)
   	throws Exception
   {
   	FilterFactory ff = new SimpleFilterFactory();
   	ConditionFactory cf = new SimpleConditionFactory();
      IDManager idm = new IDManager("channel_id", 10, pm);
      ClusterNotifier cn = new DefaultClusterNotifier();

   	/*MessagingPostOffice postOffice =
   		new MessagingPostOffice(sc.getDataSource(), sc.getTransactionManager(),
   				                  sc.getPostOfficeSQLProperties(),
   									   true, 1, "NonClustered", ms, pm, tr, ff, cf, idm, cn);

   	postOffice.start();

   	return postOffice;*/            return null;
   }   

   // Attributes -----------------------------------------------------------------------------------

   protected ServiceContainer sc;

   protected IDManager channelIDManager;
   
   protected IDManager transactionIDManager;

      
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
		return createNonClusteredPostOffice(sc, ms, tr, getPersistenceManager());
	}
	
	protected PostOffice createClusteredPostOffice(int nodeID) throws Exception
   {
      //System property provides group name so we can run concurrently in QA lab
      String groupName = System.getProperty("jboss.messaging.groupname");
      
      log.info("Creating clusteredPostOffice node " + nodeID);
      
      return createClusteredPostOffice(nodeID, groupName == null ? "testgroup" : groupName, 5000, 5000, sc, ms, tr, getPersistenceManager());
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


      /*pm =
         new JDBCPersistenceManager(sc.getDataSource(), sc.getTransactionManager(),
                  sc.getPersistenceManagerSQLProperties(),
                  true, true, true, false, 100, !sc.getDatabaseName().equals("oracle"));
      ((JDBCPersistenceManager)pm).injectNodeID(1);*/

      transactionIDManager = new IDManager("TRANSACTION_ID", 10, getPersistenceManager());
      transactionIDManager.start();

      ms = new SimpleMessageStore();
      ms.start();

      tr = new TransactionRepository(getPersistenceManager(), ms, transactionIDManager);
      tr.start();

      channelIDManager = new IDManager("CHANNEL_ID", 10, getPersistenceManager());
      channelIDManager.start();

      conditionFactory = new SimpleConditionFactory();

      log.debug("setup done");
   }

   protected void tearDown() throws Exception
   {
   	Thread.sleep(2000);

       try
       {
           if (this.checkNoMessageData())
           {
               fail("Message data still exists");
           }

           if (this.checkNoBindingData())
           {
               fail("Binding data still exists");
           }
       }
       finally
       {
          tr.stop();
          ms.stop();
          transactionIDManager.stop();
          channelIDManager.stop();

          super.tearDown();
       }


   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}


