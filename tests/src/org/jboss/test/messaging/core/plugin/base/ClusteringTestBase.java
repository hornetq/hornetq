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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import javax.naming.InitialContext;
import javax.sql.DataSource;
import javax.transaction.TransactionManager;

import org.jboss.jms.server.QueuedExecutorPool;
import org.jboss.messaging.core.FilterFactory;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.Queue;
import org.jboss.messaging.core.plugin.IdManager;
import org.jboss.messaging.core.plugin.JDBCPersistenceManager;
import org.jboss.messaging.core.plugin.SimpleMessageStore;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.messaging.core.plugin.contract.PostOffice;
import org.jboss.messaging.core.plugin.postoffice.DefaultPostOffice;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.messaging.core.tx.TransactionRepository;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.core.SimpleFilterFactory;
import org.jboss.test.messaging.core.SimpleReceiver;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.test.messaging.tools.jmx.ServiceContainer;
import org.jboss.test.messaging.util.CoreMessageFactory;
import org.jboss.tm.TransactionManagerService;

/**
 * 
 * A PostOfficeTestBase
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class ClusteringTestBase extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected ServiceContainer sc;

   protected IdManager channelIdManager;   
   
   protected IdManager transactionIdManager;   
   
   protected PersistenceManager pm;
      
   protected MessageStore ms;
   
   protected TransactionRepository tr;
   
   protected QueuedExecutorPool pool;
   
   // Constructors --------------------------------------------------

   public ClusteringTestBase(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();

      sc = new ServiceContainer("all");
      
      sc.start();                
      
      pm =
         new JDBCPersistenceManager(sc.getDataSource(), sc.getTransactionManager(), null,
                                    true, false, true, 100);      
      pm.start();
      
      transactionIdManager = new IdManager("TRANSACTION_ID", 10, pm);
      transactionIdManager.start();
      
      tr = new TransactionRepository(pm, transactionIdManager);
      tr.start();
      
      ms = new SimpleMessageStore();
      ms.start();
      
      pool = new QueuedExecutorPool(10);
      
      channelIdManager = new IdManager("CHANNEL_ID", 10, pm);
      channelIdManager.start();
            
      log.debug("setup done");
   }

   public void tearDown() throws Exception
   {      
      if (!ServerManagement.isRemote())
      {
         sc.stop();
         sc = null;
      }
      pm.stop();
      tr.stop();
      ms.stop();
      transactionIdManager.stop();
      channelIdManager.stop();
      
      super.tearDown();
   }
   
   // Public --------------------------------------------------------     
   
   protected PostOffice createPostOffice() throws Exception
   {
      FilterFactory ff = new SimpleFilterFactory();
      
      DefaultPostOffice postOffice = 
         new DefaultPostOffice(sc.getDataSource(), sc.getTransactionManager(),
                            null, true, 1, "Simple", ms, pm, tr, ff, pool);
      
      postOffice.start();      
      
      return postOffice;
   }
   
   protected boolean checkNoBindingData() throws Exception
   {
      InitialContext ctx = new InitialContext();

      TransactionManager mgr = (TransactionManager)ctx.lookup(TransactionManagerService.JNDI_NAME);
      DataSource ds = (DataSource)ctx.lookup("java:/DefaultDS");
      
      javax.transaction.Transaction txOld = mgr.suspend();
      mgr.begin();
      
      Connection conn = null;
      
      PreparedStatement ps = null;
      
      ResultSet rs = null;

      try
      {
         conn = ds.getConnection();
         String sql = "SELECT * FROM JMS_POSTOFFICE";
         ps = conn.prepareStatement(sql);
         
         rs = ps.executeQuery();
         
         return rs.next();
      }
      finally
      {
         if (rs != null) rs.close();
         
         if (ps != null) ps.close();
         
         if (conn != null) conn.close();
         
         mgr.commit();

         if (txOld != null)
         {
            mgr.resume(txOld);
         }
                  
      } 
   }
   
   protected boolean checkNoMessageData() throws Exception
   {
      InitialContext ctx = new InitialContext();

      TransactionManager mgr = (TransactionManager)ctx.lookup(TransactionManagerService.JNDI_NAME);
      DataSource ds = (DataSource)ctx.lookup("java:/DefaultDS");
      
      javax.transaction.Transaction txOld = mgr.suspend();
      mgr.begin();
      
      Connection conn = null;
      
      PreparedStatement ps = null;
      
      ResultSet rs = null;

      try
      {
         conn = ds.getConnection();
         String sql = "SELECT * FROM JMS_MESSAGE_REFERENCE";
         ps = conn.prepareStatement(sql);
         
         rs = ps.executeQuery();
         
         boolean exists = rs.next();
         
         if (!exists)
         {
            rs.close();
            
            ps.close();
            
            ps = conn.prepareStatement("SELECT * FROM JMS_MESSAGE");
            
            rs = ps.executeQuery();
           
            exists = rs.next();
         }
         
         return exists;
      }
      finally
      {
         if (rs != null) rs.close();
         
         if (ps != null) ps.close();
         
         if (conn != null) conn.close();
         
         mgr.commit();

         if (txOld != null)
         {
            mgr.resume(txOld);
         }
                  
      } 
   }
   
   private static long msgCount;
   
   protected List sendMessages(String condition, boolean persistent, PostOffice office, int num, Transaction tx) throws Exception
   {
      List list = new ArrayList();
      
      for (int i = 0; i < num; i++)
      {         
         Message msg = CoreMessageFactory.createCoreMessage(msgCount++, persistent, null);      
         
         MessageReference ref = ms.reference(msg);         
         
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
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}


