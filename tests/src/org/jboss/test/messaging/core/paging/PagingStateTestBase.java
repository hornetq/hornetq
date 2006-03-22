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
package org.jboss.test.messaging.core.paging;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.naming.InitialContext;
import javax.sql.DataSource;
import javax.transaction.TransactionManager;

import org.jboss.messaging.core.Channel;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.SimpleDelivery;
import org.jboss.messaging.core.State;
import org.jboss.messaging.core.plugin.JDBCPersistenceManager;
import org.jboss.messaging.core.plugin.SimpleMessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.messaging.core.tx.TransactionRepository;
import org.jboss.messaging.core.tx.XidImpl;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.jmx.ServiceContainer;
import org.jboss.tm.TransactionManagerService;
import org.jboss.util.id.GUID;

/**
 * 
 * A PagingStateTestBase.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version 1.1
 *
 * PagingStateTestBase.java,v 1.1 2006/03/22 10:23:35 timfox Exp
 */
public class PagingStateTestBase extends MessagingTestCase
{
   // Constants -----------------------------------------------------


   // Static --------------------------------------------------------
   
      
   // Attributes ----------------------------------------------------

   protected ServiceContainer sc;
   protected PersistenceManager pm;
   protected SimpleMessageStore ms;
   protected TransactionRepository tr;

   // Constructors --------------------------------------------------

   public PagingStateTestBase(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------
   
   public void testEmpty()
   {
      
   }

   public void setUp() throws Exception
   {
      super.setUp();

      sc = new ServiceContainer("all,-remoting,-security");
      sc.start();

      pm =
         new JDBCPersistenceManager(sc.getDataSource(), sc.getTransactionManager());

      ((JDBCPersistenceManager)pm).start();

      ms = new SimpleMessageStore("store1");
      
      tr = new TransactionRepository();
      
      tr.start(pm);
      
   }
   
   
   public void tearDown() throws Exception
   {
      ((JDBCPersistenceManager)pm).stop();
      pm = null;
      sc.stop();
      sc = null;
      ms = null;
      
      super.tearDown();
   }
   
   protected Transaction createXATx() throws Exception
   {
      XidImpl xid = new XidImpl(new GUID().toString().getBytes(), 345, new GUID().toString().getBytes());
      
      return tr.createTransaction(xid);
   }

   protected void assertSameIds(List ids, MessageReference[] refs, int start, int end)
   {
      assertNotNull(ids);
      assertEquals(ids.size(), end - start + 1);
      Iterator iter = ids.iterator();
      int i = start;
      while (iter.hasNext())
      {
         Long id = (Long)iter.next();
         assertEquals(id.longValue(), refs[i].getMessageID());
         i++;
      }
   }
   

   protected void consume(Channel channel, State state, int consumeCount,
         MessageReference[] refs, int num)
      throws Throwable
   {
      for (int i = 0; i < num; i++)
      {
         MessageReference ref = state.removeFirstInMemory();
         assertNotNull(ref);
         assertNotNull(ref.getMessage());
         assertEquals(refs[consumeCount + i].getMessageID(), ref.getMessageID());
         Delivery del = new SimpleDelivery(channel, ref, false);
         state.addDelivery(del);
         state.acknowledge(del);
      }
   }
   
   protected void consumeInTx(Channel channel, State state, int consumeCount,
         MessageReference[] refs, int num)
      throws Throwable
   {
      Transaction tx = tr.createTransaction();
      for (int i = 0; i < num; i++)
      {
         MessageReference ref = state.removeFirstInMemory();
         assertNotNull(ref);
         assertNotNull(ref.getMessage());
         assertEquals(refs[consumeCount + i].getMessageID(), ref.getMessageID());
         Delivery del = new SimpleDelivery(channel, ref, false);
         state.addDelivery(del);
         state.acknowledge(del, tx);
      }
      tx.commit();
   }
   
   protected void consumeIn2PCTx(Channel channel, State state, int consumeCount,
         MessageReference[] refs, int num)
      throws Throwable
   {
      Transaction tx = createXATx();
      for (int i = 0; i < num; i++)
      {
         MessageReference ref = state.removeFirstInMemory();
         assertNotNull(ref);
         assertNotNull(ref.getMessage());
         assertEquals(refs[consumeCount + i].getMessageID(), ref.getMessageID());
         Delivery del = new SimpleDelivery(channel, ref, false);
         state.addDelivery(del);
         state.acknowledge(del, tx);
      }
      tx.prepare();
      tx.commit();
   }
   
   
   protected List getReferenceIds(long channelId) throws Exception
   {
      InitialContext ctx = new InitialContext();

      TransactionManager mgr = (TransactionManager)ctx.lookup(TransactionManagerService.JNDI_NAME);
      DataSource ds = (DataSource)ctx.lookup("java:/DefaultDS");
      
      javax.transaction.Transaction txOld = mgr.suspend();
      mgr.begin();

      Connection conn = ds.getConnection();
      String sql = "SELECT MESSAGEID, ORD FROM JMS_MESSAGE_REFERENCE WHERE CHANNELID=? ORDER BY ORD";
      PreparedStatement ps = conn.prepareStatement(sql);
      ps.setLong(1, channelId);
   
      ResultSet rs = ps.executeQuery();
      
      List msgIds = new ArrayList();
      
      while (rs.next())
      {
         long msgId = rs.getLong(1);
         msgIds.add(new Long(msgId));
      }
      rs.close();
      ps.close();
      conn.close();

      mgr.commit();

      if (txOld != null)
      {
         mgr.resume(txOld);
      }
      
      return msgIds;
   }
   
   protected List getUnloadedReferenceIds(long channelId) throws Exception
   {
      InitialContext ctx = new InitialContext();

      TransactionManager mgr = (TransactionManager)ctx.lookup(TransactionManagerService.JNDI_NAME);
      DataSource ds = (DataSource)ctx.lookup("java:/DefaultDS");
      
      javax.transaction.Transaction txOld = mgr.suspend();
      mgr.begin();

      Connection conn = ds.getConnection();
      String sql = "SELECT MESSAGEID, ORD FROM JMS_MESSAGE_REFERENCE WHERE CHANNELID=? AND LOADED='N' ORDER BY ORD";
      PreparedStatement ps = conn.prepareStatement(sql);
      ps.setLong(1, channelId);
   
      ResultSet rs = ps.executeQuery();
      
      List msgIds = new ArrayList();
      
      while (rs.next())
      {
         long msgId = rs.getLong(1);
         msgIds.add(new Long(msgId));
      }
      rs.close();
      ps.close();
      conn.close();

      mgr.commit();

      if (txOld != null)
      {
         mgr.resume(txOld);
      }
      
      return msgIds;
   }
   
   protected List getMessageIds() throws Exception
   {
      InitialContext ctx = new InitialContext();

      TransactionManager mgr = (TransactionManager)ctx.lookup(TransactionManagerService.JNDI_NAME);
      DataSource ds = (DataSource)ctx.lookup("java:/DefaultDS");
      
      javax.transaction.Transaction txOld = mgr.suspend();
      mgr.begin();

      Connection conn = ds.getConnection();
      String sql = "SELECT MESSAGEID FROM JMS_MESSAGE ORDER BY MESSAGEID";
      PreparedStatement ps = conn.prepareStatement(sql);
      
      ResultSet rs = ps.executeQuery();
      
      List msgIds = new ArrayList();
      
      while (rs.next())
      {
         long msgId = rs.getLong(1);
         msgIds.add(new Long(msgId));
      }
      rs.close();
      ps.close();
      conn.close();

      mgr.commit();

      if (txOld != null)
      {
         mgr.resume(txOld);
      }
      
      return msgIds;
   }
   
}
