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
package org.jboss.test.messaging.core.persistence;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.Channel;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.MessageStore;
import org.jboss.messaging.core.message.PersistentMessageStore;
import org.jboss.messaging.core.persistence.JDBCPersistenceManager;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.messaging.core.tx.TransactionRepository;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.core.SimpleChannel;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.util.id.GUID;


/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class CoreMessageJDBCPersistenceManagerTest extends MessagingTestCase
{
   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------

   public CoreMessageJDBCPersistenceManagerTest(String name)
   {
      super(name);
   }

   public void setUp() throws Exception
   {

      super.setUp();
      ServerManagement.start("all");
  
   }

   public void tearDown() throws Exception
   {
      
      super.tearDown();
   }
   
   public void testAddReference() throws Exception
   {
      if (ServerManagement.isRemote()) return;      
      
      JDBCPersistenceManager pm = new JDBCPersistenceManager();
      pm.start();
      MessageStore ms = new PersistentMessageStore("persistentMessageStore0", pm);
      Channel channel = new SimpleChannel("channel0", ms);

      Message[] messages = createMessages();     
      
      for (int i = 0; i < messages.length; i++)
      {
         Message m = messages[i];
         
         MessageReference ref = ms.reference(m);
         
         pm.addReference(channel.getChannelID(), ref, null);
      
         List refs = pm.messageRefs(ms.getStoreID(), channel.getChannelID());
         
         assertNotNull(refs);
         assertEquals(1, refs.size());
         String messageID = (String)refs.get(0);
         
         assertEquals(ref.getMessageID(), messageID);
         
         pm.removeAllMessageData(channel.getChannelID());

         refs = pm.messageRefs(ms.getStoreID(), channel.getChannelID());
         assertTrue(refs.isEmpty());
      }
   }
   
   
   public void testRemoveReference() throws Exception
   {
      if (ServerManagement.isRemote()) return;      
      
      JDBCPersistenceManager pm = new JDBCPersistenceManager();
      pm.start();
      MessageStore ms = new PersistentMessageStore("persistentMessageStore0", pm);
      Channel channel = new SimpleChannel("channel0", ms);

      Message[] messages = createMessages();     
      
      for (int i = 0; i < messages.length; i++)
      {
         Message m = messages[i];
         
         MessageReference ref = ms.reference(m);
         
         pm.addReference(channel.getChannelID(), ref, null);
      
         List refs = pm.messageRefs(ms.getStoreID(), channel.getChannelID());
         
         assertNotNull(refs);
         assertEquals(1, refs.size());
         String messageID = (String)refs.get(0);
         
         assertEquals(ref.getMessageID(), messageID);         
         
         pm.removeReference(channel.getChannelID(), ref, null);
         
         refs = pm.messageRefs(ms.getStoreID(), channel.getChannelID());
         
         assertTrue(refs.isEmpty());
                 
      }
   }
   
   
   public void testAddRetrieveRemoveMessage() throws Exception
   {
      if (ServerManagement.isRemote()) return;
      
      JDBCPersistenceManager pm = new JDBCPersistenceManager();
      pm.start();

      Message[] messages = createMessages();

      for (int i = 0; i < messages.length; i++)
      {
         Message m = messages[i];
         String id = (String)m.getMessageID();

         pm.storeMessage(m);
         assertEquals(1, pm.getMessageReferenceCount(id));

         Message m2 = pm.retrieveMessage(id);
         assertNotNull(m2);
         checkEquivalent(m, m2);
         assertEquals(2, pm.getMessageReferenceCount(id));

         boolean removed = pm.removeMessage(id);
         assertTrue(removed);
         assertEquals(1, pm.getMessageReferenceCount(id));

         removed = pm.removeMessage(id);
         assertTrue(removed);
         assertEquals(0, pm.getMessageReferenceCount(id));

         Message m3 = pm.retrieveMessage(id);
         assertNull(m3);

         assertFalse(pm.removeMessage(id));
      }
   }

   public void testGetMessageReferences() throws Exception
   {
      if (ServerManagement.isRemote()) return;
      
      
      JDBCPersistenceManager pm = new JDBCPersistenceManager();
      pm.start();
      MessageStore ms = new PersistentMessageStore("persistentMessageStore0", pm);
      Channel channel = new SimpleChannel("channel0", ms);
      
      Message[] messages = createMessages();     
      
      for (int i = 0; i < messages.length; i++)
      {
         Message m = messages[i];
         
         MessageReference ref = ms.reference(m);
                           
         pm.addReference(channel.getChannelID(), ref, null);
      }
         
      List refs = pm.messageRefs(ms.getStoreID(), channel.getChannelID());
      assertNotNull(refs);
      assertEquals(messages.length, refs.size());
      
      for (int i = 0; i < messages.length; i++)
      {
         Message m = messages[i];         
         assertTrue(refs.contains(m.getMessageID()));         
      }
      
      pm.removeAllMessageData(channel.getChannelID());
      refs = pm.messageRefs(ms.getStoreID(), channel.getChannelID());
      assertNotNull(refs);
      assertTrue(refs.isEmpty());
 
   }
   
   
   public void testRemoveAllMessageData() throws Exception
   {
      if (ServerManagement.isRemote()) return;
      
      JDBCPersistenceManager pm = new JDBCPersistenceManager();
      pm.start();
      MessageStore ms = new PersistentMessageStore("persistentMessageStore0", pm);
      Channel channel = new SimpleChannel("channel0", ms);
      
      Message[] messages = createMessages();     
      
      for (int i = 0; i < messages.length; i++)
      {
         Message m = messages[i];
         
         MessageReference ref = ms.reference(m);
                           
         pm.addReference(channel.getChannelID(), ref, null);
      }
         
      List refs = pm.messageRefs(ms.getStoreID(), channel.getChannelID());
      assertNotNull(refs);
      assertEquals(messages.length, refs.size());
      
      for (int i = 0; i < messages.length; i++)
      {
         Message m = messages[i];         
         assertTrue(refs.contains(m.getMessageID()));         
      }
      
      pm.removeAllMessageData(channel.getChannelID());
            
      refs = pm.messageRefs(ms.getStoreID(), channel.getChannelID());
      assertNotNull(refs);
      assertTrue(refs.isEmpty());      
 
   }
   
 
   public void testCommit_NotXA_Long_NotStoreXid() throws Exception
   {
      doTransactionCommit(false, false, false);
   }
   
   public void testCommit_NotXA_Long_StoreXid() throws Exception
   {
      doTransactionCommit(false, false, true);
   }
   
   public void testCommit_NotXA_Guid_NotStoreXid() throws Exception
   {
      doTransactionCommit(false, true, false);
   }
   
   public void testCommit_NotXA_Guid_StoreXid() throws Exception
   {
      doTransactionCommit(false, true, true);
   }
         
   public void testCommit_XA_Long_NotStoreXid() throws Exception
   {
      doTransactionCommit(true, false, false);
   }
   
   public void testCommit_XA_Long_StoreXid() throws Exception
   {
      doTransactionCommit(true, false, true);
   }
   
   public void testCommit_XA_Guid_NotStoreXid() throws Exception
   {
      doTransactionCommit(true, true, false);
   }
   
   public void testCommit_XA_Guid_StoreXid() throws Exception
   {
      doTransactionCommit(true, true, true);
   }
   
   public void testRollback_NotXA_Long_NotStoreXid() throws Exception
   {
      doTransactionRollback(false, false, false);
   }
   
   public void testRollback_NotXA_Long_StoreXid() throws Exception
   {
      doTransactionRollback(false, false, true);
   }
   
   public void testRollback_NotXA_Guid_NotStoreXid() throws Exception
   {
      doTransactionRollback(false, true, false);
   }
   
   public void testRollback_NotXA_Guid_StoreXid() throws Exception
   {
      doTransactionRollback(false, true, true);
   }
         
   public void testRollback_XA_Long_NotStoreXid() throws Exception
   {
      doTransactionRollback(true, false, false);
   }
   
   public void testRollback_XA_Long_StoreXid() throws Exception
   {
      doTransactionRollback(true, false, true);
   }
   
   public void testRollback_XA_Guid_NotStoreXid() throws Exception
   {
      doTransactionRollback(true, true, false);
   }
   
   public void testRollback_XA_Guid_StoreXid() throws Exception
   {
      doTransactionRollback(true, true, true);
   }
   
   
   public void testRetrievePreparedTransactions() throws Exception
   {
      if (ServerManagement.isRemote()) return;      
            
      JDBCPersistenceManager pm = new JDBCPersistenceManager();
      pm.start();
      MessageStore ms = new PersistentMessageStore("persistentMessageStore0", pm);
      Channel channel = new SimpleChannel("channel0", ms);
      
      TransactionRepository txRep = new TransactionRepository(pm);
            
      Message[] messages = createMessages();  
      
      Xid[] xids = new Xid[messages.length];
      
      for (int i = 0; i < messages.length; i++)
      {         
         xids[i] = new MockXid();
         Transaction tx = txRep.createTransaction(xids[i]);
         MessageReference ref = ms.reference(messages[i]);
         pm.addReference(channel.getChannelID(), ref, tx);  
         tx.prepare();
      }
      
      List txs = pm.retrievePreparedTransactions();
      assertNotNull(txs);
      assertEquals(messages.length, txs.size());
      
      for (int i = 0; i < xids.length; i++)
      {
         Xid xid = xids[i];
         assertTrue(txs.contains(xid));
      }
      
      pm.removeAllMessageData(channel.getChannelID());
      
   }
   
   protected Message createMessage(int i) throws Exception
   {
      Map headers = generateFilledMap(true);
      
      Message m =
         new CoreMessage(new GUID().toString(),
            true,
            System.currentTimeMillis() + 1000 * 60 * 60,
            System.currentTimeMillis(),
            i,
            headers,            
            i % 2 == 0 ? new WibblishObject() : null);
      
      return m;
   }
   
   protected Message[] createMessages() throws Exception
   {
      //Generate some messages with a good range of attribute values
      Message[] messages = new Message[10];
      for (int i = 0; i < 10; i++)
      {            
         messages[i] = createMessage(i);
      }
      return messages;
   }
   
   protected void checkEquivalent(Message m1, Message m2) throws Exception
   {
      if (m1 == m2)
      {
         fail();
      }
      
      if (m1 == null || m2 == null)
      {
         fail();
      }
      
      //Attributes from org.jboss.messaging.core.Message
      assertEquals(m1.getMessageID(), m2.getMessageID());
      assertEquals(m1.isReference(), m2.isReference());
      assertEquals(m1.isReliable(), m2.isReliable());
      assertEquals(m1.getExpiration(), m2.getExpiration());
      assertEquals(m1.isExpired(), m2.isExpired());
      assertEquals(m1.getTimestamp(), m2.getTimestamp());
      assertEquals(m1.getPriority(), m2.getPriority());
      assertEquals(m1.isRedelivered(), m2.isRedelivered());
      Map m1Headers = m1.getHeaders();
      Map m2Headers = m2.getHeaders();
      checkMapsEquivalent(m1Headers, m2Headers);
      checkMapsEquivalent(m2Headers, m1Headers);
      
      if (m1.getPayload() instanceof byte[] && m2.getPayload() instanceof byte[])
      {
         this.checkByteArraysEqual((byte[])m1.getPayload(), (byte[])m2.getPayload());
      }
      else if (m1.getPayload() instanceof Map && m2.getPayload() instanceof Map)
      {
         this.checkMapsEquivalent((Map)m1.getPayload(), (Map)m2.getPayload());
      }
      else if (m1.getPayload() instanceof List && m2.getPayload() instanceof List)
      {
         this.checkListsEquivalent((List)m1.getPayload(), (List)m2.getPayload());
      }
      else
      {      
         assertEquals(m1.getPayload(), m2.getPayload());
      }
      
   }
   
   protected void checkMapsEquivalent(Map headers1, Map headers2)
   {
      Iterator iter = headers1.entrySet().iterator();
      while (iter.hasNext())
      {
         Map.Entry entry1 = (Map.Entry)iter.next();
         Object value2 = headers2.get(entry1.getKey());
         assertNotNull(value2);
         if (value2 instanceof byte[])
         {
            checkByteArraysEqual((byte[])entry1.getValue(), (byte[])value2);
         }
         else
         {
            assertEquals(entry1.getValue(), value2);
         }
      }
   }
   
   protected void checkListsEquivalent(List l1, List l2)
   {      
      Iterator iter1 = l1.iterator();
      Iterator iter2 = l2.iterator();
      while (iter1.hasNext())
      {
         Object o1 = iter1.next();
         Object o2 = iter2.next();
         
         if (o1 instanceof byte[])
         {
            checkByteArraysEqual((byte[])o1, (byte[])o2);
         }
         else
         {
            assertEquals(o1, o2);
         }
      }
   }
   
   public static class WibblishObject implements Serializable
   {
      private static final long serialVersionUID = -822739710811857027L;
      public String wibble;
      public WibblishObject()
      {
         this.wibble = new GUID().toString();
      }
      public boolean equals(Object other)
      {
         if (!(other instanceof WibblishObject))
         {
            return false;
         }
         WibblishObject oo = (WibblishObject)other;
         return oo.wibble.equals(this.wibble);
      }
   }
   
   protected Map generateFilledMap(boolean useObject)
   {
      Map headers = new HashMap();
      for (int j = 0; j < 27; j++)
      {
         //put some crap in the map
         int k;
         if (useObject)
         {
            k = j % 11;
         }
         else
         {
            k = j % 10;
         }
         
         switch (k)
         {
            case 0:
               headers.put(new GUID().toString(), randString(1000));
            case 1:
               headers.put(new GUID().toString(), randByte());
            case 2:
               headers.put(new GUID().toString(), randShort());
            case 3:
               headers.put(new GUID().toString(), randInt());
            case 4:
               headers.put(new GUID().toString(), randLong());
            case 5:
               headers.put(new GUID().toString(), randBool());
            case 6:
               headers.put(new GUID().toString(), randFloat());
            case 7:
               headers.put(new GUID().toString(), randDouble());
            case 8:
               headers.put(new GUID().toString(), randLong());
            case 9:
               headers.put(new GUID().toString(), randByteArray());
            case 10:
               headers.put(new GUID().toString(), new WibblishObject());               
         }
      }
      return headers;
   }
   
   protected Byte randByte()
   {
      return new Byte((byte)(Math.random() * (2^8 - 1) - (2^7)));
   }
   
   protected Short randShort()
   {
      return new Short((short)(Math.random() * (2^16 - 1) - (2^15)));
   }
   
   protected Integer randInt()
   {
      return new Integer((int)(Math.random() * (2^32 - 1) - (2^31)));
   }
   
   protected Long randLong()
   {
      return new Long((long)(Math.random() * (2^64 - 1) - (2^64)));
   }
   
   protected Boolean randBool()
   {
      return new Boolean(Math.random() > 0.5 ? true : false);
   }
   
   protected Float randFloat()
   {
      return new Float((float)(Math.random() * 1000000));
   }
   
   protected Double randDouble()
   {
      return new Double(Math.random() * 1000000);
   }
   
   protected String randString(int length)
   {
      StringBuffer buf = new StringBuffer(length);
      for (int i = 0; i < length; i++)
      {
         buf.append(randChar().charValue());
      }
      return buf.toString();
   }
   
   protected byte[] randByteArray()
   {
      String s = randString(1000);
      return s.getBytes();
   }
   
   protected Character randChar()
   {
      return new Character((char)randShort().shortValue());
   }
   
   protected void checkByteArraysEqual(byte[] b1, byte[] b2)
   {
      if (b1 == null || b2 == null)
      {
         fail();
      }
      if (b1.length != b2.length)
      {
         fail();
      }
      
      for (int i = 0; i < b1.length; i++)
      {
         assertEquals(b1[i], b2[i]);
      }
      
   }
   
   protected class MockXid implements Xid
   {
      byte[] branchQual;
      int formatID;
      byte[] globalTxId;
      
      protected MockXid()
      {
         branchQual = new GUID().toString().getBytes();
         formatID = randInt().intValue();
         globalTxId = new GUID().toString().getBytes();
      }

      public byte[] getBranchQualifier()
      {
         return branchQual;
      }

      public int getFormatId()
      {
         return formatID;
      }

      public byte[] getGlobalTransactionId()
      {
         return globalTxId;
      }
      
      public boolean equals(Object other)
      {
         if (!(other instanceof Xid))
         {
            return false;
         }
         Xid xother = (Xid)other;
         if (xother.getFormatId() != this.formatID)
         {
            return false;
         }
         if (xother.getBranchQualifier().length != this.branchQual.length)
         {
            return false;
         }
         if (xother.getGlobalTransactionId().length != this.globalTxId.length)
         {
            return false;
         }
         for (int i = 0; i < this.branchQual.length; i++)
         {
            byte[] otherBQ = xother.getBranchQualifier();
            if (this.branchQual[i] != otherBQ[i])
            {
               return false;
            }
         }
         for (int i = 0; i < this.globalTxId.length; i++)
         {
            byte[] otherGtx = xother.getGlobalTransactionId();
            if (this.globalTxId[i] != otherGtx[i])
            {
               return false;
            }
         }
         return true;
      }
      
   }
   
   protected void doTransactionCommit(boolean xa, boolean idIsGuid, boolean storeXid) throws Exception
   {
      if (ServerManagement.isRemote()) return;
      
      JDBCPersistenceManager pm = new JDBCPersistenceManager();
      if (idIsGuid)
      {
         this.configTablesForGUID(pm);
      }
      pm.setTxIdGuid(idIsGuid);
      pm.setStoringXid(storeXid);
      MessageStore ms = new PersistentMessageStore("persistentMessageStore0", pm);
      Channel channel = new SimpleChannel("channel0", ms);      
      TransactionRepository txRep = new TransactionRepository(pm);
      pm.start();
      
      Message[] messages = createMessages();     
      
      Message m1 = messages[0];
      Message m2 = messages[1];
      Message m3 = messages[2];      
      Message m4 = messages[3];
      Message m5 = messages[4];

      Transaction tx = null;
      if (xa)
      {         
         tx = txRep.createTransaction(new MockXid());
      }
      else
      {
         tx = txRep.createTransaction();
      }
      
      MessageReference ref1 = ms.reference(m1);
      MessageReference ref2 = ms.reference(m2);  
      MessageReference ref3 = ms.reference(m3);       
      MessageReference ref4 = ms.reference(m4);
      MessageReference ref5 = ms.reference(m5);  

      //Add first two refs non transactionally
      pm.addReference(channel.getChannelID(), ref1, null);
      pm.addReference(channel.getChannelID(), ref2, null);
      
      //check they're there
      List refs = pm.messageRefs(ms.getStoreID(), channel.getChannelID());
      assertNotNull(refs);
      assertEquals(2, refs.size());
      assertTrue(refs.contains(ref1.getMessageID()));
      assertTrue(refs.contains(ref2.getMessageID()));      
      
      //Add the next 3 refs transactionally
      pm.addReference(channel.getChannelID(), ref3, tx);
      pm.addReference(channel.getChannelID(), ref4, tx);
      pm.addReference(channel.getChannelID(), ref5, tx);
      
      //Remove the other 2 transactionally
      pm.removeReference(channel.getChannelID(), ref1, tx);
      pm.removeReference(channel.getChannelID(), ref2, tx);
      
      //Check the changes aren't visible
      refs = pm.messageRefs(ms.getStoreID(), channel.getChannelID());
      assertNotNull(refs);
      assertEquals(2, refs.size());
      assertTrue(refs.contains(ref1.getMessageID()));
      assertTrue(refs.contains(ref2.getMessageID()));  
      
      //commit transaction
      pm.commitTx(tx);
      
      //check we can see only the last 3 refs
      refs = pm.messageRefs(ms.getStoreID(), channel.getChannelID());
      assertNotNull(refs);
      assertEquals(3, refs.size()); 
      assertTrue(refs.contains(ref3.getMessageID()));
      assertTrue(refs.contains(ref4.getMessageID()));  
      assertTrue(refs.contains(ref5.getMessageID()));
      
      pm.removeAllMessageData(channel.getChannelID());
      
   }
   

   
   
   protected void doTransactionRollback(boolean xa, boolean idIsGuid, boolean storeXid) throws Exception
   {
      if (ServerManagement.isRemote()) return;
      
      JDBCPersistenceManager pm = new JDBCPersistenceManager();
      if (idIsGuid)
      {
         this.configTablesForGUID(pm);
      }
      pm.setTxIdGuid(idIsGuid);
      pm.setStoringXid(storeXid);
      MessageStore ms = new PersistentMessageStore("persistentMessageStore0", pm);
      Channel channel = new SimpleChannel("channel0", ms);      
      TransactionRepository txRep = new TransactionRepository(pm);
      pm.start();
      
      Message[] messages = createMessages();     
      
      Message m1 = messages[0];
      Message m2 = messages[1];
      Message m3 = messages[2];      
      Message m4 = messages[3];
      Message m5 = messages[4];

      
      Transaction tx = null;
      if (xa)
      {
         tx = txRep.createTransaction(new MockXid());
      }
      else
      {
         tx = txRep.createTransaction();
      }
      
      MessageReference ref1 = ms.reference(m1);
      MessageReference ref2 = ms.reference(m2);  
      MessageReference ref3 = ms.reference(m3);       
      MessageReference ref4 = ms.reference(m4);
      MessageReference ref5 = ms.reference(m5);  

      //Add first two refs non transactionally
      pm.addReference(channel.getChannelID(), ref1, null);
      pm.addReference(channel.getChannelID(), ref2, null);
      
      //check they're there
      List refs = pm.messageRefs(ms.getStoreID(), channel.getChannelID());
      assertNotNull(refs);
      assertEquals(2, refs.size());
      assertTrue(refs.contains(ref1.getMessageID()));
      assertTrue(refs.contains(ref2.getMessageID()));      
      
      //Add the next 3 refs transactionally
      pm.addReference(channel.getChannelID(), ref3, tx);
      pm.addReference(channel.getChannelID(), ref4, tx);
      pm.addReference(channel.getChannelID(), ref5, tx);
      
      //Remove the other 2 transactionally
      pm.removeReference(channel.getChannelID(), ref1, tx);
      pm.removeReference(channel.getChannelID(), ref2, tx);
      
      //Check the changes aren't visible
      refs = pm.messageRefs(ms.getStoreID(), channel.getChannelID());
      assertNotNull(refs);
      assertEquals(2, refs.size());
      assertTrue(refs.contains(ref1.getMessageID()));
      assertTrue(refs.contains(ref2.getMessageID()));  
      
      //rollback transaction
      pm.rollbackTx(tx);
      
      refs = pm.messageRefs(ms.getStoreID(), channel.getChannelID());
      assertNotNull(refs);
      assertEquals(2, refs.size());
      assertTrue(refs.contains(ref1.getMessageID()));
      assertTrue(refs.contains(ref2.getMessageID()));  
      
      pm.removeAllMessageData(channel.getChannelID());
      
      
   }
   

   protected void configTablesForGUID(JDBCPersistenceManager pm)
   {
      Properties props = new Properties();
      
      props.put("CREATE_TRANSACTION",
            "CREATE TABLE TRANSACTION ( TRANSACTIONID VARCHAR(255), BRANCH_QUAL OBJECT, FORMAT_ID INTEGER, " +
            "GLOBAL_TXID OBJECT, PRIMARY KEY (TRANSACTIONID) )");
      
      props.put("CREATE_DELIVERY",
         "CREATE TABLE DELIVERY (CHANNELID VARCHAR(256), MESSAGEID VARCHAR(256), " +
         "STOREID VARCHAR(256), TRANSACTIONID VARCHAR(255), STATE CHAR(1), PRIMARY KEY(CHANNELID, MESSAGEID))");
      
      props.put("CREATE_MESSAGE_REFERENCE",
         "CREATE TABLE MESSAGE_REFERENCE (CHANNELID VARCHAR(256), MESSAGEID VARCHAR(256), " +
         "STOREID VARCHAR(256), TRANSACTIONID VARCHAR(255), STATE CHAR(1), PRIMARY KEY(CHANNELID, MESSAGEID))");
      
      pm.setSqlProperties(props);
   }
   
   
}



