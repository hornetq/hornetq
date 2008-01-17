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
package org.jboss.messaging.core.impl.bdbje.integration.test.unit;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.Queue;
import org.jboss.messaging.core.impl.MessageImpl;
import org.jboss.messaging.core.impl.QueueImpl;
import org.jboss.messaging.core.impl.bdbje.BDBJEPersistenceManager;
import org.jboss.messaging.core.tx.MessagingXid;
import org.jboss.messaging.test.unit.UnitTestCase;

/**
 * 
 * A BDBSpeedTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class BDBSpeedTest extends UnitTestCase
{
   
   public void test1()
   {      
   }
   
//   public void testCommitMessage() throws Exception
//   {
//      String envPath = "/home/tim/test-env";
//      
//      File file = new File(envPath);
//      
//      deleteDirectory(file);
//      
//      file.mkdir();
//      
//      final int numMessages = 5000;
//      
//      final int numRefs = 10;
//      
//      BDBJEPersistenceManager2 bdb = new BDBJEPersistenceManager2(envPath);
//      
//      bdb.start();
//      
//      Queue queue = new QueueImpl(1);
//      
//      //List<Message> messages = new ArrayList<Message>();
//      
//      long start = System.currentTimeMillis();
//      
//      byte[] payload = new byte[1 * 1024];
//      
//      for (int i = 0; i < numMessages; i++)
//      {
//         Message message = new MessageImpl(i, 0, true, 0, System.currentTimeMillis(), (byte)4);
//         
//         for (int j = 0; j < numRefs; j++)
//         {
//            message.createReference(queue);
//            
//            message.setPayload(payload);
//         }
//         
//         //messages.add(message);
//         
//         bdb.commitMessage(message);
//      }
//      
//      long end = System.currentTimeMillis();
//      
//      double rate = 1000 * (double)numMessages / (end - start);
//      
//      System.out.println("Insert Rate: " + rate);
//      
//   }
//   
//   public void testCommitMessages() throws Exception
//   {
//      String envPath = "/home/tim/test-env";
//      
//      File file = new File(envPath);
//      
//      deleteDirectory(file);
//      
//      file.mkdir();
//      
//      final int numMessages = 5000;
//      
//      final int numRefs = 10;
//      
//      BDBJEPersistenceManager2 bdb = new BDBJEPersistenceManager2(envPath);
//      
//      bdb.start();
//      
//      Queue queue = new QueueImpl(1);
//      
//      List<Message> messages = new ArrayList<Message>();
//      
//      long start = System.currentTimeMillis();
//      
//      byte[] payload = new byte[1 * 1024];
//                  
//      for (int i = 0; i < numMessages; i++)
//      {
//         Message message = new MessageImpl(i, 0, true, 0, System.currentTimeMillis(), (byte)4);
//         
//         for (int j = 0; j < numRefs; j++)
//         {
//            message.createReference(queue);
//            
//            message.setPayload(payload);
//         }
//         
//         messages.add(message);
//         
//         if (i % 100 == 0)
//         {
//            bdb.commitMessages(messages);
//            
//            messages.clear();
//         }         
//      }
//      bdb.commitMessages(messages);
//      
//      long end = System.currentTimeMillis();
//      
//      double rate = 1000 * (double)numMessages / (end - start);
//      
//      System.out.println("Insert Rate: " + rate);
//      
//   }
//   
//   public void testDeleteReferences() throws Exception
//   {
//      String envPath = "/home/tim/test-env";
//      
//      File file = new File(envPath);
//      
//      deleteDirectory(file);
//      
//      file.mkdir();
//      
//      final int numMessages = 5000;
//      
//      final int numRefs = 10;
//      
//      BDBJEPersistenceManager2 bdb = new BDBJEPersistenceManager2(envPath);
//      
//      bdb.start();
//      
//      Queue queue = new QueueImpl(1);
//      
//      List<Message> messages = new ArrayList<Message>();
//      
//      byte[] payload = new byte[1 * 1024];
//      
//      List<MessageReference> refs = new ArrayList<MessageReference>();
//                  
//      for (int i = 0; i < numMessages; i++)
//      {
//         Message message = new MessageImpl(i, 0, true, 0, System.currentTimeMillis(), (byte)4);
//         
//         for (int j = 0; j < numRefs; j++)
//         {
//            MessageReference ref = message.createReference(queue);
//            
//            message.setPayload(payload);
//            
//            refs.add(ref);
//         }
//         
//         messages.add(message);
//         
//         if (i % 100 == 0)
//         {
//            bdb.commitMessages(messages);
//            
//            messages.clear();
//         }         
//      }
//      bdb.commitMessages(messages);
//      
//      long start = System.currentTimeMillis();
//      
//      //Now delete them
//      
//      bdb.deleteReferences(refs);      
//      
//      long end = System.currentTimeMillis();
//      
//      double rate = 1000 * (double)numMessages / (end - start);
//      
//      System.out.println("Delete Rate: " + rate);
//      
//   }
//   
//   public void testDeleteReference() throws Exception
//   {
//      String envPath = "/home/tim/test-env";
//      
//      File file = new File(envPath);
//      
//      deleteDirectory(file);
//      
//      file.mkdir();
//      
//      final int numMessages = 5000;
//      
//      final int numRefs = 1;
//      
//      BDBJEPersistenceManager bdb = new BDBJEPersistenceManager(envPath);
//      
//      bdb.start();
//      
//      Queue queue = new QueueImpl(1);
//      
//      List<Message> messages = new ArrayList<Message>();
//      
//      byte[] payload = new byte[1 * 1024];
//      
//      List<MessageReference> refs = new ArrayList<MessageReference>();
//                  
//      for (int i = 0; i < numMessages; i++)
//      {
//         Message message = new MessageImpl(i, 0, true, 0, System.currentTimeMillis(), (byte)4);
//         
//         for (int j = 0; j < numRefs; j++)
//         {
//            MessageReference ref = message.createReference(queue);
//            
//            message.setPayload(payload);
//            
//            refs.add(ref);
//         }
//         
//         messages.add(message);
//         
//         if (i % 100 == 0)
//         {
//            bdb.commitMessages(messages);
//            
//            messages.clear();
//         }         
//      }
//      bdb.commitMessages(messages);
//      
//      System.out.println("Added them");
//      
//      long start = System.currentTimeMillis();
//      
//      //Now delete them
//      
//      for (MessageReference ref: refs)
//      {      
//         bdb.deleteReference(ref);
//      }
//      
//      long end = System.currentTimeMillis();
//      
//      double rate = 1000 * (double)numMessages / (end - start);
//      
//      System.out.println("Delete Rate: " + rate);
//      
//   }
//   
//   
//   public void testPrepareMessages() throws Exception
//   {
//      String envPath = "/home/tim/test-env";
//      
//      File file = new File(envPath);
//      
//      deleteDirectory(file);
//      
//      file.mkdir();
//      
//      final int numMessages = 5000;
//      
//      final int numRefs = 10;
//      
//      BDBJEPersistenceManager2 bdb = new BDBJEPersistenceManager2(envPath);
//      
//      bdb.start();
//      
//      Queue queue = new QueueImpl(1);
//      
//      List<Message> messages = new ArrayList<Message>();
//      
//      long start = System.currentTimeMillis();
//      
//      byte[] payload = new byte[1 * 1024];
//                  
//      for (int i = 0; i < numMessages; i++)
//      {
//         Message message = new MessageImpl(i, 0, true, 0, System.currentTimeMillis(), (byte)4);
//         
//         for (int j = 0; j < numRefs; j++)
//         {
//            message.createReference(queue);
//            
//            message.setPayload(payload);
//         }
//         
//         messages.add(message);
//         
//         if (i % 100 == 0)
//         {
//            Xid xid = this.generateXid();
//            
//            bdb.prepareMessages(xid, messages);
//            
//            bdb.commitPreparedMessages(xid);
//            
//            messages.clear();
//         }         
//      }
//      
//      Xid xid = this.generateXid();
//      
//      bdb.prepareMessages(xid, messages);
//      
//      bdb.commitPreparedMessages(xid);
//      
//      long end = System.currentTimeMillis();
//      
//      double rate = 1000 * (double)numMessages / (end - start);
//      
//      System.out.println("Insert Rate: " + rate);
//      
//   }
   
//   /*
//    * Test with message and ref in separate databases
//    */
//   public void testMsgandRefsDifferentDBSNoXA() throws Exception
//   {
//      File file = new File("/home/tim/test-env");
//      
//      deleteDirectory(file);
//      
//      file.mkdir();
//      
//      EnvironmentConfig envConfig = new EnvironmentConfig();
//        
//      envConfig.setAllowCreate(true);
//      
//      envConfig.setTransactional(true);
//      
////      envConfig.setTxnNoSync(true);
////      
////      envConfig.setTxnWriteNoSync(true);
//      
//      Environment env1 = new Environment(file, envConfig);
//
//      DatabaseConfig dbConfig1 = new DatabaseConfig();
//      
//      dbConfig1.setTransactional(true);
//      
//      dbConfig1.setAllowCreate(true);
//      
//      Database msgDB = env1.openDatabase(null, "message", dbConfig1);
//      
//      DatabaseConfig dbConfig2 = new DatabaseConfig();
//      
//      dbConfig2.setTransactional(true);
//      
//      dbConfig2.setAllowCreate(true);
//      
//      Database refDB = env1.openDatabase(null, "ref", dbConfig2);
//      
//      
//      DatabaseConfig dbConfig3 = new DatabaseConfig();
//      
//      dbConfig3.setTransactional(true);
//      
//      dbConfig3.setAllowCreate(true);
//      
//      Database txDB = env1.openDatabase(null, "tx", dbConfig3);
//      
//      
//      
//      final int numMessages = 5000;
//      
//      byte[] msgBytes = new byte[256];
//      
//      final int numRefs = 10;
//      
//      byte[] refBytes = new byte[numRefs * 20];
//      
//      byte[] txBytes = new byte[numRefs * 50];
//      
//      
//      long start = System.currentTimeMillis();
//      
//      for (int i = 0; i < numMessages; i++)
//      {
//         byte[] keyBytes = new byte[8];
//         
//         ByteBuffer buffer = ByteBuffer.wrap(keyBytes);
//         
//         buffer.putLong(i);
//         
//         DatabaseEntry key = new DatabaseEntry(keyBytes);
//         
//         
//         //Prepare refs
//         
//         DatabaseEntry refVal = new DatabaseEntry(refBytes);
//         
//         Transaction tx = env1.beginTransaction(null, null);
//                  
//         refDB.put(tx, key, refVal);
//         
//         // Message
//         
//         DatabaseEntry msgVal = new DatabaseEntry(msgBytes);
//         
//         msgDB.put(tx, key, msgVal);
//         
//         //Write a tx record
//         
//         DatabaseEntry txVal = new DatabaseEntry(txBytes);
//         
//         txDB.put(tx, key, txVal);
//               
//         //Commit the prepare
//         
//         tx.commit();    
//         
//         tx = env1.beginTransaction(null, null);
//         
//         //Now commit the refs
//                  
//         refVal = new DatabaseEntry(refBytes);
//         
//         refDB.put(tx, key, refVal);
//         
//         //And delete the tx record
//         
//         txDB.delete(tx, key);
//                  
//         tx.commit();         
//      }
//      
//      long end = System.currentTimeMillis();
//      
//      double rate = 1000 * (double)numMessages / (end - start);
//      
//      System.out.println("Insert Rate: " + rate);
//      
//    
//   }
//   
//   public void testMsgandRefsDifferentDBSXA() throws Exception
//   {
//      File file = new File("/home/tim/test-env");
//      
//      deleteDirectory(file);
//      
//      file.mkdir();
//      
//      EnvironmentConfig envConfig = new EnvironmentConfig();
//        
//      envConfig.setAllowCreate(true);
//      
//      envConfig.setTransactional(true);
//      
//      XAEnvironment env1 = new XAEnvironment(file, envConfig);
//
//      DatabaseConfig dbConfig1 = new DatabaseConfig();
//      
//      dbConfig1.setTransactional(true);
//      
//      dbConfig1.setAllowCreate(true);
//      
//      Database msgDB = env1.openDatabase(null, "message", dbConfig1);
//      
//      DatabaseConfig dbConfig2 = new DatabaseConfig();
//      
//      dbConfig2.setTransactional(true);
//      
//      dbConfig2.setAllowCreate(true);
//      
//      Database refDB = env1.openDatabase(null, "ref", dbConfig2);
//      
//      
//      
//      final int numMessages = 5000;
//      
//      byte[] msgBytes = new byte[256];
//      
//      final int numRefs = 10;
//      
//      byte[] refBytes = new byte[numRefs * 20];
//      
//      long start = System.currentTimeMillis();
//      
//      for (int i = 0; i < numMessages; i++)
//      {
//         Xid xid = generateXid();
//         
//         env1.start(xid, XAResource.TMNOFLAGS);
//         
//         byte[] keyBytes = new byte[8];
//         
//         ByteBuffer buffer = ByteBuffer.wrap(keyBytes);
//         
//         buffer.putLong(i);
//         
//         DatabaseEntry key = new DatabaseEntry(keyBytes);
//         
//         DatabaseEntry msgVal = new DatabaseEntry(msgBytes);
//         
//         Transaction tx = env1.beginTransaction(null, null);
//         
//         msgDB.put(null, key, msgVal);
//         
//         //Now the refs
//                  
//         DatabaseEntry refVal = new DatabaseEntry(refBytes);
//         
//         refDB.put(null, key, refVal);
//         
//         env1.end(xid, XAResource.TMSUCCESS);
//         
//         env1.prepare(xid);
//         
//         env1.commit(xid, false);
//                  
//        // tx.commit();         
//      }
//      
//      long end = System.currentTimeMillis();
//      
//      double rate = 1000 * (double)numMessages / (end - start);
//      
//      System.out.println("Insert Rate: " + rate);
//      
//      start = System.currentTimeMillis();
//   }
//   
//   private Xid generateXid()
//   {      
//      String id = java.util.UUID.randomUUID().toString();
//      
//      Xid xid = new MessagingXid("blah".getBytes(), 123, id.getBytes());
//      
//      return xid;
//   }
//   
//   
//// This is very slow   
////   public void testMsgandRefsSameDBSNoXA() throws Exception
////   {
////      File file = new File("/home/tim/test-env");
////      
////      deleteDirectory(file);
////      
////      file.mkdir();
////      
////      EnvironmentConfig envConfig = new EnvironmentConfig();
////        
////      envConfig.setAllowCreate(true);
////      
////      envConfig.setTransactional(true);
////      
////      Environment env1 = new Environment(file, envConfig);
////
////      DatabaseConfig dbConfig1 = new DatabaseConfig();
////      
////      dbConfig1.setTransactional(true);
////      
////      dbConfig1.setAllowCreate(true);
////      
////      Database msgDB = env1.openDatabase(null, "message", dbConfig1);      
////     
////      final int numMessages = 5000;
////      
////      byte[] msgBytes = new byte[10 * 1024];
////      
////      byte[] refBytes = new byte[24];
////      
////      long start = System.currentTimeMillis();
////      
////      for (int i = 0; i < numMessages; i++)
////      {
////         byte[] keyBytes = new byte[8];
////         
////         ByteBuffer buffer = ByteBuffer.wrap(keyBytes);
////         
////         buffer.putLong(i);
////         
////         DatabaseEntry key = new DatabaseEntry(keyBytes);
////         
////         DatabaseEntry msgVal = new DatabaseEntry(msgBytes);
////         
////         Transaction tx = env1.beginTransaction(null, null);
////         
////         msgDB.put(tx, key, msgVal);
////         
////         //Now the refs
////         
////         final int numRefs = 50;
////         
////         for (int j = 0; j < numRefs; j++)
////         {
////            keyBytes = new byte[8];
////            
////            buffer = ByteBuffer.wrap(keyBytes);
////            
////            buffer.putLong(j);
////            
////            key = new DatabaseEntry(keyBytes);
////                                    
////            DatabaseEntry value = new DatabaseEntry();
////            
////            value.setPartial(0, refBytes.length, true);
////                        
////            value.setData(refBytes);
////            
////            msgDB.put(tx, key, value);
////         }
////         
////         tx.commit();
////         
////      }
////      
////      long end = System.currentTimeMillis();
////      
////      double rate = 1000 * (double)numMessages / (end - start);
////      
////      System.out.println("Rate: " + rate);
////   }
}
