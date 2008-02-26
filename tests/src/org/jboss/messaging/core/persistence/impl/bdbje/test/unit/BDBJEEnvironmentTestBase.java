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
package org.jboss.messaging.core.persistence.impl.bdbje.test.unit;

import java.io.File;
import java.util.List;

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.persistence.impl.bdbje.BDBJEDatabase;
import org.jboss.messaging.core.persistence.impl.bdbje.BDBJEEnvironment;
import org.jboss.messaging.core.persistence.impl.bdbje.BDBJETransaction;
import org.jboss.messaging.core.server.impl.XidImpl;
import org.jboss.messaging.test.unit.UnitTestCase;

import com.sleepycat.je.DatabaseException;

/**
 * 
 * Base for tests for BDBJEEnvironment and BDBJEDatabase
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public abstract class BDBJEEnvironmentTestBase extends UnitTestCase
{
   protected BDBJEEnvironment env;
   
   protected BDBJEDatabase database;
   
   protected static final String HOME_DIR = System.getProperty("user.home");
   
   protected static final String ENV_DIR = HOME_DIR + "/test-bdb-environment";
      
   protected static final String ENV_COPY_DIR = HOME_DIR + "/test-bdb-environment-copy";
   
   protected static final String DB_NAME = "test-db";
   
   @Override
   protected void setUp() throws Exception
   {   
      super.setUp();
      
      env = createEnvironment();
      
      env.setCreateEnvironment(true);
      
      env.setEnvironmentPath(ENV_DIR);
      
      env.start();
      
      database = env.getDatabase(DB_NAME);      
   }
   
   protected abstract void createDir(String path);
   
   protected void tearDown() throws Exception
   {
      super.tearDown();
      
      if (env.isStarted())
      {
         env.stop();
      }
   }
   
   protected abstract BDBJEEnvironment createEnvironment() throws Exception;
   
   protected abstract void copyEnvironment() throws Exception;

   protected abstract void copyBackEnvironment() throws Exception;
   
   protected abstract boolean isFake();
   

   // The tests ----------------------------------------------------------------
      
   public void testGetInDoubtXidsCompleteWithCommit() throws Exception
   {
      List<Xid> xids = env.getInDoubtXids();
      
      assertTrue(xids.isEmpty());
      
      Xid xid1 = generateXid();
      
      env.startWork(xid1);
      
      database.put(null, 1, new byte[10], 0, 10);
      
      env.endWork(xid1, false);
      
      env.prepare(xid1);
      
      xids = env.getInDoubtXids();
      
      assertEquals(xid1, xids.get(0));
      
      env.commit(xid1);
      
      xids = env.getInDoubtXids();
      
      assertTrue(xids.isEmpty());
   }
      
   public void testGetInDoubtXidsCompleteWithRollback() throws Exception
   {
      List<Xid> xids = env.getInDoubtXids();
      
      assertTrue(xids.isEmpty());
      
      Xid xid1 = generateXid();
      
      env.startWork(xid1);
      
      database.put(null, 1, new byte[10], 0, 10);
      
      env.endWork(xid1, false);
      
      env.prepare(xid1);
      
      xids = env.getInDoubtXids();
      
      assertEquals(xid1, xids.get(0));
      
      env.rollback(xid1);
      
      xids = env.getInDoubtXids();
      
      assertTrue(xids.isEmpty());
   }
   
   
   public void testGetInDoubtXidsMultiple() throws Exception
   {
      List<Xid> xids = env.getInDoubtXids();
      
      assertTrue(xids.isEmpty());
      
      Xid xid1 = generateXid();      
      env.startWork(xid1);      
      database.put(null, 1, new byte[10], 0, 10);      
      env.endWork(xid1, false); 
      
      env.prepare(xid1);      
      xids = env.getInDoubtXids();      
      assertEquals(xid1, xids.get(0));
      
      
      Xid xid2 = generateXid();      
      env.startWork(xid2);      
      database.put(null, 2, new byte[10], 0, 10);      
      env.endWork(xid2, false); 
      
      env.prepare(xid2);      
      xids = env.getInDoubtXids();      
      assertTrue(xids.contains(xid1));
      assertTrue(xids.contains(xid2));
      
      Xid xid3 = generateXid();      
      env.startWork(xid3);      
      database.put(null, 3, new byte[10], 0, 10);      
      env.endWork(xid3, false); 
      
      env.prepare(xid3);      
      xids = env.getInDoubtXids();      
      assertTrue(xids.contains(xid1));
      assertTrue(xids.contains(xid2));
      assertTrue(xids.contains(xid3));
      
      env.commit(xid1);
      
      env.commit(xid2);
      
      env.commit(xid3);
     
   }
   
   // Commented out until http://jira.jboss.org/jira/browse/JBMESSAGING-1192 is complete   
//   public void testGetInDoubtXidsMultipleWithRestart() throws Exception
//   {
//      List<Xid> xids = env.getInDoubtXids();
//      
//      assertTrue(xids.isEmpty());
//      
//      Xid xid1 = generateXid();      
//      env.startWork(xid1);      
//      database.put(null, 1, new byte[10], 0, 10);      
//      env.endWork(xid1, false); 
//      
//      env.prepare(xid1);      
//      xids = env.getInDoubtXids();      
//      assertEquals(xid1, xids.get(0));
//      
//      
//      Xid xid2 = generateXid();      
//      env.startWork(xid2);      
//      database.put(null, 2, new byte[10], 0, 10);      
//      env.endWork(xid2, false); 
//      
//      env.prepare(xid2);      
//      xids = env.getInDoubtXids();      
//      assertTrue(xids.contains(xid1));
//      assertTrue(xids.contains(xid2));
//      
//      Xid xid3 = generateXid();      
//      env.startWork(xid3);      
//      database.put(null, 3, new byte[10], 0, 10);      
//      env.endWork(xid3, false); 
//      
//      env.prepare(xid3);      
//      xids = env.getInDoubtXids();      
//      assertTrue(xids.contains(xid1));
//      assertTrue(xids.contains(xid2));
//      assertTrue(xids.contains(xid3));
//      
//      //Now to simulate a crash we copy the environment directory from under BDB, then close
//      //BDB. Then start it again with the copied config.
//      //We can't just stop the environment and restart it since on stopping cleanly BDB will abort any open
//      //transactions
//      
//      copyEnvironment();
//      
//      database.close();
//      
//      env.stop();
//      
//      copyBackEnvironment();
//      
//      env.start();
//                 
//      database = env.getDatabase(DB_NAME);
//      
//      xids = env.getInDoubtXids();      
//      assertTrue(xids.contains(xid1));
//      assertTrue(xids.contains(xid2));
//      assertTrue(xids.contains(xid3));
//      
//      env.commit(xid1);
//      env.commit(xid2);
//      env.commit(xid3);
//      
//      xids = env.getInDoubtXids();
//      
//      assertTrue(xids.isEmpty());     
//   }
      
      
   public void testPutAndRemoveNonTransactional() throws Exception
   {
      byte[] bytes = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
       
      long id = 1;
      
      int offset = 0;
      
      database.put(null, id, bytes, offset, bytes.length);
      
      assertContainsPair(id, bytes, 1);
      
      database.remove(null, 1);
      
      assertStoreEmpty();
   }
   
   public void testPutAndRemoveNonTransactionalWithRestart() throws Exception
   {
      byte[] bytes = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
      
      long id = 1;
      
      int offset = 0;
      
      database.put(null, id, bytes, offset, bytes.length);
      
      assertContainsPair(id, bytes, 1);
      
      database.close();
      
      env.stop();
      
      env.start();
      
      database = env.getDatabase(DB_NAME);
      
      assertContainsPair(id, bytes, 1);
   }
   
   public void testPutAndRemoveMultipleNonTransactional() throws Exception
   {
      byte[] bytes1 = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
      
      byte[] bytes2 = new byte[] { 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 };
      
      byte[] bytes3 = new byte[] { 21, 22, 23, 24, 25, 26, 27, 28, 29, 30 };
       
      long id1 = 1;
      
      long id2 = 2;
      
      long id3 = 3;
      
      int offset = 0;
      
      database.put(null, id1, bytes1, offset, bytes1.length);
      
      database.put(null, id2, bytes2, offset, bytes2.length);
      
      database.put(null, id3, bytes3, offset, bytes3.length);
      
      assertContainsPair(id1, bytes1, 3);
      
      assertContainsPair(id2, bytes2, 3);
      
      assertContainsPair(id3, bytes3, 3);
                       
      database.remove(null, id2);
      
      assertContainsPair(id1, bytes1, 2);
      
      assertContainsPair(id3, bytes3, 2);
            
      database.remove(null, id3);
      
      assertContainsPair(id1, bytes1, 1);
      
      database.remove(null, id1);
      
      assertStoreEmpty();      
   }
            
   public void testPutTransactionalCommit() throws Exception
   {
      byte[] bytes = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
       
      long id = 1;
      
      int offset = 0;
      
      BDBJETransaction tx = env.createTransaction();
      
      database.put(tx, id, bytes, offset, bytes.length);
      
      tx.commit();
      
      assertContainsPair(id, bytes, 1);
      
      database.remove(null, 1);
      
      assertStoreEmpty();            
   }
   
   public void testPutTransactionalWithRestart() throws Exception
   {
      byte[] bytes = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
       
      long id = 1;
      
      int offset = 0;
      
      BDBJETransaction tx = env.createTransaction();
      
      database.put(tx, id, bytes, offset, bytes.length);
      
      //Now restart before committing
      
      database.close();
      
      //This will abort the transaction
      env.stop();

      env.start();
      
      database = env.getDatabase(DB_NAME);
      
      assertStoreEmpty();            
   }
   
   public void testPutXACommit() throws Exception
   {
      byte[] bytes = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
       
      long id = 1;
      
      int offset = 0;
      
      Xid xid = generateXid();
      
      env.startWork(xid);
      
      database.put(null, id, bytes, offset, bytes.length);
      
      env.endWork(xid, false);
      
      env.prepare(xid);
      
      env.commit(xid);
      
      assertContainsPair(id, bytes, 1);
      
      database.remove(null, 1);
      
      assertStoreEmpty();            
   }
   
   public void testPutXAWithRestart() throws Exception
   {
      byte[] bytes = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
       
      long id = 1;
      
      int offset = 0;
      
      Xid xid = generateXid();
      
      env.startWork(xid);
      
      database.put(null, id, bytes, offset, bytes.length);
      
      env.endWork(xid, false);
      
      // Now restart
      
      database.close();
      
      env.stop();
      
      env.start();
      
      database = env.getDatabase(DB_NAME);
      
      assertStoreEmpty();            
   }
   
   
   public void testPutXAWithRestartAfterPrepare() throws Exception
   {
      byte[] bytes = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
       
      long id = 1;
      
      int offset = 0;
      
      Xid xid = generateXid();
      
      env.startWork(xid);
      
      database.put(null, id, bytes, offset, bytes.length);
      
      env.endWork(xid, false);
      
      env.prepare(xid);
      
      // Now restart
      
      database.close();
      
      env.stop();
      
      env.start();
      
      database = env.getDatabase(DB_NAME);
      
      assertStoreEmpty();            
   }
   
   public void testRemoveTransactional() throws Exception
   {
      byte[] bytes = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
       
      long id = 1;
      
      int offset = 0;
      
      database.put(null, id, bytes, offset, bytes.length);
      
      assertContainsPair(id, bytes, 1);
                        
      BDBJETransaction tx = env.createTransaction();
      
      database.remove(tx, id);
      
      tx.commit();
      
      assertStoreEmpty();        
   }
   
   public void testRemoveTransactionalWithRestart() throws Exception
   {
      byte[] bytes = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
       
      long id = 1;
      
      int offset = 0;
      
      database.put(null, id, bytes, offset, bytes.length);
      
      assertContainsPair(id, bytes, 1);
                        
      BDBJETransaction tx = env.createTransaction();
      
      database.remove(tx, id);
      
      // Now restart
      
      database.close();
      
      env.stop();
      
      env.start();
      
      database = env.getDatabase(DB_NAME);
      
      assertContainsPair(id, bytes, 1);       
   }
   
   public void testRemoveXACommit() throws Exception
   {
      byte[] bytes = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
       
      long id = 1;
      
      int offset = 0;
            
      database.put(null, id, bytes, offset, bytes.length);
      
      assertContainsPair(id, bytes, 1);
      
      Xid xid = generateXid();
      
      env.startWork(xid);
                             
      database.remove(null, id);
      
      env.endWork(xid, false);
      
      env.prepare(xid);
      
      env.commit(xid);

      assertStoreEmpty();        
   }
   
   public void testRemoveXAWithRestart() throws Exception
   {
      byte[] bytes = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
       
      long id = 1;
      
      int offset = 0;
            
      database.put(null, id, bytes, offset, bytes.length);
      
      assertContainsPair(id, bytes, 1);
      
      Xid xid = generateXid();
      
      env.startWork(xid);
                             
      database.remove(null, id);
      
      env.endWork(xid, false);
      
      // Now restart
      
      database.close();
      
      env.stop();
      
      env.start();
      
      database = env.getDatabase(DB_NAME);

      assertContainsPair(id, bytes, 1);     
   }
   
   public void testRemoveXAWithRestartAfterPrepare() throws Exception
   {
      byte[] bytes = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
       
      long id = 1;
      
      int offset = 0;
            
      database.put(null, id, bytes, offset, bytes.length);
      
      assertContainsPair(id, bytes, 1);
      
      Xid xid = generateXid();
      
      env.startWork(xid);
                             
      database.remove(null, id);
      
      env.endWork(xid, false);
      
      env.prepare(xid);
      
      // Now restart
      
      database.close();
      
      env.stop();
      
      env.start();
      
      database = env.getDatabase(DB_NAME);

      assertContainsPair(id, bytes, 1);     
   }
   
   public void testPutTransactionalRollback() throws Exception
   {
      byte[] bytes = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
       
      long id = 1;
      
      int offset = 0;
      
      BDBJETransaction tx = env.createTransaction();
      
      database.put(tx, id, bytes, offset, bytes.length);
      
      tx.rollback();
      
      assertStoreEmpty();            
   }
   
   public void testPutXARollback() throws Exception
   {
      byte[] bytes = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
       
      long id = 1;
      
      int offset = 0;
      
      Xid xid = generateXid();
      
      env.startWork(xid);
      
      database.put(null, id, bytes, offset, bytes.length);
      
      env.endWork(xid, false);
      
      env.prepare(xid);
      
      env.rollback(xid);
      
      assertStoreEmpty();            
   }
      
   public void testRemoveTransactionalRollback() throws Exception
   {
      byte[] bytes = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
       
      long id = 1;
      
      int offset = 0;
      
      database.put(null, id, bytes, offset, bytes.length);
      
      assertContainsPair(id, bytes, 1);
                        
      BDBJETransaction tx = env.createTransaction();
      
      database.remove(tx, id);
      
      tx.rollback();
      
      assertContainsPair(id, bytes, 1);            
   }
   
   public void testRemoveXARollback() throws Exception
   {
      byte[] bytes = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
       
      long id = 1;
      
      int offset = 0;
      
      database.put(null, id, bytes, offset, bytes.length);
      
      assertContainsPair(id, bytes, 1);
                        
      Xid xid = generateXid();
      
      env.startWork(xid);
      
      database.remove(null, id);
      
      env.endWork(xid, false);
      
      env.prepare(xid);
      
      env.rollback(xid);
      
      assertContainsPair(id, bytes, 1);            
   }
   
   
   public void testPutAndRemoveMultipleTransactionalCommit() throws Exception
   {
      byte[] bytes1 = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
      
      byte[] bytes2 = new byte[] { 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 };
      
      byte[] bytes3 = new byte[] { 21, 22, 23, 24, 25, 26, 27, 28, 29, 30 };
      
      byte[] bytes4 = new byte[] { 31, 32, 33, 34, 35, 36, 37, 38, 39, 40 };
       
      long id1 = 1;
      
      long id2 = 2;
      
      long id3 = 3;
      
      long id4 = 4;
      
      int offset = 0;
      
      database.put(null, id1, bytes1, offset, bytes1.length);
      
      database.put(null, id2, bytes2, offset, bytes2.length);
                 
      assertContainsPair(id1, bytes1, 2);
      
      assertContainsPair(id2, bytes2, 2);
      
      BDBJETransaction tx = env.createTransaction();
      
      database.put(tx, id3, bytes3, offset, bytes3.length);
      
      database.put(tx, id4, bytes4, offset, bytes4.length);
      
      database.remove(tx, id1);
      
      database.remove(tx, id2);
      
      tx.commit();
      
      assertContainsPair(id3, bytes3, 2);
      
      assertContainsPair(id4, bytes4, 2);        
   }
   
   public void testPutAndRemoveMultipleXACommit() throws Exception
   {
      byte[] bytes1 = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
      
      byte[] bytes2 = new byte[] { 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 };
      
      byte[] bytes3 = new byte[] { 21, 22, 23, 24, 25, 26, 27, 28, 29, 30 };
      
      byte[] bytes4 = new byte[] { 31, 32, 33, 34, 35, 36, 37, 38, 39, 40 };
       
      long id1 = 1;
      
      long id2 = 2;
      
      long id3 = 3;
      
      long id4 = 4;
      
      int offset = 0;
      
      database.put(null, id1, bytes1, offset, bytes1.length);
      
      database.put(null, id2, bytes2, offset, bytes2.length);
                 
      assertContainsPair(id1, bytes1, 2);
      
      assertContainsPair(id2, bytes2, 2);
      
      XidImpl xid = generateXid();
      
      env.startWork(xid);
      
      database.put(null, id3, bytes3, offset, bytes3.length);
      
      database.put(null, id4, bytes4, offset, bytes4.length);
      
      database.remove(null, id1);
      
      database.remove(null, id2);
      
      env.endWork(xid, false);
      
      env.prepare(xid);
      
      env.commit(xid);
      
      assertContainsPair(id3, bytes3, 2);
      
      assertContainsPair(id4, bytes4, 2);        
   }
   
   /*
    * As above but we use different but equal() XidImpl instances at each point
    */
   public void testPutAndRemoveMultipleXACommitWithCopy() throws Exception
   {
      byte[] bytes1 = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
      
      byte[] bytes2 = new byte[] { 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 };
      
      byte[] bytes3 = new byte[] { 21, 22, 23, 24, 25, 26, 27, 28, 29, 30 };
      
      byte[] bytes4 = new byte[] { 31, 32, 33, 34, 35, 36, 37, 38, 39, 40 };
       
      long id1 = 1;
      
      long id2 = 2;
      
      long id3 = 3;
      
      long id4 = 4;
      
      int offset = 0;
      
      database.put(null, id1, bytes1, offset, bytes1.length);
      
      database.put(null, id2, bytes2, offset, bytes2.length);
                 
      assertContainsPair(id1, bytes1, 2);
      
      assertContainsPair(id2, bytes2, 2);
      
      XidImpl xid = generateXid();
      
      env.startWork(xid);
      
      database.put(null, id3, bytes3, offset, bytes3.length);
      
      database.put(null, id4, bytes4, offset, bytes4.length);
      
      database.remove(null, id1);
      
      database.remove(null, id2);
      
      xid = new XidImpl(xid);
            
      env.endWork(xid, false);
      
      xid = new XidImpl(xid);      
      
      env.prepare(xid);
      
      xid = new XidImpl(xid);
      
      env.commit(xid);
      
      assertContainsPair(id3, bytes3, 2);
      
      assertContainsPair(id4, bytes4, 2);        
   }
   
   public void testPutAndRemoveMultipleXACommitDifferentThread() throws Exception
   {
      byte[] bytes1 = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
      
      byte[] bytes2 = new byte[] { 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 };
      
      byte[] bytes3 = new byte[] { 21, 22, 23, 24, 25, 26, 27, 28, 29, 30 };
      
      byte[] bytes4 = new byte[] { 31, 32, 33, 34, 35, 36, 37, 38, 39, 40 };
       
      long id1 = 1;
      
      long id2 = 2;
      
      long id3 = 3;
      
      long id4 = 4;
      
      int offset = 0;
      
      database.put(null, id1, bytes1, offset, bytes1.length);
      
      database.put(null, id2, bytes2, offset, bytes2.length);
                 
      assertContainsPair(id1, bytes1, 2);
      
      assertContainsPair(id2, bytes2, 2);
      
      final XidImpl xid = generateXid();
      
      env.startWork(xid);
      
      database.put(null, id3, bytes3, offset, bytes3.length);
      
      database.put(null, id4, bytes4, offset, bytes4.length);
      
      database.remove(null, id1);
      
      database.remove(null, id2);
      
      env.endWork(xid, false);
      
      env.prepare(xid);
      
      //Commit on a different thread
      
      class MyThread extends Thread
      {
         volatile boolean failed;
         
         public void run()
         {
            try
            {
               env.commit(xid);
            }
            catch (Exception e)
            {
               failed = true;
            }
         }         
      }
      
      MyThread t = new MyThread();
      
      t.start();
      
      t.join();
      
      assertFalse(t.failed);
      
      assertContainsPair(id3, bytes3, 2);
      
      assertContainsPair(id4, bytes4, 2);        
   }
   
   public void testPutAndRemoveMultipleXAWithRestart() throws Exception
   {
      byte[] bytes1 = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
      
      byte[] bytes2 = new byte[] { 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 };
      
      byte[] bytes3 = new byte[] { 21, 22, 23, 24, 25, 26, 27, 28, 29, 30 };
      
      byte[] bytes4 = new byte[] { 31, 32, 33, 34, 35, 36, 37, 38, 39, 40 };
       
      long id1 = 1;
      
      long id2 = 2;
      
      long id3 = 3;
      
      long id4 = 4;
      
      int offset = 0;
      
      database.put(null, id1, bytes1, offset, bytes1.length);
      
      database.put(null, id2, bytes2, offset, bytes2.length);
                 
      assertContainsPair(id1, bytes1, 2);
      
      assertContainsPair(id2, bytes2, 2);
      
      Xid xid = generateXid();
      
      env.startWork(xid);
      
      database.put(null, id3, bytes3, offset, bytes3.length);
      
      database.put(null, id4, bytes4, offset, bytes4.length);
      
      database.remove(null, id1);
      
      database.remove(null, id2);
      
      env.endWork(xid, false);
      
      // Now restart
      
      database.close();
      
      env.stop();
      
      env.start();
      
      database = env.getDatabase(DB_NAME);
      
      assertContainsPair(id1, bytes1, 2);
      
      assertContainsPair(id2, bytes2, 2);      
   }
   
   public void testPutAndRemoveMultipleTransactionalRollback() throws Exception
   {
      byte[] bytes1 = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
      
      byte[] bytes2 = new byte[] { 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 };
      
      byte[] bytes3 = new byte[] { 21, 22, 23, 24, 25, 26, 27, 28, 29, 30 };
      
      byte[] bytes4 = new byte[] { 31, 32, 33, 34, 35, 36, 37, 38, 39, 40 };
       
      long id1 = 1;
      
      long id2 = 2;
      
      long id3 = 3;
      
      long id4 = 4;
      
      int offset = 0;
      
      database.put(null, id1, bytes1, offset, bytes1.length);
      
      database.put(null, id2, bytes2, offset, bytes2.length);
                 
      assertContainsPair(id1, bytes1, 2);
      
      assertContainsPair(id2, bytes2, 2);
      
      BDBJETransaction tx = env.createTransaction();
      
      database.put(tx, id3, bytes3, offset, bytes3.length);
      
      database.put(tx, id4, bytes4, offset, bytes4.length);
      
      database.remove(tx, id1);
      
      database.remove(tx, id2);
      
      tx.rollback();
      
      assertContainsPair(id1, bytes1, 2);
      
      assertContainsPair(id2, bytes2, 2);            
   }
   
   public void testPutAndRemoveMultipleXARollback() throws Exception
   {
      byte[] bytes1 = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
      
      byte[] bytes2 = new byte[] { 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 };
      
      byte[] bytes3 = new byte[] { 21, 22, 23, 24, 25, 26, 27, 28, 29, 30 };
      
      byte[] bytes4 = new byte[] { 31, 32, 33, 34, 35, 36, 37, 38, 39, 40 };
       
      long id1 = 1;
      
      long id2 = 2;
      
      long id3 = 3;
      
      long id4 = 4;
      
      int offset = 0;
      
      database.put(null, id1, bytes1, offset, bytes1.length);
      
      database.put(null, id2, bytes2, offset, bytes2.length);
                 
      assertContainsPair(id1, bytes1, 2);
      
      assertContainsPair(id2, bytes2, 2);
      
      Xid xid = generateXid();
      
      env.startWork(xid);
      
      database.put(null, id3, bytes3, offset, bytes3.length);
      
      database.put(null, id4, bytes4, offset, bytes4.length);
      
      database.remove(null, id1);
      
      database.remove(null, id2);
      
      env.endWork(xid, false);
      
      env.prepare(xid);
      
      env.rollback(xid);
      
      assertContainsPair(id1, bytes1, 2);
      
      assertContainsPair(id2, bytes2, 2);            
   }
   
   public void testOverwiteNonTransactional() throws Exception
   {
      byte[] bytes1 = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
      
      byte[] bytes2 = new byte[] { 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 };
       
      long id = 1;
      
      int offset = 0;
      
      database.put(null, id, bytes1, offset, bytes1.length);
      
      assertContainsPair(id, bytes1, 1);
      
      database.put(null, id, bytes2, offset, bytes1.length);
      
      assertContainsPair(id, bytes2, 1);
            
      database.remove(null, 1);
      
      assertStoreEmpty();
   }
   
   public void testOverwiteTransactionalCommit() throws Exception
   {
      byte[] bytes1 = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
      
      byte[] bytes2 = new byte[] { 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 };
       
      long id = 1;
      
      int offset = 0;
      
      BDBJETransaction tx = env.createTransaction();
            
      database.put(tx, id, bytes1, offset, bytes1.length);
      
      database.put(tx, id, bytes2, offset, bytes1.length);
      
      tx.commit();
      
      assertContainsPair(id, bytes2, 1);
            
      database.remove(null, 1);
      
      assertStoreEmpty();
   }
   
   public void testOverwiteXACommit() throws Exception
   {
      byte[] bytes1 = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
      
      byte[] bytes2 = new byte[] { 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 };
       
      long id = 1;
      
      int offset = 0;
      
      Xid xid = generateXid();
      
      env.startWork(xid);
      
      database.put(null, id, bytes1, offset, bytes1.length);
      
      database.put(null, id, bytes2, offset, bytes1.length);
      
      env.endWork(xid, false);
      
      env.prepare(xid);
      
      env.commit(xid);
      
      assertContainsPair(id, bytes2, 1);
            
      database.remove(null, 1);
      
      assertStoreEmpty();
   }
   
   public void testOverwiteTransactionalRollback() throws Exception
   {
      byte[] bytes1 = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
      
      byte[] bytes2 = new byte[] { 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 };
       
      long id = 1;
      
      int offset = 0;
      
      BDBJETransaction tx = env.createTransaction();
            
      database.put(tx, id, bytes1, offset, bytes1.length);
      
      database.put(tx, id, bytes2, offset, bytes1.length);
      
      tx.rollback();
      
      assertStoreEmpty();
   }
   
   public void testOverwiteXARollback() throws Exception
   {
      byte[] bytes1 = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
      
      byte[] bytes2 = new byte[] { 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 };
       
      long id = 1;
      
      int offset = 0;
      
      Xid xid = generateXid();
      
      env.startWork(xid);      
      
      database.put(null, id, bytes1, offset, bytes1.length);
      
      database.put(null, id, bytes2, offset, bytes1.length);
      
      env.endWork(xid, false);
      
      env.prepare(xid);
      
      env.rollback(xid);
      
      assertStoreEmpty();
   }
   
   public void testPutAndRemovePartialNonTransactional() throws Exception
   {
      byte[] bytes = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
      
      byte[] bytes2 = new byte[] { 20, 21, 22, 23 };
       
      long id = 1;
      
      int offset = 0;
      
      database.put(null, id, bytes, offset, bytes.length);
      
      assertContainsPair(id, bytes, 1);
      
      database.put(null, id, bytes2, 10, bytes2.length);
      
      byte[] bytes3 = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 21, 22, 23 };
      
      assertContainsPair(id, bytes3, 1);  
            
      database.put(null, id, bytes2, 3, bytes2.length);
      
      byte[] bytes4 = new byte[] { 1, 2, 3, 20, 21, 22, 23, 8, 9, 10, 20, 21, 22, 23 };
      
      assertContainsPair(id, bytes4, 1);  
      
      byte[] bytes5 = new byte[0];
      
      //blank out 4 bytes
      database.put(null, id, bytes5, 5, 4);
      
      byte[] bytes6 = new byte[] { 1, 2, 3, 20, 21, 10, 20, 21, 22, 23 };
      
      assertContainsPair(id, bytes6, 1);  
      
      
      database.put(null, id, new byte[0], 0, 4);
      
      byte[] bytes7 = new byte[] { 21, 10, 20, 21, 22, 23 };
      
      assertContainsPair(id, bytes7, 1);  
                        
      database.remove(null, 1);
      
      assertStoreEmpty();
   }
   
   public void testPutAndRemovePartialTransactional() throws Exception
   {
      byte[] bytes = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
      
      byte[] bytes2 = new byte[] { 20, 21, 22, 23 };
       
      long id = 1;
      
      int offset = 0;
      
      BDBJETransaction tx = env.createTransaction();
            
      database.put(tx, id, bytes, offset, bytes.length);
      
      tx.commit();
      
      assertContainsPair(id, bytes, 1);
      
      tx = env.createTransaction();
      
      database.put(tx, id, bytes2, 10, bytes2.length);
      
      tx.commit();
      
      byte[] bytes3 = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 21, 22, 23 };
      
      assertContainsPair(id, bytes3, 1);  
      
      tx = env.createTransaction();
            
      database.put(tx, id, bytes2, 3, bytes2.length);
      
      tx.commit();
      
      byte[] bytes4 = new byte[] { 1, 2, 3, 20, 21, 22, 23, 8, 9, 10, 20, 21, 22, 23 };
      
      assertContainsPair(id, bytes4, 1);  
      
      byte[] bytes5 = new byte[0];
      
      tx = env.createTransaction();
      
      //blank out 4 bytes
      database.put(tx, id, bytes5, 5, 4);
      
      tx.commit();
      
      byte[] bytes6 = new byte[] { 1, 2, 3, 20, 21, 10, 20, 21, 22, 23 };
      
      assertContainsPair(id, bytes6, 1);  
      
      database.put(null, id, new byte[0], 0, 4);
      
      byte[] bytes7 = new byte[] { 21, 10, 20, 21, 22, 23 };
      
      assertContainsPair(id, bytes7, 1);  
                  
      database.remove(null, 1);
      
      assertStoreEmpty();
   }
   
   public void testSetAndGetEnvironment() throws Exception   
   {
      try
      {
         env.setEnvironmentPath("blah");
         
         fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         //OK
      }
      
      assertEquals(ENV_DIR, env.getEnvironmentPath());
      
      env.stop();
      
      final String path2 = HOME_DIR + "/test-path123651";
      
      env.setEnvironmentPath(path2);
      
      assertEquals(path2, env.getEnvironmentPath());
      
      env.start();
   }
   
   
   public void testSetAndGetTransacted() throws Exception   
   {
      env.stop();
            
      env.setTransacted(false);
      
      assertFalse(env.isTransacted());
      
      env.setTransacted(true);
      
      assertTrue(env.isTransacted());
      
      env.start();
      
      try
      {
         env.setTransacted(true);
         
         fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         //OK
      }
      
      assertTrue(env.isTransacted());
      
      env.stop();
      
      env.setTransacted(false);
      
      assertFalse(env.isTransacted());  
   }
   
   public void testSetAndGetSyncOS() throws Exception   
   {
      env.stop();
      
      assertFalse(env.isSyncOS());
      
      env.setSyncOS(true);
      
      assertTrue(env.isSyncOS());
      
      env.start();
      
      try
      {
         env.setSyncOS(true);
         
         fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         //OK
      }
      
      assertTrue(env.isSyncOS());
      
      env.stop();
      
      env.setSyncOS(false);
      
      assertFalse(env.isSyncOS());  
   }
   
   public void testSetAndGetSyncVM() throws Exception   
   {
      env.stop();
      
      assertFalse(env.isSyncVM());
      
      env.setSyncVM(true);
      
      assertTrue(env.isSyncVM());
      
      env.start();
      
      try
      {
         env.setSyncVM(true);
         
         fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         //OK
      }
      
      assertTrue(env.isSyncVM());
      
      env.stop();
      
      env.setSyncVM(false);
      
      assertFalse(env.isSyncVM());  
   }      
   
   public void testSetAndGetMemoryCacheSize() throws Exception   
   {
      env.stop();
      
      assertEquals(-1, env.getMemoryCacheSize());
      
      final long size = 16251762;
      
      env.setMemoryCacheSize(size);
      
      assertEquals(size, env.getMemoryCacheSize());
      
      env.start();
      
      try
      {
         env.setMemoryCacheSize(1897291289);
         
         fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         //OK
      }
      
      assertEquals(size, env.getMemoryCacheSize());
      
      env.stop();
      
      final long size2 = 1625534783;
      
      env.setMemoryCacheSize(size2);
      
      assertEquals(size2, env.getMemoryCacheSize());
   }
   

   public void testStartAndStop() throws Exception
   {      
      try
      {
         env.start();
         fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         //OK
      }
      
      env.stop();
      
      final String path = HOME_DIR + "/test-path123";
      
      createDir(path);
      
      env.setEnvironmentPath(path);      
      
      env.start();
      
      try
      {
         env.start();
         
         fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         //OK
      }
      
      env.stop();
      
      try
      {
         env.stop();
         
         fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         //OK
      }
   }
   
   /*
    * This test only makes sense for the RealBDBJEEnvironment, not the fake one
    */
   public void testCreateEnvironment() throws Exception
   {      
      if (isFake())
      {
         return;
      }
      
      env.stop();
      
      env.setCreateEnvironment(false);
      
      String dir = HOME_DIR + "/blah678";
      
      deleteDirectory(new File(dir));
      
      env.setEnvironmentPath(dir);
      
      try
      {
         env.start();
         fail("Should throw exception");
      }
      catch (IllegalArgumentException e)
      {
         //OK
      }
      
      this.createDir(dir);
      
      try
      {
         env.start();         
      }
      catch (DatabaseException e)
      {
         //Ok - this will still fail since there is no actual BDB environment in the directory even though
         //the directory exists
      }
      
      env.setCreateEnvironment(true);
      
      env.start();
     
      env.stop();
      
      deleteDirectory(new File(dir));
      
      env.start();             
   }
   
   public void testWrongOrderCommit() throws Exception
   {
      testXAWrongOrder(true);
   }
   
   public void testWrongOrderRollback() throws Exception
   {
      testXAWrongOrder(false);
   }
   
   public void testXAWrongXidCommit() throws Exception
   {
      testXAWrongXid(true);
   }
   
   public void testXAWrongXidRollback() throws Exception
   {
      testXAWrongXid(false);
   }
   
   private void testXAWrongXid(boolean commit) throws Exception
   {
      Xid xid = generateXid();
      
      env.startWork(xid);
      
      Xid xid2 = generateXid();
      
      try
      {
         env.endWork(xid2, false);
         fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         //Ok
      }
      
      //do some work
      
      database.put(null, 23, new byte[10], 0, 10);
            
      env.endWork(xid, false);
      
      try
      {
         env.prepare(xid2);
         fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         //Ok
      }
      
      env.prepare(xid);
      
      if (commit)
      {
         try
         {
            env.commit(xid2);
            fail("Should throw exception");
         }
         catch (IllegalStateException e)
         {
            //Ok
         }
         env.commit(xid);
      }
      else
      {
         try
         {
            env.rollback(xid2);
            fail("Should throw exception");
         }
         catch (IllegalStateException e)
         {
            //Ok
         }
         env.rollback(xid);
      }
   }
   
   private void testXAWrongOrder(boolean commit) throws Exception
   {
      Xid xid = generateXid();
      
      try
      {
         env.endWork(xid, false);
         fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         //Ok
      }
      
      try
      {
         env.prepare(xid);
         fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         //Ok
      }
      
      try
      {
         env.commit(xid);
         fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         //Ok
      }
      
      try
      {
         env.rollback(xid);
         fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         //Ok
      }
      
      env.startWork(xid);
      
      //do some work
      
      database.put(null, 23, new byte[10], 0, 10);
      
      try
      {
         env.startWork(xid);
         fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         //Ok
      }
      
      try
      {
         env.prepare(xid);
         fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         //Ok
      }
      
      try
      {
         env.commit(xid);
         fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         //Ok
      }
      
      try
      {
         env.rollback(xid);
         fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         //Ok
      }
      
      env.endWork(xid, false);
      
      try
      {
         env.startWork(xid);
         fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         //Ok
      }
      
      try
      {
         env.endWork(xid, false);
         fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         //Ok
      }
      
      try
      {
         env.commit(xid);
         fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         //Ok
      }
      
      try
      {
         env.rollback(xid);
         fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         //Ok
      }
      
      env.prepare(xid);
      
      try
      {
         env.startWork(xid);
         fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         //Ok
      }
      
      try
      {
         env.endWork(xid, false);
         fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         //Ok
      }
      
      if (commit)
      {
         env.commit(xid);
      }
      else
      {
         env.rollback(xid);
      }
      
      try
      {
         env.endWork(xid, false);
         fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         //Ok
      }
      
      try
      {
         env.prepare(xid);
         fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         //Ok
      }
      
      try
      {
         env.commit(xid);
         fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         //Ok
      }
      
      try
      {
         env.rollback(xid);
         fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         //Ok
      }
      
      env.startWork(xid);
      
      database.put(null, 23, new byte[10], 0, 10);
      
      env.endWork(xid, false);
      
      env.prepare(xid);
      
      env.rollback(xid);
      
   }
     
   // Private -------------------------------------------------------------------------------------
   
   private void assertContainsPair(long id, byte[] bytes, long size) throws Exception
   {
      byte[] b = database.get(id);
      
      assertNotNull(b);
      
      assertByteArraysEquivalent(bytes, b);
      
      assertEquals(size, database.size());
   }
   
   private void assertStoreEmpty() throws Exception
   {
      assertEquals(0, database.size());
   }               
}
