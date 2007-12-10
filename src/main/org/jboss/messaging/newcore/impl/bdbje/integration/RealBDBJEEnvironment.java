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
package org.jboss.messaging.newcore.impl.bdbje.integration;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.jboss.messaging.newcore.impl.bdbje.BDBJEDatabase;
import org.jboss.messaging.newcore.impl.bdbje.BDBJEEnvironment;
import org.jboss.messaging.newcore.impl.bdbje.BDBJETransaction;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.XAEnvironment;

/**
 * 
 * A RealBDBJEEnvironment
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class RealBDBJEEnvironment implements BDBJEEnvironment
{
   /**
    * The actual DB environment
    */
   private XAEnvironment environment;

   /**
    * The path of the environment
    */
   private String environmentPath;

   /**
    * Is the environment transacted?
    */
   private boolean transacted;

   /**
    * Do we sync OS buffers to disk on transaction commit?
    */
   private boolean syncOS;

   /**
    * Do we sync to the OS on transaction commit ?
    */
   private boolean syncVM;     

   /** 
    * Memory cache size in bytes, or -1 to use BDB default
    * 
    */
   private long memoryCacheSize = -1;

   /**
    * Are we started?
    */
   private volatile boolean started;

   /**
    * Are we in debug mode? Used in testing
    */
   private boolean debug;

   /**
    * Used for debug only to ensure the XA operations are called in the right order
    */
   private Map<Thread, ThreadTXStatus> threadTXStatuses;

   public RealBDBJEEnvironment(boolean debug)
   {
      this.debug = debug;

      if (debug)
      {
         threadTXStatuses = new ConcurrentHashMap<Thread, ThreadTXStatus>();
      }
   }

   public synchronized void start() throws Exception
   {      
      if (started)
      {
         throw new IllegalStateException("Already started");
      }
      if (environmentPath == null)
      {
         throw new IllegalStateException("environmentPath has not been specified");
      }

      EnvironmentConfig envConfig = new EnvironmentConfig();

      if (memoryCacheSize != -1)
      {
         envConfig.setCacheSize(memoryCacheSize);
      }

      envConfig.setTxnNoSync(!syncOS);

      envConfig.setTxnWriteNoSync(!syncVM);

      envConfig.setAllowCreate(true);

      envConfig.setTransactional(transacted);

      environment = new XAEnvironment(new File(environmentPath), envConfig);

      DatabaseConfig dbConfig = new DatabaseConfig();

      dbConfig.setTransactional(transacted);

      dbConfig.setAllowCreate(true);

      started = true;      
   }

   public synchronized void stop() throws Exception
   {
      if (!started)
      {
         throw new IllegalStateException("Not started");
      }

      try
      {
         environment.close();
      }
      catch (Exception ignore)
      {
         //Environment close might fail since there are open transactions - this is ok
      }

      started = false;      
   }

   public BDBJETransaction createTransaction() throws Exception
   {
      return new RealBDBJETransaction(environment.beginTransaction(null, null));
   }

   public BDBJEDatabase getDatabase(String databaseName) throws Exception
   {
      DatabaseConfig dbConfig = new DatabaseConfig();

      dbConfig.setTransactional(transacted);

      dbConfig.setAllowCreate(true);

      Database database = environment.openDatabase(null, databaseName, dbConfig); 

      BDBJEDatabase db = new RealBDBJEDatabase(database);

      return db;
   }

   public String getEnvironmentPath()
   {
      return this.environmentPath;
   }

   public long getMemoryCacheSize()
   {
      return this.memoryCacheSize;
   }

   public boolean isSyncOS()
   {
      return this.syncOS;
   }

   public boolean isSyncVM()
   {
      return this.syncVM;
   }

   public boolean isTransacted()
   {
      return this.transacted;
   }

   public void setEnvironmentPath(String environmentPath)
   {
      if (started)
      {
         throw new IllegalStateException("Cannot set EnvironmentPath when started");
      }
      this.environmentPath = environmentPath;
   }

   public void setMemoryCacheSize(long size)
   {
      if (started)
      {
         throw new IllegalStateException("Cannot set MemoryCacheSize when started");
      }
      this.memoryCacheSize = size;
   }

   public void setSyncOS(boolean sync)
   {
      if (started)
      {
         throw new IllegalStateException("Cannot set SyncOS when started");
      }
      this.syncOS = sync;
   }

   public void setSyncVM(boolean sync)
   {
      if (started)
      {
         throw new IllegalStateException("Cannot set SyncVM when started");
      }
      this.syncVM = sync;
   }

   public void setTransacted(boolean transacted)
   {
      if (started)
      {
         throw new IllegalStateException("Cannot set Transacted when started");
      }
      this.transacted = transacted;
   }

   public List<Xid> getInDoubtXids() throws Exception
   {
      Xid[] xids = environment.recover(XAResource.TMSTARTRSCAN);

      List<Xid> list = Arrays.asList(xids);

      return list;
   }

   public void startWork(Xid xid) throws Exception
   {
      if (debug)
      {
         checkNoStatus();

         threadTXStatuses.put(Thread.currentThread(), new ThreadTXStatus(xid));
      }

      environment.start(xid, XAResource.TMNOFLAGS);
   }

   public void endWork(Xid xid, boolean failed) throws Exception
   {
      if (debug)
      {
         checkXAState(xid, XAState.IN_WORK);

         setXAState(XAState.DONE_WORK);
      }

      environment.end(xid, failed ? XAResource.TMFAIL : XAResource.TMSUCCESS);
   }
     
   public void prepare(Xid xid) throws Exception
   {
      if (debug)
      {
         checkXAState(xid, XAState.DONE_WORK);

         setXAState(XAState.PREPARE_CALLED);
      }

      environment.prepare(xid);
   }

   public void commit(Xid xid) throws Exception
   {
      if (debug)
      {
         checkXAState(xid, XAState.PREPARE_CALLED);

         threadTXStatuses.remove(Thread.currentThread());
      }

      environment.commit(xid, false);       
   }   

   public void rollback(Xid xid) throws Exception
   {
      if (debug)
      {
         checkXAState(xid, XAState.PREPARE_CALLED);

         threadTXStatuses.remove(Thread.currentThread());
      }

      environment.rollback(xid);
   }

   // Private -------------------------------------------------------------------------

   /*
    * Used for debug only
    */
   private ThreadTXStatus getTxStatus()
   {
      return threadTXStatuses.get(Thread.currentThread());
   }
   
   private void checkXAState(Xid xid, XAState state)
   {
      ThreadTXStatus status = getTxStatus();

      if (status == null)
      {
         throw new IllegalStateException("Not started any xa work");
      }
      
      if (!state.equals(status.state))
      {
         throw new IllegalStateException("Invalid XAState expected " + state + " got " + status.state);
      }
      
      if (xid != status.implicitXid)
      {
         throw new IllegalStateException("Wrong xid");
      }
   }
   
   private void checkNoStatus()
   {
      ThreadTXStatus status = getTxStatus();

      if (status != null)
      {
         throw new IllegalStateException("XA status should not exist");
      }
   }
   
   private void setXAState(XAState state)
   {
      threadTXStatuses.get(Thread.currentThread()).state = state;
   }
   

   // Inner classes --------------------------------------------------------------------

   private enum XAState
   {
      NOT_STARTED, IN_WORK, DONE_WORK, PREPARE_CALLED
   }
   
   /*
    * Used for debug only
    */
   private class ThreadTXStatus
   {
      ThreadTXStatus(Xid xid)
      {
         this.implicitXid = xid;      
      }

      Xid implicitXid;

      XAState state = XAState.IN_WORK;
   }

}
