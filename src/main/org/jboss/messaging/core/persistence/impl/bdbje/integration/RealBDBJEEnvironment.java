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
package org.jboss.messaging.core.persistence.impl.bdbje.integration;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.persistence.impl.bdbje.BDBJEDatabase;
import org.jboss.messaging.core.persistence.impl.bdbje.BDBJEEnvironment;
import org.jboss.messaging.core.persistence.impl.bdbje.BDBJETransaction;

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
   private static final Logger log = Logger.getLogger(RealBDBJEEnvironment.class);
      
   /**
    * The actual DB environment
    */
   private XAEnvironment environment;

   /**
    * The path of the environment
    */
   private String environmentPath;
   
   /**
    * Create the environment path and environment if doesn't already exist
    */
   private boolean createEnvironment;

   /**
    * Is the environment transacted?
    */
   private boolean transacted = true;

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
   private final boolean debug;

   /**
    * Used for debug only to ensure the XA operations are called in the right order
    */
   private Map<Thread, ThreadTXStatus> threadTXStatuses;   
   private Map<Xid, ThreadTXStatus> transactions;
   
   public RealBDBJEEnvironment()
   {      
   	this.debug = false;
   }
   
   public RealBDBJEEnvironment(final boolean debug)
   {
      this.debug = debug;

      if (debug)
      {
         threadTXStatuses = new ConcurrentHashMap<Thread, ThreadTXStatus>();
         
         transactions = new ConcurrentHashMap<Xid, ThreadTXStatus>();
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

      File envDir = new File(environmentPath);
      
      if (!envDir.exists())
      {
         if (createEnvironment)
         {
            if (!envDir.mkdirs())
            {
               throw new IOException("Failed to create directory structure " + environmentPath);
            }
         }
         else
         {
            throw new IllegalArgumentException("Cannot start BDBJEEnvironment, since environment path " +
                                               environmentPath + " does not exist and property createEnvironment is false");
         }
      }
      
      EnvironmentConfig envConfig = new EnvironmentConfig();

      if (memoryCacheSize != -1)
      {
         envConfig.setCacheSize(memoryCacheSize);
      }

      envConfig.setTxnNoSync(!syncOS);

      envConfig.setTxnWriteNoSync(!syncVM);

      envConfig.setAllowCreate(createEnvironment);

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
   
   public synchronized boolean isStarted()
   {
      return started;
   }

   public BDBJETransaction createTransaction() throws Exception
   {
      return new RealBDBJETransaction(environment.beginTransaction(null, null));
   }

   public BDBJEDatabase getDatabase(final String databaseName) throws Exception
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
   
   public boolean isCreateEnvironment()
   {
      return createEnvironment;
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

   public void setEnvironmentPath(final String environmentPath)
   {
      if (started)
      {
         throw new IllegalStateException("Cannot set EnvironmentPath when started");
      }
      this.environmentPath = environmentPath;
   }
   
   public void setCreateEnvironment(final boolean createEnvironment)
   {
      if (started)
      {
         throw new IllegalStateException("Cannot set CreateEnvironment when started");
      }
      this.createEnvironment = createEnvironment;
   }

   public void setMemoryCacheSize(final long size)
   {
      if (started)
      {
         throw new IllegalStateException("Cannot set MemoryCacheSize when started");
      }
      this.memoryCacheSize = size;
   }

   public void setSyncOS(final boolean sync)
   {
      if (started)
      {
         throw new IllegalStateException("Cannot set SyncOS when started");
      }
      this.syncOS = sync;
   }

   public void setSyncVM(final boolean sync)
   {
      if (started)
      {
         throw new IllegalStateException("Cannot set SyncVM when started");
      }
      this.syncVM = sync;
   }

   public void setTransacted(final boolean transacted)
   {
      if (started)
      {
         throw new IllegalStateException("Cannot set Transacted when started");
      }
      this.transacted = transacted;
   }

   public List<Xid> getInDoubtXids() throws Exception
   {
      Xid[] xids = environment.recover(XAResource.TMSTARTRSCAN & XAResource.TMENDRSCAN);

      List<Xid> list = Arrays.asList(xids);
      
      return list;
   }

   public void startWork(final Xid xid) throws Exception
   {
      if (debug)
      {
         checkStartWork(xid);   
      }

      environment.start(xid, XAResource.TMNOFLAGS);
   }

   public void endWork(final Xid xid, final boolean failed) throws Exception
   {
      if (debug)
      {
         checkEndWork(xid);                  
      }

      environment.end(xid, failed ? XAResource.TMFAIL : XAResource.TMSUCCESS);
   }
   
   public void prepare(final Xid xid) throws Exception
   {
      if (debug)
      {
         checkPrepare(xid);
      }

      environment.prepare(xid);
   }

   public void commit(final Xid xid) throws Exception
   {
      if (debug)
      {
         checkCommitRollback(xid);
      }

      environment.commit(xid, false);       
   }   

   public void rollback(final Xid xid) throws Exception
   {
      if (debug)
      {
         checkCommitRollback(xid);
      }
  
      environment.rollback(xid);
   }

   // Private -------------------------------------------------------------------------

   /*
    * Used for debug only
    */
   
   private void checkStartWork(final Xid xid)
   {
      if (threadTXStatuses.get(Thread.currentThread()) != null)
      {
         throw new IllegalStateException("Already implicit transaction");
      }
      
      if (transactions.get(xid) != null)
      {
         throw new IllegalStateException("Already tx for xid");
      }            
      
      threadTXStatuses.put(Thread.currentThread(), new ThreadTXStatus(xid));
   }
   
   private void checkEndWork(final Xid xid)
   {
      ThreadTXStatus status = threadTXStatuses.get(Thread.currentThread());
      
      if (status == null)
      {
         throw new IllegalStateException("No implicit transaction");
      }            
      
      if (!status.implicitXid.equals(xid))
      {
         throw new IllegalStateException("Wrong xid");
      }
      
      threadTXStatuses.remove(Thread.currentThread());
      
      transactions.put(xid, status);
   }
   
   private void checkPrepare(final Xid xid)
   {
      ThreadTXStatus status = this.transactions.get(xid);
      
      if (status == null)
      {
         throw new IllegalStateException("Cannot find tx for xid");
      }
      
      if (this.threadTXStatuses.get(Thread.currentThread()) != null)
      {
         throw new IllegalStateException("Implicit transaction exists");
      }
      
      status.prepared = true;
   }
      
   private void checkCommitRollback(final Xid xid)
   {
      ThreadTXStatus status = this.transactions.get(xid);
      
      if (status == null)
      {
         throw new IllegalStateException("Cannot find tx for xid");
      }
      
      if (!status.prepared)
      {
         throw new IllegalStateException("Tx not prepared");
      }
      
      if (this.threadTXStatuses.get(Thread.currentThread()) != null)
      {
         throw new IllegalStateException("Implicit transaction exists");
      }
      
      transactions.remove(xid);
   }
   
   // Inner classes --------------------------------------------------------------------

   /*
    * Used for debug only
    */
   private class ThreadTXStatus
   {
      ThreadTXStatus(Xid xid)
      {
         this.implicitXid = xid;      
      }

      final Xid implicitXid;

      volatile boolean prepared;
   }

}
