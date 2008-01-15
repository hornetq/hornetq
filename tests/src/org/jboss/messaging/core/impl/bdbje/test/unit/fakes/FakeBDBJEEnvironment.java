package org.jboss.messaging.core.impl.bdbje.test.unit.fakes;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.impl.bdbje.BDBJECursor;
import org.jboss.messaging.core.impl.bdbje.BDBJEDatabase;
import org.jboss.messaging.core.impl.bdbje.BDBJEEnvironment;
import org.jboss.messaging.core.impl.bdbje.BDBJETransaction;
import org.jboss.messaging.util.Pair;

/**
 * 
 * A FakeBDBJEEnvironment
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class FakeBDBJEEnvironment implements BDBJEEnvironment
{
   private String environmentPath;
   
   private Map<String, FakeBDBJETransaction> transactions = new HashMap<String, FakeBDBJETransaction>();
   
   private boolean transacted;
   
   private boolean syncOS;
   
   private boolean syncVM;     

   private long memoryCacheSize = -1;
   
   private boolean started;
         
   private Map<Thread, FakeBDBJETransaction> implicitTXs = new ConcurrentHashMap<Thread, FakeBDBJETransaction>();
   
   private Map<String, BDBJEDatabase> databases = new HashMap<String, BDBJEDatabase>();
                     
   public boolean isSyncOS()
   {
      return syncOS;
   }

   public void setSyncOS(boolean sync)
   {
      if (started)
      {
         throw new IllegalStateException("Cannot set SyncOS when started");
      }
      syncOS = sync;
   }

   public boolean isSyncVM()
   {
      return syncVM;
   }
   
   public void setSyncVM(boolean sync)
   {
      if (started)
      {
         throw new IllegalStateException("Cannot set SyncVM when started");
      }
      syncVM = sync;
   }

   public boolean isTransacted()
   {
      return transacted;
   }
   
   public void setTransacted(boolean transacted)
   {
      if (started)
      {
         throw new IllegalStateException("Cannot set transacted when started");
      }
      this.transacted = transacted;
   }

   public long getMemoryCacheSize()
   {
      return memoryCacheSize;
   }  

   public void setMemoryCacheSize(long size)
   {
      if (started)
      {
         throw new IllegalStateException("Cannot set memory cache size when started");
      }
      this.memoryCacheSize = size;
   }
   
   public void setEnvironmentPath(String environmentPath)
   {
      if (started)
      {
         throw new IllegalStateException("Cannot set environmentPath when started");
      }
      this.environmentPath = environmentPath;
   }
   
   private boolean createEnvironment;
   
   public boolean isCreateEnvironment()
   {
      return createEnvironment;
   }

   public void setCreateEnvironment(boolean create)
   {
      this.createEnvironment = create;
   }
   
   public String getEnvironmentPath()
   {
      return environmentPath;
   }
   
   public void start() throws Exception
   {
      if (environmentPath == null)
      {
         throw new IllegalStateException("Must set environmentPath before starting");
      }
      if (started)
      {
         throw new IllegalStateException("Already started");
      }
      started = true;
   }

   public void stop() throws Exception
   {
      if (!started)
      {
         throw new IllegalStateException("Not started");
      }
      started = false;
   }
   
   public boolean isStarted()
   {
      return started;
   }
   
   public BDBJETransaction createTransaction() throws Exception
   {
      return createTransactionInternal(null);
   }
   
   public synchronized BDBJEDatabase getDatabase(String databaseName) throws Exception
   {
      BDBJEDatabase database = databases.get(databaseName);
      
      if (database == null)
      {
         database = new FakeBDBJEDatabase();
         
         databases.put(databaseName, database);
      }
      
      return database;
   }

   public List<Xid> getInDoubtXids() throws Exception
   {
      List<Xid> xids = new ArrayList<Xid>();
      
      for (FakeBDBJETransaction tx: transactions.values())
      {
         if (tx.isPrepared())
         {
            xids.add(tx.getXid());
         }
      }
      
      return xids;
   }
   
   public void startWork(Xid xid) throws Exception
   {
      checkStartWork(xid);
      
      implicitTXs.put(Thread.currentThread(), createTransactionInternal(xid));            
   }
   
   public void endWork(Xid xid, boolean fail) throws Exception
   {
      checkEndWork(xid);
      
      implicitTXs.remove(Thread.currentThread());
   }
   
   public void prepare(Xid xid) throws Exception
   {
      FakeBDBJETransaction tx = findTXForXid(xid);
      
      checkPrepare(tx);
      
      tx.prepare();
   }
   
   public void commit(Xid xid) throws Exception
   {      
      FakeBDBJETransaction tx = findTXForXid(xid);
      
      checkCommitRollback(tx);
            
      tx.commit();
   }

   public void rollback(Xid xid) throws Exception
   {
      FakeBDBJETransaction tx = findTXForXid(xid);
      
      checkCommitRollback(tx);
                  
      tx.rollback();   
   }
         
   // Private -----------------------------------------------------------------------
   
   private FakeBDBJETransaction createTransactionInternal(Xid xid) throws Exception
   {
      FakeBDBJETransaction tx = new FakeBDBJETransaction(xid);
      
      transactions.put(tx.id, tx);
      
      return tx;
   }      
   
   private FakeBDBJETransaction findTXForXid(Xid xid)
   {
      for (FakeBDBJETransaction tx: transactions.values())
      {
         if (xid.equals(tx.xid))
         {
            return tx;
         }
      }
      return null;
   }
   
   private void checkStartWork(Xid xid)
   {
      if (implicitTXs.get(Thread.currentThread()) != null)
      {
         throw new IllegalStateException("Already implicit transaction");
      }
      
      if (findTXForXid(xid) != null)
      {
         throw new IllegalStateException("Already tx for xid");
      }            
   }
   
   private void checkEndWork(Xid xid)
   {
      FakeBDBJETransaction tx = implicitTXs.get(Thread.currentThread());
      
      if (tx == null)
      {
         throw new IllegalStateException("No implicit tx");
      }            
      
      if (!tx.xid.equals(xid))
      {
         throw new IllegalStateException("Wrong xid");
      }
   }

   
   private void checkPrepare(FakeBDBJETransaction tx)
   {
      if (tx == null)
      {
         throw new IllegalStateException("Cannot find tx for xid");
      }
      
      if (implicitTXs.containsKey(Thread.currentThread()))
      {
         throw new IllegalStateException("Work not ended");
      }
   }
   
   
   private void checkCommitRollback(FakeBDBJETransaction tx)
   {
      if (tx == null)
      {
         throw new IllegalStateException("Cannot find tx for xid");
      }
      
      if (!tx.prepared)
      {
         throw new IllegalStateException("Tx not prepared");
      }
      
      if (implicitTXs.containsKey(Thread.currentThread()))
      {
         throw new IllegalStateException("Work not ended");
      }
   }
   
   
   // Inner classes ------------------------------------------------------------------
   
   private class FakeBDBJETransaction implements BDBJETransaction
   {
      private String id = java.util.UUID.randomUUID().toString();
      
      private Xid xid;
      
      private List<Action> actions = new ArrayList<Action>();
      
      private boolean prepared;
      
      FakeBDBJETransaction(Xid xid)
      {
         this.xid = xid;
      }
      
      public void commit() throws Exception
      {
         for (Action action : actions)
         {
            if (action.put)
            {
               action.database.put(null, action.id, action.bytes, action.offset, action.length);
            }
            else
            {
               action.database.remove(null, action.id);
            }
         }
         
         actions.clear();
         
         transactions.remove(id);
      }

      public void prepare() throws Exception
      {                
         prepared = true;
      }

      public void rollback() throws Exception
      {
         actions.clear(); 
         
         transactions.remove(id);
      }
      
      public boolean isPrepared()
      {
         return prepared;
      }
      
      public Xid getXid()
      {
         return xid;
      }
            
   }
   
   private class Action
   {
      public BDBJEDatabase database;
      
      public boolean put;
      
      public long id;
      
      public byte[] bytes;
      
      public int offset;
      
      public int length;
      
      public Action(BDBJEDatabase database, boolean put, long id, byte[] bytes, int offset, int length)
      {
         this.database = database;
         
         this.put = put;
         
         this.id = id;
         
         this.bytes = bytes;
         
         this.offset = offset;
         
         this.length = length;
      }
   }
   
   private class FakeBDBJEDatabase implements BDBJEDatabase
   {     
      private Map<Long, byte[]> store = new LinkedHashMap<Long, byte[]>();
      
      private AtomicInteger cursorCount = new AtomicInteger(0);
      
      public void close() throws Exception
      {
         if (cursorCount.get() != 0)
         {
            throw new IllegalStateException("Cannot close. There are cursors open");
         }
      }

      public BDBJECursor cursor() throws Exception
      {
         return new FakeBDBJECursor();
      }

      private BDBJETransaction getTx(BDBJETransaction tx)
      {
         if (tx == null)
         {            
            tx = implicitTXs.get(Thread.currentThread());
         }
         return tx;
      }
      
      public void put(BDBJETransaction tx, long id, byte[] bytes, int offset,
            int length) throws Exception
      {
         tx = getTx(tx);
         
         if (tx == null)
         {
            if (offset == 0 && bytes.length == length)
            {               
               store.put(id, bytes);
            }
            else
            {
               byte[] currentBytes = store.get(id);
               
               if (offset == currentBytes.length)
               {
                  byte[] newBytes = new byte[currentBytes.length + bytes.length];
                  
                  ByteBuffer buffer = ByteBuffer.wrap(newBytes);
                  
                  buffer.put(currentBytes);
                  
                  buffer.put(bytes);
                  
                  store.put(id, newBytes);
               }
               else if (offset < currentBytes.length)
               {                 
                  if (offset + length > currentBytes.length)
                  {
                     throw new IllegalStateException("Invalid offset/length");
                  }
                  
                  byte[] newBytes = new byte[currentBytes.length - (length - bytes.length)];
                  
                  System.arraycopy(currentBytes, 0, newBytes, 0, offset);
                  
                  System.arraycopy(bytes, 0, newBytes, offset, bytes.length);
                                 
                  System.arraycopy(currentBytes, offset + length, newBytes,
                                   offset + bytes.length, currentBytes.length - (offset + length));
                  
                  store.put(id, newBytes);
               }
               else
               {
                  throw new IllegalStateException("Invalid offset " + offset);
               }
            }            
         }
         else
         {
            FakeBDBJETransaction ftx = (FakeBDBJETransaction)tx;
            
            ftx.actions.add(new Action(this, true, id, bytes, offset, length));
         }
         
      }

      public void remove(BDBJETransaction tx, long id) throws Exception
      {
         tx = getTx(tx);
         
         if (tx == null)
         {
            store.remove(id);
         }
         else
         {
            FakeBDBJETransaction ftx = (FakeBDBJETransaction)tx;
            
            ftx.actions.add(new Action(this, false, id, null, -1, -1));
         }
      }
      
      public byte[] get(long id) throws Exception
      {
         return store.get(id);
      }

      public long size() throws Exception
      {
         return store.size();
      }
      
      private class FakeBDBJECursor implements BDBJECursor
      {
         private Iterator<Map.Entry<Long,byte[]>> iterator = store.entrySet().iterator();
         
         FakeBDBJECursor()
         {
            cursorCount.incrementAndGet();
         }

         public void close() throws Exception
         {         
            cursorCount.decrementAndGet();
         }

         public Pair<Long, byte[]> getNext() throws Exception
         {
            if (iterator.hasNext())
            {
               Map.Entry<Long,byte[]> entry = iterator.next();
               
               return new Pair<Long, byte[]>(entry.getKey(), entry.getValue());
            }
            else
            {
               return null;
            }
         }      
      }
   }
}
