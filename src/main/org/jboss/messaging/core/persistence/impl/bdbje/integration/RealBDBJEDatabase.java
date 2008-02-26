package org.jboss.messaging.core.persistence.impl.bdbje.integration;

import org.jboss.messaging.core.persistence.impl.bdbje.BDBJECursor;
import org.jboss.messaging.core.persistence.impl.bdbje.BDBJEDatabase;
import org.jboss.messaging.core.persistence.impl.bdbje.BDBJETransaction;
import org.jboss.messaging.util.Pair;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

/**
 * 
 * A RealBDBJEDatabase
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class RealBDBJEDatabase implements BDBJEDatabase
{  
   private Database database;
   
   RealBDBJEDatabase(Database database)
   {
      this.database = database;
   }

   // BDBJEDatabase implementation ------------------------------------------
   
   public void put(BDBJETransaction tx, long id, byte[] bytes, int offset, int length) throws Exception
   {
      DatabaseEntry key = createKey(id);

      DatabaseEntry value = new DatabaseEntry();

      if (offset != 0 || bytes.length != length)
      {
         value.setPartial(offset, length, true);
      }

      value.setData(bytes);

      Transaction bdbTx = getBDBTx(tx);

      database.put(bdbTx, key, value);
   }

   public void remove(BDBJETransaction tx, long id) throws Exception
   {
      DatabaseEntry key = createKey(id);

      Transaction bdbTx = getBDBTx(tx);

      database.delete(bdbTx, key);
   }

   public BDBJECursor cursor() throws Exception
   {
      return new RealBDBJECursor();
   }

   public void close() throws Exception
   {
      database.close();
   }
   
   // For testing
   
   public byte[] get(long id) throws Exception
   {
      DatabaseEntry key = createKey(id);
      
      DatabaseEntry data = new DatabaseEntry();
      
      if (database.get(null, key, data, LockMode.DEFAULT) != OperationStatus.SUCCESS)
      {
         return null;
      }
      else
      {
         return data.getData();
      }
   }

   public long size() throws Exception
   {
      return database.count();
   }
      
   // Private ----------------------------------------------------------------------
   
   private DatabaseEntry createKey(long id)
   {
      DatabaseEntry key = new DatabaseEntry();
      
      EntryBinding keyBinding = new LongBinding();
      
      keyBinding.objectToEntry(id, key);
      
      return key;
   }
   
   private Transaction getBDBTx(BDBJETransaction tx)
   {
      Transaction bdbTx = null;
      
      if (tx != null)
      {
         RealBDBJETransaction bdbJETx = (RealBDBJETransaction)tx;
         
         bdbTx = bdbJETx.getTransaction();
      }
      
      return bdbTx;
   }
   
   // Inner classes ---------------------------------------------------------------------
   
   private class RealBDBJECursor implements BDBJECursor
   {
      private Cursor cursor;
      
      RealBDBJECursor() throws Exception
      {
         cursor = database.openCursor(null, null);
      }
      
      public Pair<Long, byte[]> getNext() throws Exception
      {
         DatabaseEntry key = new DatabaseEntry();
       
         DatabaseEntry data = new DatabaseEntry();
 
         if (cursor.getNext(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS)
         {            
            long id = LongBinding.entryToLong(key);
            
            byte[] bytes = data.getData();
            
            Pair<Long, byte[]> pair = new Pair<Long, byte[]>(id, bytes);
            
            return pair;            
         }
         else
         {
            return null;
         }
      }
      
      public void close() throws Exception
      {
         cursor.close();
      }
   }

}
