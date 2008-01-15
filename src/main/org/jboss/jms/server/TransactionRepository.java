package org.jboss.jms.server;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.Transaction;


//FIXME temp class
public class TransactionRepository
{
   private Map<Xid, Transaction> map;
   
   public TransactionRepository()
   {
      map = new ConcurrentHashMap<Xid, Transaction>();
   }
   
   public void addTransaction(Xid xid, Transaction transaction)
   {
      map.put(xid, transaction);
   }
   
   public Transaction getTransaction(Xid xid)
   {
      return map.get(xid);
   }
   
   public Transaction removeTransaction(Xid xid)
   {
      return map.remove(xid);
   }
      
}
