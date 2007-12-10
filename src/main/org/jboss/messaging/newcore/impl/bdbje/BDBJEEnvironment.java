package org.jboss.messaging.newcore.impl.bdbje;

import java.util.List;

import javax.transaction.xa.Xid;

/**
 * 
 * A BDBJEEnvironment
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public interface BDBJEEnvironment
{
   void start() throws Exception;
   
   void stop() throws Exception;
   
   BDBJETransaction createTransaction() throws Exception;
   
   BDBJEDatabase getDatabase(String databaseName) throws Exception;
   
   void setEnvironmentPath(String environmentPath);
   
   String getEnvironmentPath();
      
   void setTransacted(boolean transacted);
   
   boolean isTransacted();
   
   void setSyncVM(boolean sync);
   
   boolean isSyncVM();
   
   void setSyncOS(boolean sync);
   
   boolean isSyncOS();
   
   void setMemoryCacheSize(long size);
   
   long getMemoryCacheSize();
   
   void startWork(Xid xid) throws Exception;
   
   void endWork(Xid xid, boolean fail) throws Exception;
   
   void prepare(Xid xid) throws Exception;
   
   void commit(Xid xid) throws Exception;
   
   void rollback(Xid xid) throws Exception;
   
   List<Xid> getInDoubtXids() throws Exception;
}
