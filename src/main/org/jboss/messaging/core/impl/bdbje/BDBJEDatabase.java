package org.jboss.messaging.core.impl.bdbje;


/**
 * 
 * A BDBJEDatabase
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public interface BDBJEDatabase
{
   void put(BDBJETransaction tx, long id, byte[] bytes, int offset, int length) throws Exception;
   
   void remove(BDBJETransaction tx, long id) throws Exception;
   
   void close() throws Exception;
   
   BDBJECursor cursor() throws Exception;   
   
   //Only used for testing
   
   long size() throws Exception;
   
   byte[] get(long id) throws Exception;
}
