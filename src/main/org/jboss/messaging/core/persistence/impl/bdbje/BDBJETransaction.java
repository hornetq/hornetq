package org.jboss.messaging.core.persistence.impl.bdbje;

/**
 * 
 * A BDBJETransaction
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public interface BDBJETransaction
{
   public void commit() throws Exception;
   
   public void rollback() throws Exception;     
}
