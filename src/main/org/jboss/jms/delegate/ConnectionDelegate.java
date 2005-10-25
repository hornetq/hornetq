/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.delegate;

import java.io.Serializable;

import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.ServerSessionPool;

import org.jboss.jms.MetaDataRepository;
import org.jboss.jms.client.Closeable;
import org.jboss.jms.client.JBossConnectionConsumer;
import org.jboss.jms.tx.ResourceManager;
import org.jboss.jms.tx.TransactionRequest;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.l.fox@gmail.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public interface ConnectionDelegate extends Closeable, MetaDataRepository
{
   public SessionDelegate createSessionDelegate(boolean transacted,
                                                int acknowledgmentMode,
                                                boolean isXA)
          throws JMSException;

   public String getClientID() throws JMSException;
   public void setClientID(String id) throws JMSException;

   public void start() throws JMSException;
   public void stop() throws JMSException;   
   
   public ExceptionListener getExceptionListener() throws JMSException;
   public void setExceptionListener(ExceptionListener listener) throws JMSException;
  
	public void sendTransaction(TransactionRequest request) throws JMSException;

   public Serializable getConnectionID();
   
   public ConnectionMetaData getConnectionMetaData() throws JMSException;
   
   public JBossConnectionConsumer createConnectionConsumer(Destination dest,
                                                           String subscriptionName,
                                                           String messageSelector,
                                                           ServerSessionPool sessionPool,
                                                           int maxMessages) throws JMSException;
   
   public void setResourceManager(ResourceManager rm);
   public ResourceManager getResourceManager();
   
}
