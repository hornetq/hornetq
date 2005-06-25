/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.delegate;

import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;

import org.jboss.jms.client.Closeable;
import org.jboss.jms.tx.TxInfo;
import org.jboss.jms.MetaDataRepository;

import java.io.Serializable;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.l.fox@gmail.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public interface ConnectionDelegate extends Closeable, MetaDataRepository
{
   public SessionDelegate createSessionDelegate(boolean transacted, int acknowledgmentMode)
          throws JMSException;

   public String getClientID() throws JMSException;
   public void setClientID(String id) throws JMSException;

   public void start() throws JMSException;
   public void stop() throws JMSException;   
   
   public ExceptionListener getExceptionListener() throws JMSException;
   public void setExceptionListener(ExceptionListener listener) throws JMSException;
  
	public void sendTransaction(TxInfo tx) throws JMSException;

   public void addTemporaryDestination(Destination destination) throws JMSException;
   
   public void deleteTemporaryDestination(Destination destination) throws JMSException;
   
   /**
    * Unsubscribe the client represented by this connection from the durable subscription
    * specified by subscriptionName
    * 
    * @param subscriptionName the Name of the durable subscription to unsubscribe from
    * @throws JMSException if the unsubscribe fails
    */
   public void unsubscribe(String subscriptionName) throws JMSException;

   public Serializable getConnectionID();
}
