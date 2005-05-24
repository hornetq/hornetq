/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.delegate;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;

import org.jboss.jms.client.Closeable;
import org.jboss.jms.tx.LocalTx;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public interface SessionDelegate extends Closeable
{
   public ProducerDelegate createProducerDelegate(Destination destination) throws JMSException;

   public ConsumerDelegate createConsumerDelegate(Destination destination) throws JMSException;

   public Message createMessage() throws JMSException;
   
   public void sendTransaction(LocalTx tx) throws JMSException;
   
   public void commit() throws JMSException;
   
   public void rollback() throws JMSException;
   
   public void recover() throws JMSException;

   public BrowserDelegate createBrowserDelegate(Destination queue, String messageSelector)
         throws JMSException;
}
