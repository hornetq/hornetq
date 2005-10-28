/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.delegate;

import java.io.Serializable;

import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.JMSException;
import javax.jms.MessageListener;

import org.jboss.jms.client.Closeable;
import org.jboss.jms.MetaDataRepository;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public interface ConsumerDelegate extends Closeable, MetaDataRepository
{

   MessageListener getMessageListener() throws JMSException;
   void setMessageListener(MessageListener listener) throws JMSException;

   /**
    * @param timeout - a 0 timeout means wait forever and a negative value timeout means 
    *        "receiveNoWait".
    * @return
    * @throws JMSException
    */
   Message receive(long timeout) throws JMSException;
   
   Destination getDestination() throws JMSException;
   
   boolean getNoLocal() throws JMSException;
   
   String getMessageSelector() throws JMSException;
   
   String getReceiverID();
   
   void setDestination(Destination dest);
   
   void setNoLocal(boolean noLocal);
   
   void setMessageSelector(String selector);
   
   void setReceiverID(String receiverID);
   
   Message getMessage();
   
   //void readyForMessage(boolean ready);
   
   void stopDelivering();
   
   void cancelMessage(Serializable messageID) throws JMSException;
}
