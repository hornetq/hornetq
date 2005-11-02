/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
  * by the @authors tag. See the copyright.txt in the distribution for a
  * full listing of individual contributors.
  *
  * This is free software; you can redistribute it and/or modify it
  * under the terms of the GNU Lesser General Public License as
  * published by the Free Software Foundation; either version 2.1 of
  * the License, or (at your option) any later version.
  *
  * This software is distributed in the hope that it will be useful,
  * but WITHOUT ANY WARRANTY; without even the implied warranty of
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
  * Lesser General Public License for more details.
  *
  * You should have received a copy of the GNU Lesser General Public
  * License along with this software; if not, write to the Free
  * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
  * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
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
   
   void stopDelivering();
   
   void cancelMessage(Serializable messageID) throws JMSException;
   
   Message getMessage() throws JMSException;
}
