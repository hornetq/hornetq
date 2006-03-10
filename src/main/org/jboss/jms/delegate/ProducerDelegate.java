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

import javax.jms.JMSException;
import javax.jms.Message;

import org.jboss.jms.destination.JBossDestination;
import org.jboss.jms.server.endpoint.ProducerEndpoint;

/**
 * Represents the minimal set of operations to provide producer functionality.
 * Some of the methods may be implemented on the server, others will be handled in the advice stack.
 * 
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 */
public interface ProducerDelegate extends ProducerEndpoint
{
   void setDisableMessageID(boolean value) throws JMSException;
   
   boolean getDisableMessageID() throws JMSException;
   
   void setDisableMessageTimestamp(boolean value) throws JMSException;
   
   boolean getDisableMessageTimestamp() throws JMSException;
   
   void setDeliveryMode(int deliveryMode) throws JMSException;
   
   int getDeliveryMode() throws JMSException;
   
   void setPriority(int defaultPriority) throws JMSException;
   
   int getPriority() throws JMSException;
   
   void setTimeToLive(long timeToLive) throws JMSException;
   
   long getTimeToLive() throws JMSException;
   
   JBossDestination getDestination() throws JMSException;
   
   void setDestination(JBossDestination dest);

   /**
    * This method is only handled by the advice stack, the corresponding invocation is never sent
    * to the server.
    */
   void send(JBossDestination destination,
             Message message,
             int deliveryMode,
             int priority,
             long timeToLive) throws JMSException;
   
}
