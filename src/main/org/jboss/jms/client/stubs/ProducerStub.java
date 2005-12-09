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
package org.jboss.jms.client.stubs;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;

import org.jboss.jms.delegate.ProducerDelegate;
import org.jboss.remoting.InvokerLocator;

/**
 * 
 * The client stub class for ProducerDelegate
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 */
public class ProducerStub extends ClientStubBase implements ProducerDelegate
{
   private static final long serialVersionUID = -6976930316308905681L;
   
   public ProducerStub(String objectID, InvokerLocator locator)
   {
      super(objectID, locator);
   }
   
   public void close() throws JMSException
   {
   }

   public void closing() throws JMSException
   {
   }

   public int getDeliveryMode() throws JMSException
   {
      return 0;
   }

   public Destination getDestination() throws JMSException
   {
      return null;
   }

   public boolean getDisableMessageID() throws JMSException
   {
      return false;
   }

   public boolean getDisableMessageTimestamp() throws JMSException
   {
      return false;
   }

   public int getPriority() throws JMSException
   {
      return 0;
   }

   public long getTimeToLive() throws JMSException
   {
      return 0;
   }

   public void send(Destination destination, Message message, int deliveryMode, int priority, long timeToLive) throws JMSException
   {      
   }

   public void setDeliveryMode(int deliveryMode) throws JMSException
   {
   }

   public void setDestination(Destination dest)
   {
   }

   public void setDisableMessageID(boolean value) throws JMSException
   {
   }

   public void setDisableMessageTimestamp(boolean value) throws JMSException
   {
   }

   public void setPriority(int defaultPriority) throws JMSException
   {
   }

   public void setTimeToLive(long timeToLive) throws JMSException
   {
   }

}
