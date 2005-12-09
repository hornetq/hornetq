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

import java.io.Serializable;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;

import org.jboss.aop.util.PayloadKey;
import org.jboss.jms.delegate.ConsumerDelegate;
import org.jboss.jms.server.remoting.MetaDataConstants;
import org.jboss.remoting.InvokerLocator;

/**
 * 
 * The client stub class for ConsumerDelegate
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 */
public class ConsumerStub extends ClientStubBase implements ConsumerDelegate
{
   private static final long serialVersionUID = -2578195153435251519L;
   
   public ConsumerStub(String objectID, InvokerLocator locator)
   {
      super(objectID, locator);
   }
   
   public void init()
   {
      super.init();
      getMetaData()
         .addMetaData(MetaDataConstants.TAG_NAME, MetaDataConstants.CONSUMER_ID, objectID, PayloadKey.TRANSIENT);
      
   }
   
   public void activate() throws JMSException
   {
   }

   public void cancelMessage(Serializable messageID) throws JMSException
   {
   }

   public void close() throws JMSException
   {
   }

   public void closing() throws JMSException
   {
   }

   public void deactivate() throws JMSException
   {
   }

   public MessageListener getMessageListener()
   {
      return null;
   }

   public Message getMessageNow() throws JMSException
   {
      return null;
   }
   
   public Message receive(long timeout) throws JMSException
   {
      return null;
   }   

   public void setMessageListener(MessageListener listener)
   {
   }  
   
   public boolean getNoLocal()
   {
      return false;
   }
   
   public Destination getDestination()
   {
      return null;
   }
   
   public String getMessageSelector()
   {
      return null;
   }
}
