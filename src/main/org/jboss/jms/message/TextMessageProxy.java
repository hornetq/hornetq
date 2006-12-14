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
package org.jboss.jms.message;

import javax.jms.JMSException;
import javax.jms.MessageNotWriteableException;
import javax.jms.TextMessage;

/**
 * 
 * Thin proxy for a JBossTextMessage
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 */
public class TextMessageProxy extends MessageProxy implements TextMessage
{
   private static final long serialVersionUID = -3530143417050205123L;

   public TextMessageProxy(long deliveryId, JBossTextMessage message, int deliveryCount)
   {
      super(deliveryId, message, deliveryCount);
   }
   
   public TextMessageProxy(JBossTextMessage message)
   {
      super(message);
   }
   
   public void setText(String string) throws JMSException
   {
      if (bodyReadOnly)
         throw new MessageNotWriteableException("Cannot set the content; message is read-only");
      bodyChange();
      ((TextMessage)message).setText(string);
   }

   public String getText() throws JMSException
   {
      return ((TextMessage)message).getText();
   }
}
