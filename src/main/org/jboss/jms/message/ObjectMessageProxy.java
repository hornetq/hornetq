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

import java.io.Serializable;

import javax.jms.JMSException;
import javax.jms.MessageNotWriteableException;
import javax.jms.ObjectMessage;

/**
 * 
 * Thin proxy for a JBossObjectMessage
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 * ObjectMessageProxy.java,v 1.1 2006/03/08 08:00:34 timfox Exp
 */
public class ObjectMessageProxy extends MessageProxy implements ObjectMessage
{
   private static final long serialVersionUID = 8797295997477962825L;

   public ObjectMessageProxy(JBossObjectMessage message, int deliveryCount)
   {
      super(message, deliveryCount);
   }

   public void setObject(Serializable object) throws JMSException
   {
      if (bodyReadOnly)
      {
         throw new MessageNotWriteableException("setObject");
      }
      bodyChange();
      ((ObjectMessage)message).setObject(object);
   }

   public Serializable getObject() throws JMSException
   {
      return ((ObjectMessage)message).getObject();
   }
}
