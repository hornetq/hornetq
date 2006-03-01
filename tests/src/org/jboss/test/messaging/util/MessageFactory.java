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
package org.jboss.test.messaging.util;

import java.io.Serializable;
import java.util.Map;

import org.jboss.test.messaging.core.CoreMessage;

/**
 * 
 * A MessageFactory.
 * 
 * Used in core tests to create messages
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version 1.1
 *
 * MessageFactory.java,v 1.1 2006/02/26 16:02:09 timfox Exp
 */
public class MessageFactory
{
   /*
    * FIXME - Only used by tests - remote it to a test specific factory
    */
   public static CoreMessage createCoreMessage(Serializable messageID,
         boolean reliable,
         long expiration,
         long timestamp,
         byte priority,
         Map coreHeaders,
         Serializable payload)
   {
      CoreMessage cm =  new CoreMessage(messageID, reliable, expiration,
                                        timestamp, priority, coreHeaders, null);
      
      cm.setPayload(payload);
      
      return cm;
   }
   
   public static CoreMessage createCoreMessage(Serializable messageID)
   {
      return new CoreMessage(messageID, false, 0, 0, (byte)4, null, null);
   }
   
   public static CoreMessage createCoreMessage(Serializable messageID, boolean reliable,
                                               Serializable payload)
   {
      CoreMessage cm =  new CoreMessage(messageID, reliable, 0, 0, (byte)4, null, null);
      
      cm.setPayload(payload);
      
      return cm;
   }
   
}
