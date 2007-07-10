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

import org.jboss.messaging.core.impl.message.CoreMessage;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>  
 * @version <tt>$Revision: 764 $</tt>
 * 
 * $Id: MessageFactory.java 764 2006-03-22 10:23:38Z timfox $
 */
public class CoreMessageFactory
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   public static CoreMessage createCoreMessage(long messageID)
   {
      return createCoreMessage(messageID, false, 0, 0, (byte)4, null, null);
   }

   public static CoreMessage createCoreMessage(long messageID,
                                               boolean reliable,
                                               Serializable payload)
   {
      return createCoreMessage(messageID, reliable, 0, 0, (byte)4, null, payload);
   }

   public static CoreMessage createCoreMessage(long messageID,
                                               boolean reliable,
                                               long expiration,
                                               long timestamp,
                                               byte priority,
                                               Map coreHeaders,
                                               Serializable payload)
   {
      CoreMessage cm =
         new CoreMessage(messageID, reliable, expiration, timestamp, priority, coreHeaders, null);
      cm.setPayload(payload);
      return cm;
   }
         
   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
