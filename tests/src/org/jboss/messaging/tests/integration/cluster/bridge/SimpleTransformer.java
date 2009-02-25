/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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


package org.jboss.messaging.tests.integration.cluster.bridge;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.server.cluster.Transformer;
import org.jboss.messaging.utils.SimpleString;

/**
 * A SimpleTransformer
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 21 Nov 2008 11:44:37
 *
 *
 */
public class SimpleTransformer implements Transformer
{
   private static final Logger log = Logger.getLogger(SimpleTransformer.class);
   
   public ServerMessage transform(final ServerMessage message)
   {
      SimpleString oldProp = (SimpleString)message.getProperty(new SimpleString("wibble"));
      
      if (!oldProp.equals(new SimpleString("bing")))
      {
         throw new IllegalStateException("Wrong property value!!");
      }
      
      //Change a property
      message.putStringProperty(new SimpleString("wibble"), new SimpleString("bong"));
      
      //Change the body
      MessagingBuffer buffer = message.getBody();
      
      String str = buffer.getString();
      
      if (!str.equals("doo be doo be doo be doo"))
      {
         throw new IllegalStateException("Wrong body!!");
      }
        
      buffer.flip();
      
      buffer.putString("dee be dee be dee be dee");
      
      return message;
   }

}
