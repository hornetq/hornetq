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
package org.jboss.jms.client.remoting;

import java.io.DataInputStream;
import java.io.DataOutputStream;

import org.jboss.messaging.util.Streamable;

/**
 * A HandleMessageResponse
 * 
 * This is the response the server gets after delivering messages to a client consumer

 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public class HandleMessageResponse implements Streamable
{
   private static final long serialVersionUID = 2500443290413453569L;

   private boolean full;
   
   private int messagesAccepted;
   
   public HandleMessageResponse()
   {      
   }
   
   public HandleMessageResponse(boolean full, int messagesAccepted)
   {
      this.full = full;
      
      this.messagesAccepted = messagesAccepted;
   }
   
   public boolean clientIsFull()
   {
      return full;
   }
   
   public int getNumberAccepted()
   {
      return messagesAccepted;
   }
   
   
   // Streamable implementation
   // ---------------------------------------------------------------
   
   public void write(DataOutputStream out) throws Exception
   {
      out.writeBoolean(full);
      
      out.writeInt(messagesAccepted);
   }

   public void read(DataInputStream in) throws Exception
   {
      full = in.readBoolean();
      
      messagesAccepted = in.readInt();
   }
}
