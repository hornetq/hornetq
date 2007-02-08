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
package org.jboss.jms.wireformat;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.jboss.logging.Logger;
import org.jboss.remoting.InvocationResponse;
import org.jboss.remoting.callback.Callback;

/**
 * A PolledCallbacksDelivery
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class PolledCallbacksDelivery extends PacketSupport
{
   private static final Logger log = Logger.getLogger(PolledCallbacksDelivery.class);
   
   
   private List callbacks;
   
   private String sessionID;
   
   public PolledCallbacksDelivery()
   {      
   }
   
   public PolledCallbacksDelivery(List callbacks, String sessionID)
   {
      super(PacketSupport.POLLEDCALLBACKS_DELIVERY);
      
      this.callbacks = callbacks;
      
      this.sessionID = sessionID;
   }

   public void read(DataInputStream is) throws Exception
   {
      sessionID = is.readUTF();
      
      int len = is.readInt();
      
      callbacks = new ArrayList(len);
      
      for (int i = 0; i < len; i++)
      {
         //Read the method id int - we just throw it away
         is.readInt();
         
         ClientDelivery delivery = new ClientDelivery();
         
         delivery.read(is);
         
         Callback cb = new Callback(delivery);
                  
         callbacks.add(cb);
      }      
   }
   
   public void write(DataOutputStream os) throws Exception
   {
      super.write(os);
      
      os.writeUTF(sessionID);

      os.writeInt(callbacks.size());

      Iterator iter = callbacks.iterator();
      
      while (iter.hasNext())
      {
         Callback cb = (Callback)iter.next();
         
         ClientDelivery cd = (ClientDelivery)cb.getParameter();
         
         cd.write(os);  
      }
      
      os.flush();
   }
   
   public Object getPayload()
   {
      return new InvocationResponse(sessionID, callbacks, false, null);
   }

}
