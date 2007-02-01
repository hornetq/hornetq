
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

import org.jboss.jms.server.endpoint.DeliveryRecovery;
import org.jboss.jms.server.endpoint.SessionEndpoint;

/**
 * 
 * A SessionRecoverDeliveriesRequest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class SessionRecoverDeliveriesRequest extends RequestSupport
{
   private List dels;
   
   public SessionRecoverDeliveriesRequest()
   {      
   }
   
   public SessionRecoverDeliveriesRequest(int objectId,
                                          byte version,
                                          List dels)
   {
      super(objectId, PacketSupport.REQ_SESSION_RECOVERDELIVERIES, version);
      
      this.dels = dels;
   }

   public void read(DataInputStream is) throws Exception
   {
      super.read(is);
      
      int size = is.readInt();
      
      dels = new ArrayList(size);
      
      for (int i = 0; i < size; i++)
      {
         DeliveryRecovery del = new DeliveryRecovery();
         
         del.read(is);
         
         dels.add(del);
      }
   }

   public ResponseSupport serverInvoke() throws Exception
   {
      SessionEndpoint endpoint = 
         (SessionEndpoint)Dispatcher.instance.getTarget(objectId);
      
      if (endpoint == null)
      {
         throw new IllegalStateException("Cannot find object in dispatcher with id " + objectId);
      }
      
      endpoint.recoverDeliveries(dels);
      
      return null;
   }

   public void write(DataOutputStream os) throws Exception
   {
      super.write(os);
      
      os.writeInt(dels.size());

      Iterator iter = dels.iterator();

      while (iter.hasNext())
      {
         DeliveryRecovery del = (DeliveryRecovery)iter.next();
         
         del.write(os);
      }
      
      os.flush();
   }

}


