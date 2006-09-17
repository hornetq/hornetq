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
package org.jboss.messaging.core.plugin.postoffice.cluster;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.jboss.messaging.util.StreamUtils;
import org.jboss.messaging.util.Streamable;

/**
 * A SharedState
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
class SharedState implements Streamable
{  
   private List bindings;
   
   private Map nodeIdAddressMap;
   
   SharedState()
   {      
   }
   
   SharedState(List bindings, Map nodeIdAddressMap)
   {
      this.bindings = bindings;
      
      this.nodeIdAddressMap = nodeIdAddressMap;
   }
   
   List getBindings()
   {
      return bindings;
   }
   
   Map getNodeIdAddressMap()
   {
      return nodeIdAddressMap;
   }

   public void read(DataInputStream in) throws Exception
   {
      int size = in.readInt();      
      bindings = new ArrayList(size);
      for (int i = 0; i < size; i++)
      {
         BindingInfo bb = new BindingInfo();
         bb.read(in);
         bindings.add(bb);
      }
      
      nodeIdAddressMap = (Map)StreamUtils.readObject(in, false);
   }

   public void write(DataOutputStream out) throws Exception
   {
      out.writeInt(bindings.size());
      Iterator iter = bindings.iterator();
      while (iter.hasNext())
      {
         BindingInfo info = (BindingInfo)iter.next();
         
         info.write(out);
      }
      
      StreamUtils.writeObject(out, nodeIdAddressMap, true, false);     
   }
}
