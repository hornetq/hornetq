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
package org.jboss.messaging.core.impl.postoffice;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.jboss.messaging.util.StreamUtils;
import org.jboss.messaging.util.Streamable;

/**
 * A SharedState
 * 
 * This encapsulates the shared state maintained across the cluster
 * This comprise the bindings, and the arbitrary replicated data
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1917 $</tt>
 *
 * $Id: SharedState.java 1917 2007-01-08 20:26:12Z clebert.suconic@jboss.com $
 *
 */
class SharedState implements Streamable
{  
   private List mappings;
   
   private Map replicatedData;
   
   private Map nodeIDAddressMap;
   
   SharedState()
   {      
   }
   
   SharedState(List mappings, Map replicatedData, Map nodeIDAddressMap)
   {
      this.mappings = mappings;
      
      this.replicatedData = replicatedData;
      
      this.nodeIDAddressMap = nodeIDAddressMap;
   }
   
   List getMappings()
   {
      return mappings;
   }
   
   Map getReplicatedData()
   {
      return replicatedData;
   }
   
   Map getNodeIDAddressMap()
   {
   	return nodeIDAddressMap;
   }

   public void read(DataInputStream in) throws Exception
   {
      int size = in.readInt();      
      
      mappings = new ArrayList(size);
      
      for (int i = 0; i < size; i++)
      {
         MappingInfo mapping = new MappingInfo();
         
         mapping.read(in);
         
         mappings.add(mapping);
      }
      
      size = in.readInt();
      
      replicatedData = new HashMap(size);
      
      for (int i = 0; i < size; i++)
      {
         Serializable key = (Serializable)StreamUtils.readObject(in, true);
         
         HashMap replicants = StreamUtils.readMap(in, false);
         
         replicatedData.put(key, replicants);
      }
      
      size = in.readInt();
      
      nodeIDAddressMap = new HashMap(size);
      
      for (int i = 0; i < size; i++)
      {
      	int nodeID = in.readInt();
      	
      	PostOfficeAddressInfo info = new PostOfficeAddressInfo();
      	
      	info.read(in);
      	
      	nodeIDAddressMap.put(new Integer(nodeID), info);
      }
   }

   public void write(DataOutputStream out) throws Exception
   {
      out.writeInt(mappings.size());
      
      Iterator iter = mappings.iterator();
      
      while (iter.hasNext())
      {
         MappingInfo mapping = (MappingInfo)iter.next();
         
         mapping.write(out);
      }
      
      out.writeInt(replicatedData.size());
      
      iter = replicatedData.entrySet().iterator();
      
      while (iter.hasNext())
      {
         Map.Entry entry = (Map.Entry)iter.next();
         
         Serializable key = (Serializable)entry.getKey();         
         
         StreamUtils.writeObject(out, key, true, true);
         
         Map replicants = (Map)entry.getValue();
         
         StreamUtils.writeMap(out, replicants, false);
      }
      
      out.writeInt(nodeIDAddressMap.size());
      
      iter = nodeIDAddressMap.entrySet().iterator();
      
      while (iter.hasNext())
      {
      	Map.Entry entry = (Map.Entry)iter.next();
      	
      	Integer nodeID = (Integer)entry.getKey();
      	
      	PostOfficeAddressInfo info = (PostOfficeAddressInfo)entry.getValue();
      	
      	out.writeInt(nodeID.intValue());
      	
      	info.write(out);
      }
   }
}
