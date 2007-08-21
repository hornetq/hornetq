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

package org.jboss.jms.delegate;

import org.jboss.jms.client.delegate.ClientConnectionFactoryDelegate;
import org.jboss.jms.wireformat.PacketSupport;
import org.jboss.messaging.util.Streamable;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.io.DataOutputStream;
import java.io.DataInputStream;

/**
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @version <tt>$Revision$</tt>
 *          $Id$
 */
public class TopologyResult implements Streamable
{

   // Constants ------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------
   private ClientConnectionFactoryDelegate[] delegates;

   private Map failoverMap;

   String uniqueName;

   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------


   public TopologyResult()
   {
   }

   public TopologyResult(String uniqueName, ClientConnectionFactoryDelegate[] delegates,
                                  Map failoverMap)
   {
      this.uniqueName = uniqueName;

      this.delegates = delegates;

      this.failoverMap = failoverMap;
   }



   // Public ---------------------------------------------------------------------------------------


   public ClientConnectionFactoryDelegate[] getDelegates()
   {
      return delegates;
   }

   public void setDelegates(ClientConnectionFactoryDelegate[] delegates)
   {
      this.delegates = delegates;
   }

   public Map getFailoverMap()
   {
      return failoverMap;
   }

   public void setFailoverMap(Map failoverMap)
   {
      this.failoverMap = failoverMap;
   }

   public String getUniqueName()
   {
      return uniqueName;
   }

   public void setUniqueName(String uniqueName)
   {
      this.uniqueName = uniqueName;
   }

   public void read(DataInputStream is) throws Exception
   {
      uniqueName = is.readUTF();

      int len = is.readInt();

      delegates = new ClientConnectionFactoryDelegate[len];

      for (int i = 0; i < len; i++)
      {
         delegates[i] = new ClientConnectionFactoryDelegate();

         delegates[i].read(is);
      }

      len = is.readInt();

      failoverMap = new HashMap(len);

      for (int c = 0; c < len; c++)
      {
         Integer i = new Integer(is.readInt());

         Integer j = new Integer(is.readInt());

         failoverMap.put(i, j);
      }
   }

   public void write(DataOutputStream os) throws Exception
   {
      os.writeUTF(uniqueName);

      int len = delegates.length;

      os.writeInt(len);

      for (int i = 0; i < len; i++)
      {
         delegates[i].write(os);
      }

      os.writeInt(failoverMap.size());

      Iterator iter = failoverMap.entrySet().iterator();

      while (iter.hasNext())
      {
         Map.Entry entry = (Map.Entry)iter.next();

         Integer i = (Integer)entry.getKey();

         Integer j = (Integer)entry.getValue();

         os.writeInt(i.intValue());

         os.writeInt(j.intValue());
      }

   }


   public String toString()
   {
      StringBuffer sb = new StringBuffer("UpdateConnectionFactoryResult[");

      if (delegates != null)
      {
         for(int i = 0; i < delegates.length; i++)
         {
            sb.append(delegates[i]);
            if (i < delegates.length - 1)
            {
               sb.append(',');
            }
         }
      }

      sb.append("]");

      return sb.toString();

   }
   
   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------
}
