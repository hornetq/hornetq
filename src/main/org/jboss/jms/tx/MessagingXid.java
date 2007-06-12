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
package org.jboss.jms.tx;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.Serializable;

import javax.transaction.xa.Xid;

import org.jboss.messaging.util.Streamable;

/**
 * 
 * Xid implementation
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:juha@jboss.org">Juha Lindfors</a>
 * 
 * @version $Revision 1.1 $
 */
public class MessagingXid implements Xid, Serializable, Streamable
{
   private static final long serialVersionUID = -1893120702576869245L;

   private byte[] branchQualifier;
   
   private int formatId;
   
   private byte[] globalTransactionId;
   
   private int hash;
   
   private boolean hashCalculated;

   public byte[] getBranchQualifier()
   {
      return branchQualifier;
   }

   public int getFormatId()
   {
      return formatId;
   }

   public byte[] getGlobalTransactionId()
   {
      return globalTransactionId;
   }
   
   //For serialization
   public MessagingXid()
   {      
   }
   
   public MessagingXid(byte[] branchQualifier, int formatId, byte[] globalTransactionId)
   {
      this.branchQualifier = branchQualifier;
      this.formatId = formatId;
      this.globalTransactionId = globalTransactionId;          
   }
   
   //Copy constructor
   public MessagingXid(Xid other)
   {
      this.branchQualifier = copyBytes(other.getBranchQualifier());
      this.formatId = other.getFormatId();
      this.globalTransactionId = copyBytes(other.getGlobalTransactionId());
   }
      
   public int hashCode()
   {
      if (!hashCalculated)
      {
         calcHash();
      }
      return hash;
   }
   
   public boolean equals(Object other)
   {
      if (!(other instanceof Xid))
      {
         return false;
      }
      Xid xother = (Xid)other;
      if (xother.getFormatId() != formatId)
      {
         return false;
      }
      if (xother.getBranchQualifier().length != this.branchQualifier.length)
      {
         return false;
      }
      if (xother.getGlobalTransactionId().length != this.globalTransactionId.length)
      {
         return false;
      }
      for (int i = 0; i < this.branchQualifier.length; i++)
      {
         byte[] otherBQ = xother.getBranchQualifier();
         if (this.branchQualifier[i] != otherBQ[i])
         {
            return false;
         }         
      }
      for (int i = 0; i < this.globalTransactionId.length; i++)
      {
         byte[] otherGtx = xother.getGlobalTransactionId();
         if (this.globalTransactionId[i] != otherGtx[i])
         {
            return false;
         }
      }
      return true;
   }

   public String toString()
   {
     	return "MessagingXid (" + System.identityHashCode(this) + " bq:" + stringRep(branchQualifier) +
     	" formatID:" + formatId + " gtxid:" + stringRep(globalTransactionId);
   }
   
   private String stringRep(byte[] bytes)
   {
      StringBuffer buff = new StringBuffer();
      for (int i = 0; i < bytes.length; i++)
      {
         byte b = bytes[i];
         
         buff.append(b);
         
         if (i != bytes.length - 1)
         {
            buff.append('.');
         }
      }
      
      return buff.toString();
   }

   public void read(DataInputStream in) throws Exception
   {
      int len = in.readInt();      
      branchQualifier = new byte[len];      
      in.readFully(branchQualifier);
      
      formatId = in.readInt();
      
      len = in.readInt();      
      globalTransactionId = new byte[len];      
      in.readFully(globalTransactionId);            
   }

   public void write(DataOutputStream out) throws Exception
   {
      out.writeInt(branchQualifier.length);
      out.write(branchQualifier);
      
      out.writeInt(formatId);
      
      out.writeInt(globalTransactionId.length);
      out.write(globalTransactionId);
   }
   
   private void calcHash()
   {
      byte[] hashBytes = new byte[branchQualifier.length + globalTransactionId.length + 4];
      System.arraycopy(branchQualifier, 0, hashBytes, 0, branchQualifier.length);
      System.arraycopy(globalTransactionId, 0, hashBytes, branchQualifier.length, globalTransactionId.length);
      byte[] intBytes = new byte[4];
      for (int i = 0; i < 4; i++)
      {
         intBytes[i] = (byte)((formatId >> (i * 8)) % 0xFF);
      }
      System.arraycopy(intBytes, 0, hashBytes, branchQualifier.length + globalTransactionId.length, 4);
      String s = new String(hashBytes);
      hash = s.hashCode();
      hashCalculated = true;
   }
   
   private byte[] copyBytes(byte[] other)
   {
      byte[] bytes = new byte[other.length];
      
      System.arraycopy(other, 0, bytes, 0, other.length);
      
      return bytes;
   }

}
