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
package org.jboss.messaging.core.tx;

import javax.transaction.xa.Xid;

/**
 * 
 * Xid implementation
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * @version $Revision 1.1 $
 */
public class XidImpl implements Xid
{
   protected byte[] branchQualifier;
   
   protected int formatId;
   
   protected byte[] globalTransactionId;
   
   protected int hash;

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
   
   public XidImpl(byte[] branchQualifier, int formatId, byte[] globalTransactionId)
   {
      this.branchQualifier = branchQualifier;
      this.formatId = formatId;
      this.globalTransactionId = globalTransactionId;
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
   }
   
   public int hashCode()
   {
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

}
