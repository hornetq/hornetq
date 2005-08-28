/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.tools.tx;

import java.io.IOException;
import java.io.Externalizable;
import javax.transaction.xa.Xid;

/**
 * This object encapsulates the global transaction ID of a transaction. It is similar to an Xid,
 * but holds only the GlobalId part. This implementation is immutable and always serializable at
 * runtime.
 *
 *  @see org.jboss.tm.XidImpl
 *  @author <a href="mailto:osh@sparre.dk">Ole Husgaard</a>
 *  @author <a href="mailto:reverbel@ime.usp.br">Francisco Reverbel</a>
 *  @version $Revision$
 */
class GlobalId implements Externalizable
{
   private static String thisClassName;
   
   static
   {
      thisClassName = GlobalId.class.getName();
      thisClassName = thisClassName.substring(thisClassName.lastIndexOf('.') + 1);
   }
   
   // Format id of this instance.
	private int formatId;

   /**
    * Global transaction id of this instance. The coding of this class depends on the fact that
    * this variable is initialized in the constructor and never modified. References to this array
    * are never given away, instead a clone is delivered.
    */
   private byte[] globalId;

   /**
    * Hash code of this instance. For a native GlobalId (one whose formatId is
    * XidImpl.JBOSS_FORMAT_ID), this is really a sequence number.
    */
   private int hash;

   // Constructors --------------------------------------------------

   public GlobalId()
   {
      // Used for Externalizable support
   }
   
   /**
    * Create a new instance. This constructor is package-private, as it trusts the hash parameter
    * to be good.
    */
   GlobalId(int formatId, byte[] globalId, int hash)
   {
      this.formatId = formatId;
      this.globalId = globalId;
      this.hash = hash;
   }

   /**
    * Create a new instance. This constructor is public <em>only</em> to get around a class loader
    * problem; it should be package-private.
    */
   public GlobalId(int formatId, byte[] globalId)
   {
      this.formatId = formatId;
      this.globalId = globalId;
      hash = computeHash();
   }

   public GlobalId(Xid xid)
   {
      formatId = xid.getFormatId();
      globalId = xid.getGlobalTransactionId();
      if (xid instanceof XidImpl && formatId == XidImpl.JBOSS_FORMAT_ID)
      {
         // native GlobalId: use its hash code (a sequence number)
         hash = xid.hashCode();
      }
      else 
      {
         // foreign GlobalId: do the hash computation
         hash = computeHash();
      }
   }

   public GlobalId(int formatId, int bqual_length, byte[] tid)
   {
      this.formatId = formatId;
      if (bqual_length == 0)
         globalId = tid;
      else 
      {
         int len = tid.length - bqual_length;
         globalId = new byte[len];
         System.arraycopy(tid, 0, globalId, 0, len);
      }
      hash = computeHash();
   }

   // Public --------------------------------------------------------

   /**
    * Returns the global transaction id of this transaction.
    */
   public byte[] getGlobalTransactionId()
   {
      return (byte[])globalId.clone();
   }

   /**
    * Returns the format identifier of this transaction.
    */
   public int getFormatId() 
   {
      return formatId;
   }

   /**
    * Compare for equality.
    *
    * Instances are considered equal if they both refer to the same global transaction id.
    */
   public boolean equals(Object obj)
   {
      if (obj instanceof GlobalId) {
         GlobalId other = (GlobalId)obj;

         if (formatId != other.formatId)
            return false;

         if (globalId == other.globalId)
            return true;

         if (globalId.length != other.globalId.length)
            return false;

         int len = globalId.length;
         for (int i = 0; i < len; ++i)
            if (globalId[i] != other.globalId[i])
               return false;

         return true;
      }
      return false;
   }

   public int hashCode()
   {
      return hash;
   }
   
   public String toString() 
   {
      return thisClassName + "[formatId=" + formatId
            + ", globalId=" + new String(globalId).trim()
            + ", hash=" + hash + "]";
   }
   
   // Externalizable implementation ---------------------------------

   public void writeExternal(java.io.ObjectOutput out)
      throws IOException
   {
      out.writeInt(formatId);
      out.writeObject(globalId);
   }
   
   public void readExternal(java.io.ObjectInput in)
      throws IOException, ClassNotFoundException
   {
      formatId = in.readInt();
      globalId = (byte[])in.readObject();
      hash = computeHash();
   }

   // Private -------------------------------------------------------

   private int computeHash()
   {
      if (formatId == XidImpl.JBOSS_FORMAT_ID)
      {
         return (int)TransactionImpl.xidFactory.extractLocalIdFrom(globalId);
      }
      else
      {
         int len = globalId.length;
         int hashval = 0;
         
         for (int i = 0; i < len; ++i)
         {
            hashval = 3 * globalId[i] + hashval;
         }
         hashval += formatId;
         return hashval;
      }
   }

}
