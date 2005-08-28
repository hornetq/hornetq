/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.tools.tx;

import javax.transaction.xa.Xid;
import java.io.Serializable;

/**
 *  This object encapsulates the ID of a transaction. This implementation is immutable and always
 * serializable at runtime.
 */
class XidImpl implements Xid, Serializable
{
   // Constants -----------------------------------------------------

   public static final int JBOSS_FORMAT_ID = 0x0101;

   // Static variable -----------------------------------------------

   private static boolean trulyGlobalIdsEnabled = false;

   // Attributes ----------------------------------------------------

   /**
    *  Format id of this instance. 
    *  A JBoss-generated Xids has JBOSS_FORMAT_ID in this field.
    */
   private final int formatId;

   /**
    *  Global transaction id of this instance.
    *  The coding of this class depends on the fact that this variable is
    *  initialized in the constructor and never modified. References to
    *  this array are never given away, instead a clone is delivered.
    */
   private final byte[] globalId;

   /**
    *  Branch qualifier of this instance.
    *  This identifies the branch of a transaction.
    */
   private final byte[] branchId;

   /**
    *  Hash code of this instance. This is really a sequence number.
    */
   private final int hash;

   /**
    *  Local id of this instance. This field uniquely identifies a
    *  transaction within a given JBoss server.
    */
   private final long localId;

   /**
    *  Global id of this instance. This field uniquely identifies a
    *  transaction in a distributed environment.
    */
   private final GlobalId trulyGlobalId;


   // Static --------------------------------------------------------

   /**
    *  Setter for class variable trulyGlobalIdsEnabled.
    */
   public static void setTrulyGlobalIdsEnabled(boolean newValue) 
   {
      trulyGlobalIdsEnabled = newValue;
   }

   /**
    *  Getter for class variable trulyGlobalIdsEnabled.
    */
   public static boolean getTrulyGlobalIdsEnabled() 
   {
      return trulyGlobalIdsEnabled;
   }

   /**
    *  Return a string that describes any Xid instance.
    */
   static String toString(Xid id) 
   {
      if (id == null)
         return "[NULL Xid]";

      String s = id.getClass().getName();
      s = s.substring(s.lastIndexOf('.') + 1);
      s = s + "[FormatId=" + id.getFormatId()
            + ", GlobalId=" + new String(id.getGlobalTransactionId()).trim()
            + ", BranchQual=" + new String(id.getBranchQualifier()).trim()
            + ((id instanceof XidImpl) ? ", localId=" + ((XidImpl)id).localId 
                                       : "") 
            + "]";

      return s;
   }

   // Constructors --------------------------------------------------

   /**
    *  Create a new instance.
    */
   public XidImpl(int formatId, 
                  byte[] globalId, byte[] branchId, int hash, long localId)
   {
      this.formatId = formatId;
      this.globalId = globalId;
      this.branchId = branchId;
      this.hash = hash;
      this.localId = localId;
      this.trulyGlobalId = (trulyGlobalIdsEnabled) 
                           ? new GlobalId(formatId, globalId) 
                           : null;
   }

   /**
    *  Create a new instance with JBOSS_FORMAT_ID.
    */
   XidImpl(byte[] globalId, byte[] branchId, int hash, long localId)
   {
      this.formatId = JBOSS_FORMAT_ID;
      this.globalId = globalId;
      this.branchId = branchId;
      this.hash = hash;
      this.localId = localId;
      this.trulyGlobalId = (trulyGlobalIdsEnabled) 
                           ? new GlobalId(JBOSS_FORMAT_ID, globalId, hash)
                           : null;
   }

   /**
    *  Create a new branch of an existing global transaction ID.
    *
    *  @param xidImpl The transaction ID to create a new branch of.
    *  @param branchId The ID of the new branch.
    *
    */
   public XidImpl(final XidImpl xidImpl, final byte[] branchId)
   {
      this.formatId = xidImpl.formatId;
      this.globalId = xidImpl.globalId; // reuse array, we never modify it
      this.branchId = branchId;
      this.hash = xidImpl.hash;
      this.localId = xidImpl.localId;
      this.trulyGlobalId = (trulyGlobalIdsEnabled) 
                           ? xidImpl.trulyGlobalId 
                           : null;
   }

   /**
    *  Create a new branch of an existing global transaction.
    *
    *  @param globalId identifies the transaction to create a new branch of
    *  @param branchId the id that distiguishes the new branch from other
    *                  branches of the same transaction
    *  @param localId  the local id of the transaction 
    *
    */
   public XidImpl(final GlobalId globalId, final byte[] branchId, long localId)
   {
      this.formatId = globalId.getFormatId();
      this.globalId = globalId.getGlobalTransactionId();
      this.branchId = branchId;
      this.hash = globalId.hashCode();
      this.localId = localId;
      this.trulyGlobalId = (trulyGlobalIdsEnabled) 
                           ? globalId 
                           : null;
   }

   // Public --------------------------------------------------------

   // Xid implementation --------------------------------------------

   /**
    *  Return the global transaction id of this transaction.
    */
   public byte[] getGlobalTransactionId()
   {
      return (byte[])globalId.clone();
   }

   /**
    *  Return the branch qualifier of this transaction.
    */
   public byte[] getBranchQualifier()
   {
      if (branchId.length == 0)
         return branchId; // Zero length arrays are immutable.
      else
         return (byte[])branchId.clone();
   }

   /**
    *  Return the format identifier of this transaction.
    *
    *  The format identifier augments the global id and specifies
    *  how the global id and branch qualifier should be interpreted.
    */
   public int getFormatId() 
   {
      // The id we return here should be different from all other transaction
      // implementations.
      // Known IDs are:
      // -1:     Sometimes used to denote a null transaction id.
      // 0:      OSI TP (javadoc states OSI CCR, but that is a bit misleading
      //         as OSI CCR doesn't even have ACID properties. But OSI CCR and
      //         OSI TP do have the same id format.)
      // 1:      Was used by early betas of jBoss.
      // 0x0101: The JBOSS_FORMAT_ID we use here.
      // 0xBB14: Used by JONAS.
      // 0xBB20: Used by JONAS.

      return formatId;
   }

   /**
    *  Compare for equality.
    *
    *  Instances are considered equal if they are both instances of XidImpl,
    *  and if they have the same format id, the same global transaction id 
    *  and the same transaction branch qualifier.
    */
   public boolean equals(Object obj)
   {
      if(obj==this) 
         return true;
      if (obj instanceof XidImpl) {
         XidImpl other = (XidImpl)obj;

         if (formatId != other.formatId ||
             globalId.length != other.globalId.length ||
             branchId.length != other.branchId.length)
            return false;

         for (int i = 0; i < globalId.length; ++i)
            if (globalId[i] != other.globalId[i])
               return false;

         for (int i = 0; i < branchId.length; ++i)
            if (branchId[i] != other.branchId[i])
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
      return toString(this);
   }

   // Methods specific to JBoss Xid implementation ------------------

   /**
    *  Return the local id that identifies this transaction 
    *  within the JBoss server.
    */
   public long getLocalIdValue() 
   {
      return localId;
   }

   /**
    *  Return a LocalId instance that identifies this transaction 
    *  within the JBoss server.
    */
   public LocalId getLocalId()
   {
      return new LocalId(localId);
   }

   /**
    *  Return a GlobalId instance that identifies this transaction 
    *  in a distributed environment.
    */
   public GlobalId getTrulyGlobalId() 
   {
      return trulyGlobalId;
   }

   /**
    *  Compare for same transaction.
    *
    *  Instances represent the same transaction if they have the same 
    *  format id and global transaction id.
    */
   public boolean sameTransaction(XidImpl other)
   {
      if(other == this) 
         return true;
      if (formatId != other.formatId ||
          globalId.length != other.globalId.length)
         return false;

      for (int i = 0; i < globalId.length; ++i)
         if (globalId[i] != other.globalId[i])
            return false;

      return true;
   }

   // Package protected ---------------------------------------------

   /**
    *  Return the global transaction id of this transaction.
    *  Unlike the {@link #getGlobalTransactionId()} method, this one
    *  returns a reference to the global id byte array that may <em>not</em>
    *  be changed.
    */
   public byte[] getInternalGlobalTransactionId()
   {
      return globalId;
   }

   
   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

