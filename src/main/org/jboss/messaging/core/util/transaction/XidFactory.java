/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 *
 */
package org.jboss.messaging.core.util.transaction;

import org.jboss.logging.Logger;

import javax.transaction.xa.Xid;

import java.net.InetAddress;
import java.net.UnknownHostException;

class XidFactory
{

   private static final Logger log = Logger.getLogger(XidFactory.class);

   /**
    * The default value of baseGlobalId is the host name, followed by a colon (":"), the server's
    * JNDI port, and a slash. It the server's JNDI port cannot be obtained, then the startService
    * time is used instead.
    *
    * This is used for building globally unique transaction identifiers. It would be safer to use
    * the IP address, but a host name is better for humans to read and will do for now.
    */
   private String baseGlobalId;

   /**
    * The next transaction id to use on this host.
    */
   private long globalIdNumber = 0;

   /**
    * The variable pad says whether the byte[] should be their maximum 64 byte length or the
    * minimum. The max length is required for Oracle.
    */
   private boolean pad = false;

   /**
    * The variable noBranchQualifier is the 1 or 64 byte zero array used for initial xids.
    */
   private byte[] noBranchQualifier = new byte[1]; // len > 0, per the XA spec

   /**
    * The branchQualifier is the host name, followed by a colon (":") and the server's JNDI port.
    * It the server's JNDI port cannot be obtained, then the startService time is used instead.
    */
   private String branchQualifier;

   /**
    * This field stores the byte reprsentation of baseGlobalId
    * to avoid the expensive getBytes() call on that String object
    * when we create a new Xid, which we do VERY often!
    */
   private byte[] baseGlobalIdBytes;

   /**
    * This field stores the byte reprsentation of branchQualifier
    * to avoid the expensive getBytes() call on that String object.
    */
   private byte[] branchQualifierBytes;

   public XidFactory()
   {
      try
      {
         startService();
      }
      catch(Exception e)
      {
         log.error("Cannot start XidFactory", e);
      }
   }


   protected void startService() throws Exception
   {
      long jndiPortOrStartUpTime;
      try
      {
         Integer jndiPort = new Integer(1099);
         jndiPortOrStartUpTime = jndiPort.intValue();
      }
      catch (Exception e)
      {
         jndiPortOrStartUpTime = System.currentTimeMillis();
      }
      
      try
      {
         String hostName = InetAddress.getLocalHost().getHostName(); 
         baseGlobalId = hostName + ":" + jndiPortOrStartUpTime;
         // Ensure room for 14 digits of serial no and timestamp
         if (baseGlobalId.length() > Xid.MAXGTRIDSIZE - 15)
         {
            baseGlobalId = baseGlobalId.substring(0, Xid.MAXGTRIDSIZE - 15);
         }
         baseGlobalId = baseGlobalId + "/";
         branchQualifier = hostName + ":" + jndiPortOrStartUpTime;
      }
      catch (UnknownHostException e)
      {
         baseGlobalId = "localhost:" + jndiPortOrStartUpTime + "/";
         branchQualifier = "localhost:" + jndiPortOrStartUpTime;
      }
      baseGlobalIdBytes = baseGlobalId.getBytes();

//      int len = pad ? Xid.MAXBQUALSIZE : branchQualifier.length();
//      branchQualifierBytes = new byte[len];
//      // this method is deprecated, but does exactly what we need in a very fast way
//      // the default conversion from String.getBytes() is way too expensive
//      branchQualifier.getBytes(0, branchQualifier.length(), branchQualifierBytes, 0);

      branchQualifierBytes = branchQualifier.getBytes();
   }

   public String getBaseGlobalId()
   {
      return baseGlobalId;
   }

   public void setBaseGlobalId(final String baseGlobalId)
   {
      this.baseGlobalId = baseGlobalId;
      baseGlobalIdBytes = baseGlobalId.getBytes();
   }

   public synchronized long getGlobalIdNumber()
   {
      return globalIdNumber;
   }

   public synchronized void setGlobalIdNumber(final long globalIdNumber)
   {
      this.globalIdNumber = globalIdNumber;
   }

   public boolean isPad()
   {
      return pad;
   }

   public void setPad(boolean pad)
   {
      this.pad = pad;
      if (pad)
      {
         noBranchQualifier = new byte[Xid.MAXBQUALSIZE];
      }
      else
      {
         noBranchQualifier = new byte[1]; // length > 0, per the XA spec
      }
   }

   /**
    * Creates a XidImpl for a new transaction.
    */
   public XidImpl newXid()
   {
      long localId = getNextId();
      String id = Long.toString(localId);
      int len = pad ? Xid.MAXGTRIDSIZE : id.length() + baseGlobalIdBytes.length;
      byte[] globalId = new byte[len];
      System.arraycopy(baseGlobalIdBytes, 0, globalId, 0, baseGlobalIdBytes.length);


//      // this method is deprecated, but does exactly what we need in a very fast way
//      // the default conversion from String.getBytes() is way too expensive
//      id.getBytes(0, id.length(), globalId, baseGlobalIdBytes.length);

      byte[] idb = id.getBytes();
      System.arraycopy(idb, 0, globalId, baseGlobalIdBytes.length, idb.length);


      return new XidImpl(globalId, noBranchQualifier, (int) localId, localId);
   }

   /**
    * Creates a XidImpl for a branch of an existing transaction.
    */
   public XidImpl newBranch(GlobalId globalId)
   {
      long localId = getNextId();
      return new XidImpl(globalId, branchQualifierBytes, localId);
   }

   public XidImpl newBranch(XidImpl xid, long branchIdNum)
   {
      String id = Long.toString(branchIdNum);
//      int len = pad ? Xid.MAXBQUALSIZE : id.length();
//      byte[] branchId = new byte[len];
//      // this method is deprecated, but does exactly what we need in a very fast way
//      // the default conversion from String.getBytes() is way too expensive
//      id.getBytes(0, id.length(), branchId, 0);
      byte[] branchId = id.getBytes();
      return new XidImpl(xid, branchId);
   }

   public XidImpl fromXid(Xid xid)
   {
      if (xid instanceof XidImpl) return (XidImpl) xid;
      if (xid.getFormatId() != XidImpl.JBOSS_FORMAT_ID)
      {
         throw new RuntimeException("Format ids not equal");
      }
      byte[] globalId = xid.getGlobalTransactionId();
      byte[] branchId = xid.getBranchQualifier();
      long localId = extractLocalIdFrom(globalId);
      return new XidImpl(globalId, branchId, (int) localId, localId);
   }

   /**
    * Extracts the local id contained in a global id.
    */
   public long extractLocalIdFrom(byte[] globalId)
   {
      int i, start;
      int len = globalId.length;

      for (i = 0; globalId[i++] != (byte) '/';) {}
      start = i;
      while (i < len && globalId[i] != 0)
      {
         i++;
      }
      String globalIdNumber = new String(globalId, start, i - start);
      return Long.parseLong(globalIdNumber);
   }

   public String getBaseGlobalId(byte[] globalId)
   {
      int i, stop;
      int len = globalId.length;

      for (i = 0; globalId[i++] != (byte) '/';) {}
      stop = i;
      while (i < len && globalId[i] != 0)
      {
         i++;
      }
      String base = new String(globalId, 0, stop);
      return base;
   }


   public String toString(Xid xid)
   {
      if (xid instanceof XidImpl)
      {
         return XidImpl.toString(xid);
      }
      else
      {
         return xid.toString();
      }
   }

   private synchronized long getNextId()
   {
      return ++globalIdNumber;
   }

}
