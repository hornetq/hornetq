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

import javax.transaction.xa.Xid;

import org.jboss.messaging.util.Streamable;

/**
 * This class contains all the data needed to perform a JMS transaction.
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Partially based on JBossMQ version by:
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:Cojonudo14@hotmail.com">Hiram Chirino</a>
 * @author <a href="mailto:David.Maplesden@orion.co.nz">David Maplesden</a>
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:daniel.ramagem@gmail.com">Daniel Bloomfield Ramagem</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version $Revision$
 *
 * $Id$
 */
public class TransactionRequest implements Streamable
{
   // Constants -----------------------------------------------------
   
   public final static byte ONE_PHASE_COMMIT_REQUEST = 0;
   public final static byte TWO_PHASE_PREPARE_REQUEST = 2;
   public final static byte TWO_PHASE_COMMIT_REQUEST = 3;
   public final static byte TWO_PHASE_ROLLBACK_REQUEST = 4;
   
   private static final byte PRESENT = 1;
   
   private static final byte NULL = 0;
   
   // Attributes ----------------------------------------------------
   
   protected int requestType;

   /** For 2 phase commit, this identifies the transaction. */
   protected Xid xid;

   protected ClientTransaction state;
   
   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   public TransactionRequest()
   {      
   }
   
   public TransactionRequest(int requestType, Xid xid, ClientTransaction state)
   {      
      this.requestType = requestType;
      this.xid = xid;
      this.state = state;
   }
   
   // Streamable implementation ---------------------------------

   public void write(DataOutputStream out) throws Exception
   {
      out.writeInt(requestType);
            
      if (xid == null)
      {
         out.writeByte(NULL);
      }
      else
      {
         //Write XId info
         byte[] branchQual = xid.getBranchQualifier();
         int formatId = xid.getFormatId();
         byte[] globalTxId = xid.getGlobalTransactionId();
                  
         out.write(PRESENT);
         out.writeInt(branchQual.length);
         out.write(branchQual);
         out.writeInt(formatId);
         out.writeInt(globalTxId.length);
         out.write(globalTxId);
      }
      
      if (state != null)
      {
         out.write(PRESENT);      
         state.write(out);
      }
      else
      {
         out.write(NULL);
      }
   }

   public void read(DataInputStream in) throws Exception
   {
     requestType = in.readInt();
     
     byte isXid = in.readByte();
     
     if (isXid == NULL)
     {
        xid = null;
     }
     else if (isXid == PRESENT)
     {
        int l = in.readInt();
        byte[] branchQual = new byte[l];
        in.readFully(branchQual);
        int formatId = in.readInt();
        l = in.readInt();
        byte[] globalTxId = new byte[l];
        in.readFully(globalTxId);
        xid = new MessagingXid(branchQual, formatId, globalTxId);
     }
     else
     {
        throw new IllegalStateException("Invalid value:" + isXid);
     }
     
     byte isState = in.readByte();
     
     if (isState == NULL)
     {
        state = null;
     }
     else
     {
        state = new ClientTransaction();
     
        state.read(in);
     }
   }

   // Public --------------------------------------------------------

   public ClientTransaction getState()
   {
      return state;
   }

   public Xid getXid()
   {
      return xid;
   }

   public int getRequestType()
   {
      return requestType;
   }

   public String toString()
   {
      return "TransactionRequest[" +
         (requestType == ONE_PHASE_COMMIT_REQUEST ? "ONE_PHASE_COMMIT":
               (requestType == TWO_PHASE_PREPARE_REQUEST ? "TWO_PHASE_PREPARE":
                  (requestType == TWO_PHASE_COMMIT_REQUEST ? "TWO_PHASE_COMMIT":
                     (requestType == TWO_PHASE_ROLLBACK_REQUEST ? "TWO_PHASE_ROLLBACK_":
                        "UNKNOW_REQUEST_TYPE")))) + ", " + xid + "]";
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
}