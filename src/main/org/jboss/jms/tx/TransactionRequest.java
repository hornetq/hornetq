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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import javax.transaction.xa.Xid;

/**
 * This class contians all the data needed to perform a JMS transaction
 * 
 * @author Hiram Chirino (Cojonudo14@hotmail.com)
 * @author David Maplesden (David.Maplesden@orion.co.nz)
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author Daniel Bloomfield Ramagem (daniel.ramagem@gmail.com) 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision$
 */
public class TransactionRequest implements Externalizable
{
   // Constants -----------------------------------------------------
   
   /** The serialVersionUID */
   private static final long serialVersionUID = -5371388526898322431L;
   
   /** One phase Commit request */
   public final static byte ONE_PHASE_COMMIT_REQUEST = 0;
   /** Two phase Prepare phase */
   public final static byte TWO_PHASE_COMMIT_PREPARE_REQUEST = 1;
   /** Two phase Commit phase */
   public final static byte TWO_PHASE_COMMIT_COMMIT_REQUEST = 2;
   /** Rollback request */
   public final static byte TWO_PHASE_COMMIT_ROLLBACK_REQUEST = 3;
   
   // Attributes ----------------------------------------------------
   
   /** Request type */
   public int requestType = ONE_PHASE_COMMIT_REQUEST;

   /** For 2 phase commit, this identifies the transaction. */
   public Xid xid;

   public TxState txInfo;
   
   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   public TransactionRequest()
   {      
   }
   
   // Public --------------------------------------------------------
   
   // Externalizable implementation ---------------------------------


   public void writeExternal(ObjectOutput out) throws IOException
   {
      out.writeInt(requestType);
      out.writeObject(xid);
      out.writeObject(txInfo);
   }

   public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
   {
     requestType = in.readInt();
     xid = (Xid)in.readObject();
     txInfo = (TxState)in.readObject();
   }
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
}