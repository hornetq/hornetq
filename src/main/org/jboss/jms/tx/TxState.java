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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Holds information for a JMS transaction to be sent to the server for
 * processing.
 * Holds the messages to be sent and the acknowledgements to be made
 * for the transaction
 * 
 * @author <a href="mailto:tim.l.fox@gmail.com>Tim Fox </a>
 */
public class TxState implements Externalizable
{  
   // Constants -----------------------------------------------------
   private static final long serialVersionUID = -7255482761072658186L;
   
   public final static byte TX_OPEN = 0;
   public final static byte TX_ENDED = 1;
   public final static byte TX_PREPARED = 3;
   public final static byte TX_COMMITED = 4;
   public final static byte TX_ROLLEDBACK = 5;
   
   // Attributes ----------------------------------------------------
   
   //private Long id;
   public int state = TX_OPEN;
   public List messages = new ArrayList();
   public List acks = new ArrayList();

   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   public TxState()
   {
      
   }

   // Public --------------------------------------------------------
   
   // Externalizable implementation ---------------------------------

   public void writeExternal(ObjectOutput out) throws IOException
   {
     out.writeInt(state);
     writeList(messages, out);
     writeList(acks, out);
   }

   public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
   {
     state = in.readInt();
     messages = readList(in);
     acks = readList(in);
   }
   
   // Class YYY overrides -------------------------------------------

   // Protected -----------------------------------------------------

   // Package Private -----------------------------------------------

   // Private -------------------------------------------------------
   
   private void writeList(List l, ObjectOutput out) throws IOException
   {
      out.writeInt(l.size());
      Iterator iter = l.iterator();
      while (iter.hasNext())
      {
         out.writeObject(iter.next());
      }
   }
   
   private List readList(ObjectInput in) throws IOException, ClassNotFoundException
   {
      int size = in.readInt();
      
      List l = new ArrayList(size);
      
      for (int i = 0; i < size; i++)
      {
         l.add(in.readObject());
      }
      
      return l;
      
   }
   
   // Inner Classes -------------------------------------------------
	

}
