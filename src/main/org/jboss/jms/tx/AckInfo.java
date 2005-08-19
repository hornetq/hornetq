package org.jboss.jms.tx;

/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Struct like class for holding information regarding an acknowlegement to
 * be passed to the server for processing.
 * 
 * @author <a href="mailto:tim.l.fox@gmail.com>Tim Fox </a>
 *
 * $Id$
 */
public class AckInfo implements Externalizable
{
   // Constants -----------------------------------------------------
   
   private static final long serialVersionUID = -5951156790257302184L;
   
   // Attributes ----------------------------------------------------
   
   public String messageID;
   public String receiverID;

   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   public AckInfo()
   {      
   }
   
   public AckInfo(String messageID, String receiverID)
   {
      this.messageID = messageID;
      this.receiverID = receiverID;    
   }

   // Public --------------------------------------------------------

   // Externalizable implementation ---------------------------------

   public void writeExternal(ObjectOutput out) throws IOException
   {
     out.writeUTF(messageID);
     out.writeUTF(receiverID);
   }

   public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
   {
      messageID = in.readUTF();
      receiverID = in.readUTF();
   }
   
   // Class YYY overrides -------------------------------------------

   // Protected -----------------------------------------------------

   // Package Private -----------------------------------------------

   // Private -------------------------------------------------------
   
   // Inner Classes -------------------------------------------------
   
}
