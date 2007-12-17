package org.jboss.messaging.newcore.impl;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.Serializable;

import org.jboss.messaging.newcore.Destination;

/**
 * 
 * A DestinationImpl
 * 
 * TODO remove serializable once SendPacket has destination and scheduled delivery time
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class DestinationImpl implements Destination, Serializable
{
   private String type;
   
   private String name;
   
   private boolean temporary;
   
   public DestinationImpl()
   {      
   }
   
   public DestinationImpl(String type, String name, boolean temporary)
   {
      this.type = type;
      
      this.name = name;
      
      this.temporary = temporary;
   }
      
   public String getType()
   {
      return type;
   }
   
   public String getName()
   {
      return name;
   }
   
   public boolean isTemporary()
   {
      return temporary;
   }
   
   public void read(DataInputStream in) throws Exception
   {
      type = in.readUTF();
      
      name = in.readUTF();
      
      temporary = in.readBoolean();
   }

   public void write(DataOutputStream out) throws Exception
   {
      out.writeUTF(type);
      
      out.writeUTF(name);
      
      out.writeBoolean(temporary);
   }

}
