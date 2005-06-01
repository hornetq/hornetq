/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.tools;

import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.Routable;

import java.io.Serializable;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class ReceiverImpl implements Receiver
{
   // Attributes ----------------------------------------------------

   private String name;
   private boolean broken = false;

   // Constructors --------------------------------------------------

   public ReceiverImpl(String name)
   {
      this.name = name;
   }

   // Receiver implementation ---------------------------------------

   public Serializable getReceiverID()
   {
      return name;
   }

   public boolean handle(Routable m)
   {
      if (broken)
      {
         System.out.println("Receiver["+name+"] broken, cannot accept message "+m);
         return false;
      }
      else
      {
         System.out.println("Receiver["+name+"] got message "+m);
         return true;
      }
   }


   // Public --------------------------------------------------------

   public String getName()
   {
      return name;
   }

   /**
    * "Breaks" a reciever (makes it return false to send()) or "repairs" it.
    */
   public void setBroken(boolean b)
   {
      broken = b;
   }

   public boolean isBroken()
   {
      return broken;
   }

   public String toString()
   {
      return "Receiver["+name+"] "+(broken?"(BROKEN)":"");
   }

}