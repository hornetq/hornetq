/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.tools.tx;

import java.io.ObjectStreamException;
import java.io.Serializable;

/**
 * Type safe enumeration for <code>Resource</code> votes.
 *
 * @author Scott.Stark@jboss.org
 * @author <a href="mailto:reverbel@ime.usp.br">Francisco Reverbel</a>
 * @version $Revision$ 
 */
class Vote implements Serializable
{
   /** 
    * The max ordinal value in use for the Vote enums. When you add a
    * new key enum value you must assign it an ordinal value of the current
    * MAX_TYPE_ID+1 and update the MAX_TYPE_ID value.
    */
   private static final int MAX_TYPE_ID = 2;

   /** The array of Vote indexed by ordinal value of the key */
   private static final Vote[] values = new Vote[MAX_TYPE_ID + 1];
   
   public static final Vote COMMIT = new Vote("COMMIT", 0);
   public static final Vote ROLLBACK = new Vote("ROLLBACK", 1);
   public static final Vote READONLY = new Vote("READONLY", 2);
   
   private final transient String name;

   // this is the only value serialized
   private final int ordinal;

   private Vote(String name, int ordinal)
   {
      this.name = name;
      this.ordinal = ordinal;
      values[ordinal] = this;
   }

   public String toString()
   {
      return name;
   }
   
   Object readResolve() throws ObjectStreamException
   {
      return values[ordinal];
   }

}
