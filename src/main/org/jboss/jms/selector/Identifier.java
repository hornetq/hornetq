/*
 * JBossMQ, the OpenSource JMS implementation
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.selector;

/**
 *  This is a JMS identifier
 *
 * @author     Norbert Lataille (Norbert.Lataille@m4x.org)
 * @author     Scott.Stark@jboss.org
 * @version    $Revision$
 */
public class Identifier
{
   String           name;
   Object           value;
   private int      hash;
   
   public Identifier( String name )
   {
      this.name = name;
      hash = name.hashCode();
      value = null;
   }
   
   public String toString()
   {
      return "Identifier@" + name;
   }
   
   public boolean equals( Object obj )
   {
      if ( obj.getClass() != Identifier.class )
      {
         return false;
      }
      if ( obj.hashCode() != hash )
      {
         return false;
      }
      return ( ( Identifier )obj ).name.equals( name );
   }
   
   public int hashCode()
   {
      return hash;
   }

   public String getName()
   {
      return name;
   }
   public Object getValue()
   {
      return value;
   }
   public void setValue(Object value)
   {
      this.value = value;
   }
}
