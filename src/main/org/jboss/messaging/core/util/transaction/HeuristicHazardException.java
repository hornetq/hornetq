/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.util.transaction;

/**
 * Indicates that a heuristic decision may have been made, the disposition of
 * all relevant updates is not known, and for those updates whose disposition
 * is knwon, either all have been committed or all have been rolled back. (In
 * other words, the <code>HeuristicMixed</code> exception takes priority over
 * this exception.
 *
 * @author <a href="reverbel@ime.usp.br">Francisco Reverbel</a>
 * @version $Revision$ 
 */
class HeuristicHazardException extends Exception
{
   public HeuristicHazardException()
   {
   }

   public HeuristicHazardException(String msg)
   {
      super(msg);
   }
   
}
