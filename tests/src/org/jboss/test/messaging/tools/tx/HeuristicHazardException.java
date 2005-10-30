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
package org.jboss.test.messaging.tools.tx;

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
