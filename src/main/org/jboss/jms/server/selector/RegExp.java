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
package org.jboss.jms.server.selector;

import java.util.regex.Pattern;

/**
 * Regular expressions to support the selector LIKE operator.
 *
 * @version <tt>$Revision$</tt>
 *
 * @author Norbert Lataille (Norbert.Lataille@m4x.org)
 * @author droy@boostmyscore.com
 * @author Scott.Stark@jboss.org
 * @author Loren Rosen
 *
 * $Id$
 */
public class RegExp 
{
   protected Pattern re;
         
   public RegExp (String pattern, Character escapeChar)
      throws Exception 
   {
      String pat = adjustPattern(pattern, escapeChar);
      re = Pattern.compile(pat);
   }
    
   public boolean isMatch (Object target) 
   {
      String str = target != null ? target.toString() : "";
      return re.matcher(str).matches();
   }
    
   protected String adjustPattern (String pattern, Character escapeChar) 
      throws Exception 
   {
      
      // StringBuffer patternBuf = new StringBuffer( pattern );
      int patternLen = pattern.length();
      StringBuffer REpattern = new StringBuffer(patternLen + 10);
      boolean useEscape = (escapeChar != null);
      char escape = Character.UNASSIGNED;
      if (useEscape) {
         escape = escapeChar.charValue();
      }
      
      REpattern.append ('^');

      for ( int i = 0; i < patternLen; i++ ) {
         boolean escaped = false;
         char c = pattern.charAt( i );

         if ( useEscape && escape == c ) {
            i++;
            if ( i < patternLen ) {
               escaped = true;
               c = pattern.charAt( i );
            } else {
               throw new Exception( "LIKE ESCAPE: Bad use of escape character" );
            }
         }

         // Match characters, or escape ones special to the underlying
         // regex engine
         switch ( c ) {
         case '_':
            if ( escaped ) {
               REpattern.append( c );
            } else {
               REpattern.append( '.' );
            }
            break;
         case '%':
            if ( escaped ) {
               REpattern.append( c );
            } else {
               REpattern.append( ".*" );
            }
            break;
         case '*':
         case '.':
         case '\\':
         case '^':
         case '$':
         case '[':
         case ']':
         case '(':
         case ')':
         case '+':
         case '?':
         case '{':
         case '}':
         case '|':
            REpattern.append( "\\");
            REpattern.append ( c );
            break;
         default:
            REpattern.append( c );
            break;
         }
      }

      REpattern.append( '$' );
      return REpattern.toString();
   }
}
 
