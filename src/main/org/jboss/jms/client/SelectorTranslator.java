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

package org.jboss.jms.client;

import java.util.ArrayList;
import java.util.List;

/**
 * 
 * This class converts a JMS selector expression into a JBM core filter expression.
 * 
 * JMS selector and JBM filters use the same syntax but have different identifiers.
 * 
 * We basically just need to replace the JMS header and property Identifier names
 * with the corresponding JBM field and header Identifier names.
 * 
 * We must be careful not to substitute any literals, or identifers whose name contains the name
 * of one we want to substitute.
 * 
 * This makes it less trivial than a simple search and replace.
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SelectorTranslator
{
   public static String convertToJBMFilterString(String selectorString)
   {
      if (selectorString == null)
      {
         return null;
      }
      
      //First convert any JMS header identifiers
        
      String filterString = parse(selectorString, "JMSDeliveryMode", "JBMDurable");      
      filterString = parse(filterString, "'PERSISTENT'", "'DURABLE'");
      filterString = parse(filterString, "'NON_PERSISTENT'", "'NON_DURABLE'");
      
      filterString = parse(filterString, "JMSPriority", "JBMPriority");      
      
     // filterString = parse(selectorString, "JMSMessageID", "JMSMessageID");  //SAME
      
      filterString = parse(filterString, "JMSTimestamp", "JBMTimestamp");
      
     // filterString = parse(selectorString, "JMSCorrelationID", "JMSCorrelationID");  //SAME
      
     // filterString = parse(selectorString, "JMSType", "JMSType");  //SAME
      
     return filterString;
      
   }
   
   private static String parse(String input, String match, String replace)
   {
      final char quote = '\'';
      
      boolean inQuote = false;
        
      int matchPos = 0;
      
      List<Integer> positions = new ArrayList<Integer>();
      
      boolean replaceInQuotes = match.charAt(0) == quote;
      
      for (int i = 0; i < input.length(); i++)
      {
         char c = input.charAt(i);
                          
         if (c == quote)
         {
            inQuote = !inQuote;
         }
                           
         if ((!inQuote || replaceInQuotes)  && c == match.charAt(matchPos))
         {
            matchPos++;
              
            if (matchPos == match.length())
            {
               
               boolean matched = true;
               
               //Check that name is not part of another identifier name
               
               //Check character after match
               if (i < input.length() -1 && Character.isJavaIdentifierPart(input.charAt(i + 1)))
               {
                  matched = false;        
               }
               
               
               //Check character before match
               int posBeforeStart = i - match.length();
               
               if (posBeforeStart >= 0 && Character.isJavaIdentifierPart(input.charAt(posBeforeStart)))
               {
                  matched = false;
               }
               
               if (matched)
               {
                  positions.add(i - match.length() + 1);
               }
               
               //check previous character too
               
               matchPos = 0;
            }
         }
         else
         {
            matchPos = 0;
         }        
      }  
      
      if (!positions.isEmpty())
      {
         StringBuffer buff = new StringBuffer();
         
         int startPos = 0;
         
         for (int pos: positions)
         {
            String substr = input.substring(startPos, pos);
            
            buff.append(substr);
            
            buff.append(replace);
            
            startPos = pos + match.length();
         }
         
         if (startPos < input.length())
         {
            buff.append(input.substring(startPos, input.length()));
         }
         
         return buff.toString();
      }
      else
      {
         return input;
      }
   }
}
