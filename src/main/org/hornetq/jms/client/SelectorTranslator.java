/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.jms.client;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * This class converts a JMS selector expression into a HornetQ core filter expression.
 *
 * JMS selector and HornetQ filters use the same syntax but have different identifiers.
 *
 * We basically just need to replace the JMS header and property Identifier names
 * with the corresponding HornetQ field and header Identifier names.
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
   public static String convertToHornetQFilterString(final String selectorString)
   {
      if (selectorString == null)
      {
         return null;
      }

      // First convert any JMS header identifiers

      String filterString = SelectorTranslator.parse(selectorString, "JMSDeliveryMode", "HQDurable");
      filterString = SelectorTranslator.parse(filterString, "'PERSISTENT'", "'DURABLE'");
      filterString = SelectorTranslator.parse(filterString, "'NON_PERSISTENT'", "'NON_DURABLE'");
      filterString = SelectorTranslator.parse(filterString, "JMSPriority", "HQPriority");
      filterString = SelectorTranslator.parse(filterString, "JMSTimestamp", "HQTimestamp");
      filterString = SelectorTranslator.parse(filterString, "JMSMessageID", "HQUserID");
      filterString = SelectorTranslator.parse(filterString, "JMSExpiration", "HQExpiration");

      return filterString;

   }

   private static String parse(final String input, final String match, final String replace)
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

         if ((!inQuote || replaceInQuotes) && c == match.charAt(matchPos))
         {
            matchPos++;

            if (matchPos == match.length())
            {

               boolean matched = true;

               // Check that name is not part of another identifier name

               // Check character after match
               if (i < input.length() - 1 && Character.isJavaIdentifierPart(input.charAt(i + 1)))
               {
                  matched = false;
               }

               // Check character before match
               int posBeforeStart = i - match.length();

               if (posBeforeStart >= 0 && Character.isJavaIdentifierPart(input.charAt(posBeforeStart)))
               {
                  matched = false;
               }

               if (matched)
               {
                  positions.add(i - match.length() + 1);
               }

               // check previous character too

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

         for (int pos : positions)
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
