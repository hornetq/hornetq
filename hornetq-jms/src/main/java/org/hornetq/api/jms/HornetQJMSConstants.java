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

package org.hornetq.api.jms;

/**
 * Constants for HornetQ for property keys used for HornetQ specific extensions to JMS.
 *
 * @author Tim Fox
 *
 *
 */
public class HornetQJMSConstants
{
   public static final String JMS_HORNETQ_INPUT_STREAM = "JMS_HQ_InputStream";
   
   public static final String JMS_HORNETQ_OUTPUT_STREAM = "JMS_HQ_OutputStream";
   
   public static final String JMS_HORNETQ_SAVE_STREAM = "JMS_HQ_SaveStream";
   
   public static final String JBOSS_MESSAGING_BRIDGE_MESSAGE_ID_LIST = "HQ_BRIDGE_MSG_ID_LIST";

   public static final int PRE_ACKNOWLEDGE = 100;

   public static final int INDIVIDUAL_ACKNOWLEDGE = 101;

   public static final String JMS_HORNETQ_ENABLE_BYTE_ARRAY_JMS_CORRELATION_ID_PROPERTY_NAME =
      "hq.jms.support-bytes-id";
}
