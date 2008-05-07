/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;

import java.util.HashMap;
import java.util.Map;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public enum PacketType
{
   // System
   NULL                                ((byte)1),
   TEXT                                ((byte)2),
   BYTES                               ((byte)3),
   PING                                ((byte)4),
   PONG                                ((byte)5),
   
   // Miscellaneous   
   EXCEPTION                           ((byte)10),
   CLOSE                               ((byte)11),
   
   // Server
   CREATECONNECTION                    ((byte)20),
   CREATECONNECTION_RESP               ((byte)21),    
   
   // Connection
   CONN_CREATESESSION                  ((byte)30),
   CONN_CREATESESSION_RESP             ((byte)31),
   CONN_START                          ((byte)32),
   CONN_STOP                           ((byte)33),

   // Session   
   SESS_CREATECONSUMER                 ((byte)40),
   SESS_CREATECONSUMER_RESP            ((byte)41),
   SESS_CREATEPRODUCER                 ((byte)42),
   SESS_CREATEPRODUCER_RESP            ((byte)43),
   SESS_CREATEBROWSER                  ((byte)44),
   SESS_CREATEBROWSER_RESP             ((byte)45),      
   SESS_ACKNOWLEDGE                    ((byte)46),
   SESS_RECOVER                        ((byte)47),
   SESS_COMMIT                         ((byte)48),
   SESS_ROLLBACK                       ((byte)49),
   SESS_CANCEL                         ((byte)50),
   SESS_QUEUEQUERY                     ((byte)51),
   SESS_QUEUEQUERY_RESP                ((byte)52),
   SESS_CREATEQUEUE                    ((byte)53),
   SESS_DELETE_QUEUE                   ((byte)54),   
   SESS_ADD_DESTINATION                ((byte)55),
   SESS_REMOVE_DESTINATION             ((byte)56),
   SESS_BINDINGQUERY                   ((byte)57),
   SESS_BINDINGQUERY_RESP              ((byte)58),  
   SESS_BROWSER_RESET                  ((byte)59),
   SESS_BROWSER_HASNEXTMESSAGE         ((byte)60),
   SESS_BROWSER_HASNEXTMESSAGE_RESP    ((byte)61),
   SESS_BROWSER_NEXTMESSAGEBLOCK       ((byte)62),
   SESS_BROWSER_NEXTMESSAGEBLOCK_RESP  ((byte)63),
   SESS_BROWSER_NEXTMESSAGE            ((byte)64),
   SESS_BROWSER_NEXTMESSAGE_RESP       ((byte)65),      
   SESS_XA_START                       ((byte)66),
   SESS_XA_END                         ((byte)67),
   SESS_XA_COMMIT                      ((byte)68),
   SESS_XA_PREPARE                     ((byte)69),
   SESS_XA_RESP                        ((byte)70),
   SESS_XA_ROLLBACK                    ((byte)71),
   SESS_XA_JOIN                        ((byte)72),
   SESS_XA_SUSPEND                     ((byte)73),
   SESS_XA_RESUME                      ((byte)74),
   SESS_XA_FORGET                      ((byte)75),
   SESS_XA_INDOUBT_XIDS                ((byte)76),
   SESS_XA_INDOUBT_XIDS_RESP           ((byte)77),
   SESS_XA_SET_TIMEOUT                 ((byte)78),
   SESS_XA_SET_TIMEOUT_RESP            ((byte)79),
   SESS_XA_GET_TIMEOUT                 ((byte)80),
   SESS_XA_GET_TIMEOUT_RESP            ((byte)81),
       
   // Consumer 
   CONS_FLOWTOKEN                      ((byte)90),   
   
   //Producer
   PROD_SEND                           ((byte)100),
   PROD_RECEIVETOKENS                  ((byte)101),
   
   RECEIVE_MSG                        ((byte)111);
   
   // the ALL_TYPES map is used to find the PacketType corresponding to a given byte
   // by using the static method from(byte)
   private static final Map<Byte, PacketType> ALL_TYPES = new HashMap<Byte, PacketType>();

   static {
      PacketType[] types = PacketType.values();
      for (int i = 0; i < types.length; i++)
      {  
         PacketType type = types[i];
         ALL_TYPES.put(type.byteValue(), type);         
      }
   }

   private final byte type;

   PacketType(byte type)
   {
      this.type = type;
   }

   public byte byteValue()
   {
      return type;
   }

   public static PacketType from(byte typeByte)
   {
      PacketType type = ALL_TYPES.get(typeByte);
      if (type != null)
         return type;
      else
         throw new IllegalArgumentException(typeByte + " is not a valid PacketType byte.");
   }
}
