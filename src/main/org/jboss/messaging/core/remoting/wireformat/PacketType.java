/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public enum PacketType
{
   NULL                           ((byte) 1),
   MSG_JMSEXCEPTION               ((byte) 2),
   TEXT                           ((byte) 3),
   BYTES                          ((byte) 4),
   PING                           ((byte) 5),
   PONG                           ((byte) 6),
   MSG_SETSESSIONID               ((byte) 7),

   // Connection factory
   REQ_CREATECONNECTION           ((byte)10),
   RESP_CREATECONNECTION          ((byte)11), 
   
   // Connection
   REQ_CREATESESSION              ((byte)20),
   RESP_CREATESESSION             ((byte)21),
   MSG_STARTCONNECTION            ((byte)22),
   MSG_STOPCONNECTION             ((byte)23),
   REQ_GETCLIENTID                ((byte)24),
   RESP_GETCLIENTID               ((byte)25),
   MSG_SETCLIENTID                ((byte)26),
   
   // Session
   REQ_CREATECONSUMER             ((byte)40),
   RESP_CREATECONSUMER            ((byte)41),  
   REQ_CREATEDESTINATION          ((byte)42),
   RESP_CREATEDESTINATION         ((byte)43),  
   MSG_ADDTEMPORARYDESTINATION    ((byte)44),
   MSG_DELETETEMPORARYDESTINATION ((byte)45),
   REQ_CREATEBROWSER              ((byte)46),
   RESP_CREATEBROWSER             ((byte)47),
   MSG_SENDMESSAGE                ((byte)48),
   MSG_DELIVERMESSAGE             ((byte)49),  
   MSG_UNSUBSCRIBE                ((byte)50),
   MSG_ACKNOWLEDGE                ((byte)51),
   MSG_RECOVER                    ((byte)52),
   MSG_COMMIT                     ((byte)53),
   MSG_ROLLBACK                   ((byte)54),
   MSG_CANCEL                     ((byte)55),
   
   MSG_XA_START                   ((byte)56),
   MSG_XA_END                     ((byte)57),
   MSG_XA_COMMIT                  ((byte)58),
   REQ_XA_PREPARE                 ((byte)59),
   RESP_XA                        ((byte)60),
   MSG_XA_ROLLBACK                ((byte)61),
   MSG_XA_JOIN                    ((byte)62),
   MSG_XA_SUSPEND                 ((byte)63),
   MSG_XA_RESUME                  ((byte)64),
   MSG_XA_FORGET                  ((byte)65),
   REQ_XA_INDOUBT_XIDS            ((byte)66),
   RESP_XA_INDOUBT_XIDS           ((byte)67),
   MSG_XA_SET_TIMEOUT             ((byte)68),
   MSG_XA_SET_TIMEOUT_RESPONSE    ((byte)69),
   MSG_XA_GET_TIMEOUT             ((byte)70),
   MSG_XA_GET_TIMEOUT_RESPONSE    ((byte)71),
   
   // ClientConsumer 
   MSG_CHANGERATE                 ((byte)80),
   
   // Browser
   MSG_BROWSER_RESET              ((byte)90),
   REQ_BROWSER_HASNEXTMESSAGE     ((byte)91),
   RESP_BROWSER_HASNEXTMESSAGE    ((byte)92),
   REQ_BROWSER_NEXTMESSAGEBLOCK   ((byte)93),
   RESP_BROWSER_NEXTMESSAGEBLOCK  ((byte)94),
   REQ_BROWSER_NEXTMESSAGE        ((byte)95),
   RESP_BROWSER_NEXTMESSAGE       ((byte)96),

   // Misc
   MSG_CLOSING                    ((byte)100),  
   MSG_CLOSE                      ((byte)101);
   
   private final byte type;

   PacketType(byte type)
   {
      this.type = type;
   }

   public byte byteValue()
   {
      return type;
   }
}
