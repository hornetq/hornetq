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
   MSG_XA_PREPARE                 ((byte)59),
   MSG_XA_ROLLBACK                ((byte)60),
   MSG_XA_JOIN                    ((byte)61),
   MSG_XA_RESUME                  ((byte)62),
   MSG_XA_FORGET                  ((byte)63),
   REQ_XA_INDOUBT_XIDS            ((byte)64),
   RESP_XA_INDOUBT_XIDS           ((byte)65),
   
   // ClientConsumer 
   MSG_CHANGERATE                 ((byte)70),
   
   // Browser
   MSG_BROWSER_RESET              ((byte)80),
   REQ_BROWSER_HASNEXTMESSAGE     ((byte)81),
   RESP_BROWSER_HASNEXTMESSAGE    ((byte)82),
   REQ_BROWSER_NEXTMESSAGEBLOCK   ((byte)83),
   RESP_BROWSER_NEXTMESSAGEBLOCK  ((byte)84),
   REQ_BROWSER_NEXTMESSAGE        ((byte)85),
   RESP_BROWSER_NEXTMESSAGE       ((byte)86),

   // Misc
   MSG_CLOSING                    ((byte)90),  
   MSG_CLOSE                      ((byte)91);
   
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
