package com.yonyou.cloud.mail.utils;

import java.util.Properties;

import javax.mail.Authenticator;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

/**
 * Created by xin.lee on 2017/3/9. 邮件工具类
 */
public class MailUtils {

    /**
     * 发送邮件
     * 
     * @param to 收件人
     * @param content 激活验证代码
     * @throws MessagingException
     */
    public static void sendMail(String to, String content) throws MessagingException {
        Properties props = new Properties();
        props.put("mail.smtp.host", "mail.yonyou.com");
        props.put("mail.smtp.auth", "true");

        Session session = Session.getInstance(props, new Authenticator() {
            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication("*******", "*******");
            }
        });

        // 创建邮件对象
        Message message = new MimeMessage(session);

        // 设置发件人
        message.setFrom(new InternetAddress("*******"));

        // 设置收件人
        message.setRecipient(Message.RecipientType.TO, new InternetAddress(to));

        // 设置邮件的主题
        message.setSubject("ES报警");

        // 设置邮件的正文
        message.setContent(content, "text/html;charset=utf-8");

        // 发送邮件
        Transport.send(message);
    }

    public static void main(String[] args) throws MessagingException {
        sendMail("zengxs@yonyou.com", "teset");
    }
}
