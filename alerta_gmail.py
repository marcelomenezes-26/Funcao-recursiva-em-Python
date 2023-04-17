from bibliotecas import *

def envia_email(assunto, mensagem, anexo=''):

    # Configurações do servidor SMTP do Gmail
    smtp_server = 'smtp.gmail.com'
    smtp_port = 587
    destinatario = ['bi@sosdocs.com.br', 'marcelomenezes.filho26@gmail.com']

    # Informações da conta do Gmail
    gmail_user = user
    gmail_password = password

    for email in destinatario:
        to_address = email
        subject = assunto
        body = mensagem

        # Cria o objeto MIMEMultipart para montar o e-mail
        msg = MIMEMultipart()
        msg['From'] = gmail_user
        msg['To'] = to_address
        msg['Subject'] = subject

        # Adiciona o corpo do e-mail ao objeto MIMEMultipart
        msg.attach(MIMEText(body, 'plain'))

        # Anexo do e-mail - opcional
        if anexo != '':
            filename = anexo
            attachment = open(anexo, 'rb')
            
            part = MIMEBase('application', 'octet-stream')
            part.set_payload((attachment).read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', "attachment; filename= %s" % filename)
            msg.attach(part)

            attachment.close()

        # Conecta-se ao servidor SMTP do Gmail e envia o e-mail
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            try:
                server.ehlo()
                server.starttls()
                server.ehlo()
                server.login(gmail_user, gmail_password)
                server.sendmail(gmail_user, to_address, msg.as_string())
            except:
                print('erro ao enviar o e-mail')
            else:
                server.quit()
