# -*- coding: utf-8 -*-
"""
Notificador de email customizado para callbacks das DAGs SOST
Usa SMTP diretamente evitando bugs do backend padrão do Airflow
"""
import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

# Configurações Gmail
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("GMAIL_ADDRESS", "")
SMTP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD", "")
EMAIL_TO = os.getenv("GMAIL_TO", SMTP_USER)


def send_notification(subject: str, html_body: str):
    """Envia email via SMTP Gmail"""
    try:
        if not SMTP_USER or not SMTP_PASSWORD or not EMAIL_TO:
            print("❌ Variáveis de email ausentes (GMAIL_ADDRESS, GMAIL_APP_PASSWORD ou GMAIL_TO)")
            return False

        msg = MIMEMultipart('alternative')
        msg['From'] = SMTP_USER
        msg['To'] = EMAIL_TO
        msg['Subject'] = subject
        
        html_part = MIMEText(html_body, 'html')
        msg.attach(html_part)
        
        server = smtplib.SMTP(SMTP_HOST, SMTP_PORT)
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.send_message(msg)
        server.quit()
        
        print(f"✅ Email enviado: {subject}")
        return True
    except Exception as e:
        print(f"❌ Erro ao enviar email: {e}")
        return False


def notify_success(context):
    """Callback para sucesso da DAG"""
    dag = context.get("dag")
    dag_run = context.get("dag_run")
    run_id = dag_run.run_id if dag_run else context.get("run_id", "N/A")
    logical_date = context.get("logical_date", "N/A")
    dag_id = dag.dag_id if dag else "unknown"
    
    subject = f"✅ Sucesso: {dag_id}"
    html_body = f"""
    <html>
    <body style="font-family: Arial, sans-serif;">
        <h2 style="color: #28a745;">✅ Execução concluída com sucesso!</h2>
        <table style="border-collapse: collapse; margin: 20px 0;">
            <tr>
                <td style="padding: 8px; font-weight: bold;">DAG:</td>
                <td style="padding: 8px;">{dag_id}</td>
            </tr>
            <tr>
                <td style="padding: 8px; font-weight: bold;">Run ID:</td>
                <td style="padding: 8px;">{run_id}</td>
            </tr>
            <tr>
                <td style="padding: 8px; font-weight: bold;">Data/Hora:</td>
                <td style="padding: 8px;">{logical_date}</td>
            </tr>
            <tr>
                <td style="padding: 8px; font-weight: bold;">Timestamp:</td>
                <td style="padding: 8px;">{datetime.now().strftime('%d/%m/%Y %H:%M:%S')}</td>
            </tr>
        </table>
        <p style="background: #d4edda; padding: 15px; border-left: 4px solid #28a745;">
            <strong>Pipeline SOST executado sem erros.</strong><br>
            Os dados foram processados e estão disponíveis.
        </p>
    </body>
    </html>
    """
    
    send_notification(subject, html_body)


def notify_failure(context):
    """Callback para falha da DAG"""
    dag = context.get("dag")
    dag_run = context.get("dag_run")
    run_id = dag_run.run_id if dag_run else context.get("run_id", "N/A")
    logical_date = context.get("logical_date", "N/A")
    exception = context.get("exception", "Erro desconhecido")
    dag_id = dag.dag_id if dag else "unknown"
    
    subject = f"❌ Falha: {dag_id}"
    html_body = f"""
    <html>
    <body style="font-family: Arial, sans-serif;">
        <h2 style="color: #dc3545;">❌ Execução com falha!</h2>
        <table style="border-collapse: collapse; margin: 20px 0;">
            <tr>
                <td style="padding: 8px; font-weight: bold;">DAG:</td>
                <td style="padding: 8px;">{dag_id}</td>
            </tr>
            <tr>
                <td style="padding: 8px; font-weight: bold;">Run ID:</td>
                <td style="padding: 8px;">{run_id}</td>
            </tr>
            <tr>
                <td style="padding: 8px; font-weight: bold;">Data/Hora:</td>
                <td style="padding: 8px;">{logical_date}</td>
            </tr>
            <tr>
                <td style="padding: 8px; font-weight: bold;">Timestamp:</td>
                <td style="padding: 8px;">{datetime.now().strftime('%d/%m/%Y %H:%M:%S')}</td>
            </tr>
        </table>
        <div style="background: #f8d7da; padding: 15px; border-left: 4px solid #dc3545;">
            <strong>Erro:</strong><br>
            <code style="background: #fff; padding: 5px; display: block; margin-top: 10px;">{exception}</code>
        </div>
        <p style="margin-top: 20px;">
            <strong>Ação necessária:</strong> Acesse o Airflow para verificar os logs detalhados.
        </p>
    </body>
    </html>
    """
    
    send_notification(subject, html_body)
