import json
import logging
import os
from typing import List
from downloader.file_download import FileDownload
from downloader.subject import Observer
import stomp
from jinja2 import Environment, FileSystemLoader
from notifications import email as email_mod

class ActiveMQReporter(Observer):
    def __init__(self, host: str, port: int, username: str, password: str, destination: str) -> None:
        self.host = host; self.port = port; self.username = username; self.password = password; self.destination = destination
    def update(self, downloaded_files: List[FileDownload], files_to_download: List[FileDownload], message: str = ''):
        try:
            logging.info('Publishing message to ActiveMQ...')
            conn = stomp.Connection12([(self.host, self.port)])
            conn.connect(self.username, self.password, wait=True)
            payload = {
                "downloaded": [f.local_file for f in downloaded_files],
                "total": len(files_to_download),
                "message": message or "complete",
            }
            conn.send(self.destination, json.dumps(payload))
            logging.info('Message published.')
            conn.disconnect()
        except Exception as e:
            raise Exception(f'{repr(e)}: Unable publish message to ActiveMQ {self.host}:{self.port}')

class EmailReporter(Observer):
    def __init__(self, recipients: str, cc='', sender: str = 'noreply@freepoint.com', subject: str = 'Downloader Notification') -> None:
        self.recipients = recipients; self.cc = cc; self.sender = sender; self.subject = subject
    def update(self, downloaded_files: List[FileDownload], files_to_download: List[FileDownload], message: str = ''):
        if len(files_to_download) == 0:
            logging.info("No files to be downloaded, email not sent"); return
        body = self.get_email_template(downloaded_files, files_to_download, message)
        email_mod.send_email(senders=self.sender, receivers=self.recipients, cc=self.cc, subject=self.subject, body=body)
        logging.info(f'Email sent to: {self.recipients}, cc: {self.cc}')
    def get_email_template(self, downloaded_files: List[FileDownload], files_to_download: List[FileDownload], message: str = ''):
        env = Environment(loader=FileSystemLoader(os.path.dirname(__file__)))
        template = env.get_template('email_template.html')
        return template.render(downloaded_files=downloaded_files, files_to_download=files_to_download, message=message)

class LogReporter(Observer):
    def update(self, downloaded_files: List[FileDownload], files_to_download: List[FileDownload], message: str = ''):
        report = f'Download progress: {len(downloaded_files)} of {len(files_to_download)}'
        if message: report = f'{report}. Message: {message}'
        logging.info(report)
