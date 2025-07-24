from airflow.providers.ftp.hooks.ftp import FTPHook
from airflow.providers.sftp.hooks.sftp import SFTPHook

def get_file_from_ftp(remote_path, local_path, ftp_conn_id):
    ftp_hook = FTPHook(ftp_conn_id = ftp_conn_id)
    ftp_hook.retrieve_file(remote_path, local_path)

def get_file_from_sftp(remote_path, local_path, sftp_conn_id):
    sftp_hook = SFTPHook(ssh_conn_id = sftp_conn_id)
    sftp_hook.retrieve_file(remote_path, local_path)