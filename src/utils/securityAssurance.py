# local: src/utils/securityAssurance.py

import os
import getpass
import paramiko
from paramiko import SSHClient, AutoAddPolicy, RSAKey
import configparser
from cryptography.fernet import Fernet

# logger genérico do módulo
try: from utils.customLogging import get_logger
except: from src.utils.customLogging import get_logger

logger = get_logger(__name__, job="SecurityAssurance")

class SecurityAssurance:
    def __init__(self, *, use_remote: bool = True, keep_local_cache: bool = False):
        """
        use_remote:
            True  -> usa servidor (SSH/SFTP) para ler/escrever key e config.ini.encrypted
            False -> não toca no servidor; roda apenas com artefatos locais

        keep_local_cache:
            True  -> mantém/gera cache local (key.txt e/ou config.ini.encrypted quando aplicável)
            False -> não grava key.txt nem config.ini.encrypted localmente (stateless)
        """
        self.use_remote = use_remote
        self.keep_local_cache = keep_local_cache

        self.hostname = '162.240.232.90'
        self.port = 22022
        self.username = 'root'
        self.password = None

        # caminhos locais
        self.local_config_path = 'C:/Treviso/TrevisoDataAgent/src/config.ini'
        self.local_encrypted_path = 'C:/Treviso/TrevisoDataAgent/src/config.ini.encrypted'
        self.local_key_path = 'key.txt'
        self.local_key_path_2 = 'C:/Treviso/key.txt'

        # caminhos remotos
        self.remote_config_dir = '/root/trevisokey'
        self.remote_key_path = f'{self.remote_config_dir}/key.txt'
        self.remote_encrypted_path = f'{self.remote_config_dir}/config.ini.encrypted'

        import configparser
        self.config_parser = configparser.ConfigParser()
        self.key_path = os.path.expanduser("~/.ssh/ssh_descriptografar_acessos")

        self.ssh = self.get_ssh_connection() if self.use_remote else None

    def _exists_remote(self, sftp, path: str) -> bool:
        try:
            sftp.stat(path)
            return True
        except IOError:
            return False

    def _exists_local_nonempty(self, path: str) -> bool:
        try:
            return os.path.exists(path) and os.path.getsize(path) > 0
        except Exception:
            return False

    def _sftp_read_bytes(self, remote_path: str) -> bytes | None:
        if not self.use_remote or self.ssh is None:
            return None
        try:
            sftp = self.ssh.open_sftp()
            try:
                if not self._exists_remote(sftp, remote_path):
                    return None
                with sftp.file(remote_path, 'rb') as fh:
                    return fh.read()
            finally:
                sftp.close()
        except Exception as e:
            logger.error(f"Erro SFTP lendo {remote_path}: {e}")
            return None

    def _sftp_write_bytes(self, remote_path: str, payload: bytes) -> bool:
        if not self.use_remote or self.ssh is None:
            return False
        try:
            sftp = self.ssh.open_sftp()
            try:
                # garante diretório
                try:
                    sftp.stat(self.remote_config_dir)
                except IOError:
                    sftp.mkdir(self.remote_config_dir)
                with sftp.file(remote_path, 'wb') as fh:
                    fh.write(payload)
                return True
            finally:
                sftp.close()
        except Exception as e:
            logger.error(f"Erro SFTP gravando {remote_path}: {e}")
            return False

    def download_encrypted_config(self) -> bool:
        """
        Baixa sempre a versão mais recente do config.ini.encrypted do servidor
        para o caminho local self.local_encrypted_path.
        """
        ssh = self.ssh
        if ssh is None:
            logger.error("SSH indisponível para baixar config encriptado.")
            return False
        try:
            sftp = ssh.open_sftp()
            sftp.get(self.remote_encrypted_path, self.local_encrypted_path)
            sftp.close()
            logger.info(f"Baixado config encriptado: {self.remote_encrypted_path} -> {self.local_encrypted_path}")
            return True
        except Exception as e:
            logger.error(f"Erro ao baixar config encriptado: {e}")
            return False

    def upload_encrypted_config(self) -> bool:
        """
        Envia o config.ini.encrypted local para o servidor.
        """
        ssh = self.ssh
        if ssh is None:
            logger.error("SSH indisponível para enviar config encriptado.")
            return False
        try:
            sftp = ssh.open_sftp()
            # garante dir remoto (silencioso se já existe)
            try:
                sftp.stat(self.remote_config_dir)
            except IOError:
                sftp.mkdir(self.remote_config_dir)
            sftp.put(self.local_encrypted_path, self.remote_encrypted_path)
            sftp.close()
            logger.info(f"Enviado config encriptado: {self.local_encrypted_path} -> {self.remote_encrypted_path}")
            return True
        except Exception as e:
            logger.error(f"Erro ao enviar config encriptado: {e}")
            return False

    def get_ssh_connection(self):
        ssh = SSHClient()
        ssh.set_missing_host_key_policy(AutoAddPolicy())
        try:
            if os.path.exists(self.key_path):
                key = RSAKey.from_private_key_file(self.key_path)
                ssh.connect(self.hostname, self.port, self.username, pkey=key)
            else:
                print("🔐 Chave não encontrada. Usando login manual...")
                self.password = getpass.getpass("Digite a senha do servidor key: ")
                ssh.connect(self.hostname, self.port, self.username, password=self.password)
        except Exception as e:
            logger.error(f"Erro ao conectar via SSH: {e}")
            return None
        return ssh

    def read_key(self, local_mode=False) -> bytes | None:
        """
        Retorna bytes da chave. Respeita toggles:
        - se use_remote=True e não for local_mode -> lê do servidor
        - senão tenta local (local_key_path_2 ou local_key_path)
        """
        # forçar local_mode ignora remoto (útil para GUI)
        if not local_mode and self.use_remote:
            key_bytes = self._sftp_read_bytes(self.remote_key_path)
            if key_bytes:
                # cache local opcional
                if self.keep_local_cache:
                    try:
                        with open(self.local_key_path, 'wb') as f:
                            f.write(key_bytes)
                    except Exception as e:
                        logger.warning(f"Falha ao gravar cache local da key: {e}")
                return key_bytes

        # fallback local
        try_paths = [self.local_key_path_2, self.local_key_path]
        for p in try_paths:
            try:
                with open(p, 'rb') as key_file:
                    return key_file.read().strip()
            except Exception:
                continue

        logger.error("Chave não encontrada (nem remota, nem local).")
        return None  
    
    def storage_key(self, local_mode=False):
        if local_mode:
            return
        """Armazena a chave de criptografia no servidor remoto."""
        ssh = self.ssh
        if ssh is None:
            return
        try:
            sftp = ssh.open_sftp()
            sftp.put(self.local_key_path, self.remote_key_path)
            sftp.close()
        except Exception as e:
            logger.error(f"Erro ao enviar a chave para o servidor: {e}")

    def encrypt_and_send_config_file(self):
        """
        Criptografa o INI local e publica:
        - se use_remote=True: envia direto para o servidor via SFTP (sem arquivo local)
        - se keep_local_cache=True: também grava cache local do .encrypted
        - nunca mantém key.txt local se keep_local_cache=False
        """
        # 1) ler ini em claro
        if not os.path.exists(self.local_config_path):
            logger.error(f"INI local não encontrado: {self.local_config_path}")
            return

        config_content = open(self.local_config_path, 'rb').read()

        # 2) chave: tenta remota primeiro (se use_remote), senão local; se nenhuma, gera
        chave = None
        if self.use_remote:
            chave = self._sftp_read_bytes(self.remote_key_path)
        if not chave:
            if os.path.exists(self.local_key_path):
                chave = open(self.local_key_path, 'rb').read()
            elif os.path.exists(self.local_key_path_2):
                chave = open(self.local_key_path_2, 'rb').read()

        if not chave:
            from cryptography.fernet import Fernet
            chave = Fernet.generate_key()
            # manter local key só se cache ligado
            if self.keep_local_cache:
                try:
                    with open(self.local_key_path, 'wb') as f:
                        f.write(chave)
                except Exception as e:
                    logger.warning(f"Falha ao gravar key local: {e}")

        from cryptography.fernet import Fernet
        f = Fernet(chave)
        encrypted_content = f.encrypt(config_content)

        # 3) publicar
        ok_remote = True
        if self.use_remote:
            # publica key e encrypted no servidor
            if not self._sftp_write_bytes(self.remote_key_path, chave):
                ok_remote = False
            if not self._sftp_write_bytes(self.remote_encrypted_path, encrypted_content):
                ok_remote = False

        # 4) cache local opcional
        if self.keep_local_cache:
            try:
                with open(self.local_encrypted_path, 'wb') as fh:
                    fh.write(encrypted_content)
            except Exception as e:
                logger.warning(f"Falha ao gravar cache local do encrypted: {e}")
        else:
            # limpar artefatos locais
            try:
                if os.path.exists(self.local_key_path):
                    os.remove(self.local_key_path)
            except Exception:
                pass

        # 5) higienização do INI em claro
        try:
            os.remove(self.local_config_path)
        except Exception:
            pass

        if self.use_remote and not ok_remote:
            logger.error("Publicação remota falhou (key/encrypted).")
        else:
            logger.info("Config encriptada publicada com sucesso.")

    def decrypt_config_file(self):
        """
        Descriptografa config (preferencialmente do servidor, em memória).
        Respeita:
        - use_remote: se True tenta ler remoto; senão usa cache local
        - keep_local_cache: se True grava cache local do .encrypted
        """
        global global_config
        try:
            # 1) lê encriptado (prioridade: remoto em memória)
            enc_bytes = None
            if self.use_remote:
                enc_bytes = self._sftp_read_bytes(self.remote_encrypted_path)
                if enc_bytes and self.keep_local_cache:
                    try:
                        with open(self.local_encrypted_path, 'wb') as f:
                            f.write(enc_bytes)
                    except Exception as e:
                        logger.warning(f"Falha ao salvar cache local do encrypted: {e}")

            # 2) fallback: cache local
            if not enc_bytes:
                if self._exists_local_nonempty(self.local_encrypted_path):
                    enc_bytes = open(self.local_encrypted_path, 'rb').read()
                else:
                    logger.error("config.ini.encrypted não encontrado (remoto indisponível e cache ausente).")
                    return None

            # 3) chave (prioridade: remota)
            chave = self.read_key(local_mode=not self.use_remote)
            if chave is None:
                logger.error("Chave não obtida; abortando decrypt.")
                return None

            from cryptography.fernet import Fernet
            f = Fernet(chave)
            plaintext = f.decrypt(enc_bytes).decode()
            self.config_parser.read_string(plaintext)
            global_config = self.config_parser
            logger.info("Config carregada com sucesso.")
            return global_config

        except Exception as e:
            logger.error(f"Erro ao descriptografar config: {e}")
            global_config = None
            return None