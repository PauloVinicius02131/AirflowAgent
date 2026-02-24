# arquivo : src/utils/qualityAssurance.py

import requests
import subprocess
import sys
import time
import json
import functools
import schedule
from concurrent.futures import ThreadPoolExecutor

from requests.exceptions import RequestException

try: from utils.customLogging import get_logger, job_ctx
except : from customLogging import get_logger, job_ctx
# OBS: não importamos `jobs` diretamente

# ----------------------------
# Decorador que protege cada job para que erros não quebrem o scheduler
# ----------------------------
def safe_job(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        token = job_ctx.set(func.__name__)
        logger = get_logger(func.__name__, job=func.__name__)
        try:
            result = func(*args, **kwargs)
            logger.info(f"Job '{func.__name__}' executado com sucesso.")
            return result
        except Exception as e:
            logger.error(f"Erro em job '{func.__name__}' {e}", exc_info=False)
            return None
        finally:
            job_ctx.reset(token)
    return wrapper

# ----------------------------
# Variáveis e funções de configuração/VPN
# ----------------------------
security_assurance_instance = None
config = None

def get_security_assurance_instance():
    global security_assurance_instance
    if security_assurance_instance is None:
        import os
        from utils.securityAssurance import SecurityAssurance
        use_remote = os.getenv("TDA_USE_REMOTE", "1") == "1"
        keep_cache = os.getenv("TDA_KEEP_LOCAL_CACHE", "0") == "1"
        security_assurance_instance = SecurityAssurance(
            use_remote=use_remote,
            keep_local_cache=keep_cache
        )
    return security_assurance_instance

def get_config():
    """
    Lê e devolve o JSON de configuração descriptografado.
    """
    global config
    if config is None:
        sec = get_security_assurance_instance()
        cfg = sec.decrypt_config_file()
        if not cfg:
            logger = get_logger('Config')
            logger.error("Erro: arquivo de configuração não encontrado ou inválido")
            config = {}
        else:
            config = cfg if isinstance(cfg, dict) else dict(cfg)
    return config

def ensure_vpn_connection():
    import subprocess
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    from requests.exceptions import RequestException, Timeout, HTTPError, ConnectionError

    logger = get_logger('VPN')

    def _requests_session():
        retry = Retry(
            total=3,
            backoff_factor=0.7,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET"])
        )
        s = requests.Session()
        s.headers.update({"User-Agent": "TrevisoDataAgent/1.0 (+VPN check)"})
        s.mount("http://",  HTTPAdapter(max_retries=retry))
        s.mount("https://", HTTPAdapter(max_retries=retry))
        return s

    def _normalize_ip(val):
        if not val:
            return None
        return val.split(",")[0].strip()

    def _parse_cftrace(text: str):
        for line in text.splitlines():
            if line.startswith("ip="):
                return line.split("=", 1)[1].strip()
        return None

    def _get_external_ip(timeout: float = 8.0) -> str:
        IP_ENDPOINTS = [
            ("https://api.ipify.org?format=json",    "ip",      "json"),
            ("https://ifconfig.me/all.json",         "ip_addr", "json"),
            ("https://icanhazip.com",                None,      "text"),
            ("https://cloudflare.com/cdn-cgi/trace", "ip",      "cftrace"),
            ("https://httpbin.org/ip",               "origin",  "json"),
        ]
        s = _requests_session()
        last_err = None
        for url, key, mode in IP_ENDPOINTS:
            try:
                resp = s.get(url, timeout=timeout)
                resp.raise_for_status()
                if mode == "json":
                    data = resp.json()
                    ip = _normalize_ip(data.get(key))
                elif mode == "text":
                    ip = _normalize_ip(resp.text.strip())
                elif mode == "cftrace":
                    ip = _normalize_ip(_parse_cftrace(resp.text))
                else:
                    ip = None
                if ip:
                    return ip
            except (Timeout, HTTPError, ConnectionError, RequestException) as e:
                last_err = e
                continue
        raise RuntimeError(f"Falha ao obter IP externo de todos os provedores. Último erro: {last_err}")

    try:
        ip_externo = _get_external_ip(timeout=8.0)

        # --- IP fixo padrão ---
        expected_ip = '201.48.69.101'

        # Se houver config com outro ExpectedIP, substitui
        if isinstance(config, dict):
            vpn_cfg = (config.get('VPN') or {})
            expected_ip = vpn_cfg.get('ExpectedIP', expected_ip)

        expected_ips = [expected_ip] if isinstance(expected_ip, str) else list(expected_ip)

        if expected_ips:
            if ip_externo not in expected_ips:
                vpn = config['VPN']  # type: ignore
                cmd = f"rasdial {vpn['VPN_Name']} {vpn['User']} {vpn['Password']}"
                try:
                    subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
                    logger.info("VPN reconectada com sucesso")
                except subprocess.CalledProcessError as e:
                    err = e.stderr.strip() if e.stderr else str(e)
                    logger.error(f"Falha ao conectar VPN: {err}")
            else:
                logger.debug(f"IP externo OK: {ip_externo} (esperado em {expected_ips})")
        else:
            logger.warning(f"ExpectedIP não configurado — IP externo atual: {ip_externo}")

    except Exception as e:
        logger.error(f"Erro ao verificar/conectar VPN: {e}")

# ----------------------------
# Animação de carregamento (apenas efeito no console)
# ----------------------------
def loading_animation(duration=5, interval=0.1):
    frames = ['[    ]', '[=   ]', '[==  ]', '[=== ]', '[ ===]', '[  ==]', '[   =]']
    end_time = time.time() + duration
    while time.time() < end_time:
        for frame in frames:
            sys.stdout.write(f"\r{frame}")
            sys.stdout.flush()
            time.sleep(interval)
    sys.stdout.write("\r" + ' ' * len(frames[-1]) + "\r")

# ----------------------------
# ThreadPoolExecutor global para executar cada job em paralelo
# ----------------------------
EXECUTOR = ThreadPoolExecutor(max_workers=5)

def _threaded_wrapper(fn):
    """
    Retorna uma função que, quando invocada pelo schedule,
    submete fn() ao EXECUTOR dentro de safe_job.
    """
    wrapped = safe_job(fn)
    def _inner():
        EXECUTOR.submit(wrapped)
    return _inner

# ----------------------------
# Registra no `schedule` todos os jobs lidos do JSON.
# Não importa qual seja o módulo: basta que o chamador passe
# algo que permita fazer getattr(nome_job).
# ----------------------------
def register_schedules_from_json(path_json: str, job_module):

    logger = get_logger('Scheduler')
    try:
        with open(path_json, 'r', encoding='utf-8') as f:
            sched_conf = json.load(f)
    except Exception as e:
        logger.error(f"Falha ao ler '{path_json}': {e}", exc_info=True)
        return

    # --- DAILY ---
    for entry in sched_conf.get('daily', []):
        nome_job = entry.get('job')
        horarios = entry.get('times', [])
        fn = getattr(job_module, nome_job, None)
        if fn is None:
            logger.warning(f"[Scheduler] Função '{nome_job}' não encontrada em {job_module.__name__}")
            continue

        for hhmm in horarios:
            try:
                schedule.every().day.at(hhmm).do(_threaded_wrapper(fn))
                if logger.isEnabledFor(10):  # 10 = logging.DEBUG
                    logger.info(f"[Scheduler] Agendado '{nome_job}' diariamente às {hhmm}")
            except Exception as e:
                logger.error(f"[Scheduler] Horário inválido '{hhmm}' para job '{nome_job}': {e}")

    # --- WEEKLY ---
    for entry in sched_conf.get('weekly', []):
        nome_job = entry.get('job')
        dia       = entry.get('day', '').lower()   # ex: "saturday"
        horario   = entry.get('time')              # ex: "21:00"
        fn = getattr(job_module, nome_job, None)
        if fn is None:
            logger.warning(f"[Scheduler] Função '{nome_job}' não encontrada em {job_module.__name__}")
            continue

        try:
            # ex.: schedule.every().saturday.at("21:00").do(...)
            weekday_attr = getattr(schedule.every(), dia)
            weekday_attr.at(horario).do(_threaded_wrapper(fn))

            if logger.isEnabledFor(10):  # 10 = logging.DEBUG
                logger.info(f"[Scheduler] Agendado '{nome_job}' toda '{dia}' às {horario}")
        except Exception as e:
            logger.error(f"[Scheduler] Dia ({dia}) ou horário ({horario}) inválido para job '{nome_job}': {e}")

def send_config_file():
    sec = get_security_assurance_instance()
    sec.encrypt_and_send_config_file()
