# Autor: Paulo Vinicius (rev: logs diários + pasta custom)
# Objetivo: Logging custom com Rich + rotação diária em arquivo por dia
# Caminho : src/utils/customLogging.py

import os
import logging
import logging.config
from logging import LoggerAdapter
from logging.handlers import TimedRotatingFileHandler
from contextvars import ContextVar

try:
    from rich.logging import RichHandler
    HAS_RICH = True
except Exception:
    HAS_RICH = False

DEFAULT_LEVEL = "INFO"
DEFAULT_LOG_DIR = "logs"
DEFAULT_BASE_NAME = "treviso_agent"  # gera treviso_agent.log e rotações .YYYY-MM-DD
_DAILY_LOGGER_CACHE = {}  # key = caminho_arquivo, value = logger

# variável de contexto que armazena o nome do job
job_ctx: ContextVar[str] = ContextVar("job", default="")

class JobFilter(logging.Filter):
    def filter(self, record):
        record.job = job_ctx.get()
        return True

def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def setup_logging(
    log_dir: str = DEFAULT_LOG_DIR,
    base_name: str = DEFAULT_BASE_NAME,
    level: str = DEFAULT_LEVEL,
    backup_count: int = 14,   # quantos arquivos/dias manter
    utc: bool = False         # se True, rotação considera UTC; senão, horário local
) -> None:
    """
    Configura logging com:
      - Console (Rich, se disponível)
      - Arquivo com rotação diária (à meia-noite) e retenção configurável
      - Formato com [job] ; [data/hora] ; [level] ; [logger] ; [func] ; mensagem
    """
    _ensure_dir(log_dir)

    # Arquivo "corrente" (o Python rotaciona acrescentando sufixo YYYY-MM-DD ao antigo)
    log_current = os.path.join(log_dir, f"{base_name}.log")

    detailed_fmt = "[%(job)s] ; [%(asctime)s] ; [%(levelname)s] ; [%(name)s] ; [%(funcName)s] ; %(message)s"
    date_fmt = "%d-%m-%Y %H:%M:%S"

    # Handler de console
    console_handler_cfg = {
        "class": "rich.logging.RichHandler" if HAS_RICH else "logging.StreamHandler",
        "level": level,
    }

    # Handler de arquivo com rotação diária
    # when='midnight' => gira todo dia; backupCount => quantos arquivos manter
    file_handler = TimedRotatingFileHandler(
        filename=log_current,
        when="midnight",
        interval=1,
        backupCount=backup_count,
        encoding="utf-8",
        utc=utc
    )
    # Sufixo padrão vira .YYYY-MM-DD (mais limpo que incluir hora/minuto)
    file_handler.suffix = "%Y-%m-%d"

    # Formatadores
    formatter = logging.Formatter(detailed_fmt, datefmt=date_fmt)
    file_handler.setFormatter(formatter)

    # Console com mesmo formato (Rich ignora o formatter de mensagem “bonita”, mas mantém timestamps via record)
    if HAS_RICH:
        console_handler = RichHandler(rich_tracebacks=False, show_time=False, show_level=False, show_path=False)
    else:
        import logging as _logging
        console_handler = _logging.StreamHandler()
        console_handler.setFormatter(formatter)

    # Root logger
    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Limpa handlers antigos para evitar duplicação quando reimportar
    root.handlers.clear()

    root.addHandler(console_handler)
    root.addHandler(file_handler)
    root.addFilter(JobFilter())

    # Silenciar verbosidades conhecidas
    logging.getLogger("transport").setLevel(logging.WARNING)
    logging.getLogger("sftp").setLevel(logging.WARNING)
    logging.getLogger("paramiko").setLevel(logging.WARNING)

def get_logger(name: str = __name__, job: str = "") -> LoggerAdapter:
    base = logging.getLogger(name)
    return LoggerAdapter(base, {"job": job})

def set_job(job_name: str) -> None:
    """
    Use em início de cada job para aparecer no campo [job] dos logs.
    """
    job_ctx.set(job_name)

def get_daily_path_logger_adapter(
    log_dir: str,
    base_name: str = "audit",    # vai gerar audit.log, rotacionando para .YYYY-MM-DD
    level: str = "INFO",
    backup_count: int = 90,
    utc: bool = False,
    fmt: str = "[%(job)s] ; [%(asctime)s] ; [%(levelname)s] ; [%(name)s] ; [%(funcName)s] ; %(message)s",
    date_fmt: str = "%d-%m-%Y %H:%M:%S",
    job: str = ""
) -> LoggerAdapter:
    """
    Retorna um LoggerAdapter que escreve em <log_dir>/<base_name>.log com rotação diária.
    Reutiliza (cache) o mesmo handler para evitar duplicações.
    Não propaga para o root (sem console).
    """
    os.makedirs(log_dir, exist_ok=True)
    path_current = os.path.join(log_dir, f"{base_name}.log")
    key = os.path.abspath(path_current)

    if key in _DAILY_LOGGER_CACHE:
        lg = _DAILY_LOGGER_CACHE[key]
    else:
        lg_name = "treviso.audit.path::" + key.replace('\\', '/')
        lg = logging.getLogger(lg_name)
        lg.handlers.clear()
        lg.setLevel(getattr(logging, level.upper(), logging.INFO))
        lg.propagate = False
        lg.addFilter(JobFilter())

        h = TimedRotatingFileHandler(
            filename=path_current,
            when="midnight",
            interval=1,
            backupCount=backup_count,
            encoding="utf-8",
            utc=utc
        )
        h.suffix = "%Y-%m-%d"
        h.setFormatter(logging.Formatter(fmt, datefmt=date_fmt))
        lg.addHandler(h)

        _DAILY_LOGGER_CACHE[key] = lg

    return LoggerAdapter(lg, {"job": job})
