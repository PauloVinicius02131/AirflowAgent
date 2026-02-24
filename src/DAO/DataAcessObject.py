# Autor: Paulo Vinicius
# Objetivo: Arquivo com classes sobre os bancos de dados.
# Caminho : src/dao/DataAcessObject.py

import os
import re
import time
import tempfile
import subprocess
import inspect
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyodbc
from pandas.api import types as pd_types
from utils.customLogging import get_logger
from utils.customLogging import get_daily_path_logger_adapter  # <-- NOVO
from pytz import timezone
from typing import Union, List, Tuple, Dict, Any, Optional

import json
import uuid

def _quote_name(name: str) -> str:
    """
    Envolve o nome em colchetes para maior segurança em SQL dinâmico.
    """
    return f"[{name.replace(']', ']]')}]"

class SQLServerTuning:
    """
    Classe para cargas massivas e upsert em SQL Server, com staging via BCP
    e operações de MERGE (INSERT, UPDATE e DELETE com log de chaves).
    """
    def __init__(self):
        self.driver   = 'ODBC Driver 17 for SQL Server'
        self.server   = 'dw-treviso.cqwy8cgcgdlv.us-east-1.rds.amazonaws.com'
        self.database = 'DW_TREVISO'
        self.username = 'admin'
        self.password = 'kyjfEYDt7DX1LaCzY42R'
        self.port     = 1433
        self.bcp_path = '/usr/local/bin/bcp'

        if not Path(self.bcp_path).is_file():
            raise FileNotFoundError(f"BCP não encontrado em: {self.bcp_path}")

        self.conn_str_pyodbc = (
            f"DRIVER={{{self.driver}}};SERVER={self.server},{self.port};"
            f"DATABASE={self.database};UID={self.username};PWD={self.password};"
        )
        self.conn_str_sqlalchemy = (
            f"mssql+pyodbc://{self.username}:{self.password}@"
            f"{self.server},{self.port}/{self.database}?driver="
            f"{self.driver.replace(' ', '+')}"
        )


        # >>> BRASÍLIA (America/São_Paulo) para usar nos MERGE/DEFAULT:
        self._tz_expr = "(SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'E. South America Standard Time')"

        caller = inspect.stack()[2].function
        self.logger = get_logger(__name__, caller)
        self.audit = None  # <- inicializa explicitamente

    def _mk_run_id(self) -> str:
        # UTC para ordenação estável + pid + aleatório curto
        # Retorna timestamp em horário de Brasília + pid + aleatório curto
        dt_brasilia = datetime.now(timezone("America/Sao_Paulo"))
        return f"{dt_brasilia:%Y%m%d_%H%M%S_%f}_{os.getpid()}_{uuid.uuid4().hex[:8]}"

    def _norm_audit(self,a):
        if a is None:
            return {'update','delete'}  # padrão
        if isinstance(a, str):
            a = a.strip().lower()
            if a in ('all','*'):
                return {'insert','update','delete'}
            if a == '':
                return set()
            return {a}
        # iterável
        return {str(x).strip().lower() for x in a}

    def _audit_paths(self, audit_dir: str, schema: str, table: str) -> Dict[str, Path]:
        base = f"{schema}.{table}"
        p = Path(audit_dir)
        return {
            "inserts": p / f"{base}.inserts.jsonl",
            "updates": p / f"{base}.updates.jsonl",
            "deletes": p / f"{base}.deletes.jsonl",
        }

    def _audit_write_lines(self, path: Path, lines: List[str]) -> None:
        if not lines:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a", encoding="utf-8", newline="\n") as f:
            f.write("".join(lines))  # cada linha já termina com '\n'

    def _concat_keys(self, keys_order: List[str], row: Dict[str, Any]) -> str:
        parts = []
        for k in keys_order:
            v = row.get(k)
            sv = "" if v is None else str(v)
            parts.append(f"{k}={sv}")
        return "|".join(parts)

    def connect_pyodbc(self) -> pyodbc.Connection:
        return pyodbc.connect(self.conn_str_pyodbc)

    def _introspect(
        self, schema: str, table: str
    ) -> Tuple[pyodbc.Connection, pyodbc.Cursor, List[str], List[str]]:
        """Retorna conn, cursor, all_cols e pk_cols em uppercase."""
        t0 = time.time()
        conn = self.connect_pyodbc()
        cur  = conn.cursor()
        cur.fast_executemany = True

        # 1) Colunas "usáveis" (sem computadas e sem rowversion/timestamp)
        cur.execute("""
            SELECT c.name
            FROM sys.columns c
            JOIN sys.tables  t  ON t.object_id = c.object_id
            JOIN sys.schemas s  ON s.schema_id = t.schema_id
            JOIN sys.types   ty ON ty.user_type_id = c.user_type_id
            WHERE s.name = ? AND t.name = ?
            AND c.is_computed = 0
            AND ty.name NOT IN ('timestamp', 'rowversion')
            ORDER BY c.column_id;
        """, (schema, table))
        
        all_cols = [r[0].upper() for r in cur.fetchall()]

        # 2) Colunas da PK (ordem correta)
        cur.execute("""
            SELECT c.name
            FROM sys.indexes i
            JOIN sys.index_columns ic
                ON ic.object_id = i.object_id
                AND ic.index_id  = i.index_id
            JOIN sys.columns c
                ON c.object_id = ic.object_id
                AND c.column_id = ic.column_id
            JOIN sys.tables t  ON t.object_id = i.object_id
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            WHERE s.name = ? AND t.name = ?
            AND i.is_primary_key = 1
            ORDER BY ic.key_ordinal;
        """, (schema, table))
        pk_cols = [r[0].upper() for r in cur.fetchall()]

        self.logger.debug(f"Introspect {schema}.{table}: {time.time()-t0:.3f}s")
        return conn, cur, all_cols, pk_cols

    def _sanitize_df_strings(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normaliza colunas de texto para reduzir ruídos que causam truncamento:
        - remove NBSP (U+00A0) e zero-width (U+200B);
        - aplica strip().
        """
        NBSP = '\u00A0'
        ZWSP = '\u200B'
        df2 = df.copy()
        for col in df2.columns:
            if pd_types.is_string_dtype(df2[col]):
                s = df2[col].astype(str)
                s = s.str.replace(NBSP, ' ', regex=False)
                s = s.str.replace(ZWSP, '', regex=False)
                s = s.str.strip()
                df2[col] = s
        return df2

    def _preflight_text_widths(
        self,
        cur: pyodbc.Cursor,
        schema: str,
        table: str,
        df: pd.DataFrame,
        *,
        truncate_overflow: Optional[Dict[str, int]] = None
    ) -> None:
        """
        Verifica larguras máximas das colunas de texto conforme metadata do SQL Server.
        - Para tipos N* (NVARCHAR/NCHAR), max_length vem em bytes → divide por 2.
        - Se 'truncate_overflow' tiver a coluna, corta o valor antes e segue.
        - Caso contrário, lança DataValidationError com amostras.
        """
        truncate_overflow = {str(k).upper(): int(v) for k, v in (truncate_overflow or {}).items()}
        cur.execute("""
            SELECT c.name, UPPER(ty.name) AS typ, c.max_length
            FROM sys.columns c
            JOIN sys.tables  t  ON t.object_id = c.object_id
            JOIN sys.schemas s  ON s.schema_id = t.schema_id
            JOIN sys.types   ty ON ty.user_type_id = c.user_type_id
            WHERE s.name = ? AND t.name = ?;
        """, (schema, table))
        meta = {r[0].upper(): (r[1], int(r[2])) for r in cur.fetchall()}

        offenders: List[tuple[int, str, str, int, int]] = []  # (row_idx, COL, val_sample, max_chars, len)
        for col in df.columns:
            COL = str(col).upper()
            if COL not in meta or not pd_types.is_string_dtype(df[col]):
                continue
            typ, max_len_bytes = meta[COL]
            if max_len_bytes < 0:  # NVARCHAR(MAX) / VARCHAR(MAX)
                continue
            max_chars = (max_len_bytes // 2) if 'NCHAR' in typ or 'NVARCHAR' in typ else max_len_bytes
            if max_chars <= 0:
                continue

            s = df[col].astype(str)
            mask = s.str.len() > max_chars
            if not mask.any():
                continue

            if COL in truncate_overflow:
                # corta e continua
                df.loc[mask, col] = s[mask].str.slice(0, min(max_chars, truncate_overflow[COL]))
                continue

            # coleta até 5 amostras
            sample = s[mask].head(5)
            for idx, v in sample.items():
                offenders.append((int(idx), COL, v[:80], max_chars, len(v))) # type: ignore

        if offenders:
            lines = [f"linha={i} col={c} len={ln} > max={mx} val='{v}'" for i, c, v, mx, ln in offenders]
            msg = "Violação de largura em colunas de texto:\n" + "\n".join(lines)
            raise DataValidationError(msg) # type: ignore

    def _prepare_and_load(
        self,
        cur: pyodbc.Cursor,
        df: pd.DataFrame,
        all_cols: List[str],
        schema: str,
        table: str,
        chunk_size: int,
        *,
        load_cols: Optional[List[str]] = None,
        suffix: Optional[str] = None   # <— era Optional[List[str]]
        
    ) -> str:
        """
        Cria/trunca global temp table e carrega via BCP apenas as colunas
        load_cols (ou all_cols se load_cols=None), EXCLUINDO DT_GRAVACAO.
        Retorna stg_name.
        """

            
        AUDIT_COL = 'DT_GRAVACAO'
        base_cols = [c.upper() for c in (load_cols or all_cols)]
        cols_to_use = [c for c in base_cols if c != AUDIT_COL]

        if not cols_to_use:
            raise ValueError("Nenhuma coluna para staging (todas eram DT_GRAVACAO).")

        uid = f"{int(time.time())}_{os.getpid()}"
        stg_suffix = f"_{suffix}" if suffix else ""
        stg_name = f"##{table}{stg_suffix}_stg_{uid}"

        # NOVO: duas formas do nome totalmente qualificadas
        stg_full_sql   = f"tempdb..{_quote_name(stg_name)}"  # p/ SQL (CREATE/TRUNCATE/DROP)
        stg_full_objid = f"tempdb..{stg_name}"               # p/ OBJECT_ID (sem colchetes)
        stg_full_bcp   = f"tempdb..{stg_name}"               # p/ BCP (sem colchetes)

        col_list = ', '.join(_quote_name(c) for c in cols_to_use)

        # cria ou trunca staging (SQL usa a versão com colchetes)
        cur.execute(f"""
            IF OBJECT_ID(N'{stg_full_objid}','U') IS NULL
            BEGIN
                SELECT TOP 0 {col_list} INTO {stg_full_sql}
                FROM {_quote_name(schema)}.{_quote_name(table)};
            END 
            ELSE 
                TRUNCATE TABLE {stg_full_sql};
        """)
        cur.connection.commit()

        # somente colunas da staging (sem DT_GRAVACAO)
        df2 = df.rename(columns=str.upper).reindex(columns=cols_to_use).copy()

        # ------------------------------------------------------------------
        # NOVO: ajusta casas decimais com base no metadata DECIMAL/NUMERIC
        # ------------------------------------------------------------------
        cur.execute("""
            SELECT 
                c.name,
                UPPER(ty.name) AS typ,
                c.precision,
                c.scale
            FROM sys.columns c
            JOIN sys.tables  t  ON t.object_id = c.object_id
            JOIN sys.schemas s  ON s.schema_id = t.schema_id
            JOIN sys.types   ty ON ty.user_type_id = c.user_type_id
            WHERE s.name = ? AND t.name = ?;
        """, (schema, table))

        num_meta = {}
        for name, typ, prec, scale in cur.fetchall():
            name_u = str(name).upper()
            if typ in ("DECIMAL", "NUMERIC"):
                num_meta[name_u] = {
                    "typ": typ,
                    "precision": int(prec or 0),
                    "scale": int(scale or 0),
                }

        # Arredonda colunas DECIMAL/NUMERIC no DF antes do BCP
        for col in df2.columns:
            COL = str(col).upper()
            meta = num_meta.get(COL)
            if not meta:
                continue

            scale = meta["scale"]
            if scale is None or scale < 0:
                continue

            # Se for float/int no pandas, arredonda
            if pd_types.is_float_dtype(df2[col]) or pd_types.is_integer_dtype(df2[col]):
                df2[col] = df2[col].round(scale)


        # --- Diagnóstico: contar CR/LF/TAB e delimitador '|' em cada coluna de texto
        bad_cols = []
        for col in df2.columns:
            if pd_types.is_string_dtype(df2[col]):
                s = df2[col].astype(str)
                mask = (
                    s.str.contains(r'\r|\n|\t|\|', regex=True, na=False) |
                    s.str.contains('\u00A0', regex=False, na=False) |    # NBSP
                    s.str.contains('\u200B', regex=False, na=False)      # zero-width
                )
                if mask.any():
                    bad_cols.append((col, int(mask.sum())))
        if bad_cols:
            self.logger.warning("Campos com CR/LF/TAB/|/NBSP/ZWSP detectados: %s",
                                "; ".join(f"{c}={n}" for c,n in bad_cols[:10]))

        # NULLs: datetime -> None, texto -> '', numéricos -> None
        for col in df2.columns:
            if pd_types.is_datetime64_any_dtype(df2[col]):
                df2[col] = df2[col].where(df2[col].notnull(), None)
            elif pd_types.is_string_dtype(df2[col]):
                df2[col] = df2[col].fillna('')
            else:
                df2[col] = df2[col].where(df2[col].notnull(), None)

        # Sanitização robusta para BCP (sem aspas, sem linhas quebradas, sem delimitador)
        for col in df2.columns:
            if pd_types.is_string_dtype(df2[col]):
                s = df2[col].astype(str)
                s = (s
                    .str.replace('\u00A0', ' ',  regex=False)   # NBSP -> espaço normal
                    .str.replace('\u200B', '',   regex=False)   # zero-width remove
                    .str.replace('\r', ' ',      regex=False)
                    .str.replace('\n', ' ',      regex=False)
                    .str.replace('\t', ' ',      regex=False)
                    .str.replace('|',  '/',      regex=False)   # seu separador é '|'
                    .str.strip()
                )
                df2[col] = s

        # CSV temporário
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.csv', newline='', mode='w')
        df2.to_csv(tmp.name, index=False, header=False, sep='|', encoding='utf-8')
        tmp_path = tmp.name; tmp.close()

        # Arquivos de diagnóstico do BCP
        err_fd, err_path = tempfile.mkstemp(suffix='.err'); os.close(err_fd)
        out_fd, out_path = tempfile.mkstemp(suffix='.out'); os.close(out_fd)

        # BCP
        t0 = time.time()
        cmd = [
                    self.bcp_path, stg_full_bcp, 'in', tmp_path,
                    '-S', f"{self.server},{self.port}",
                    '-U', self.username, '-P', self.password,
                    '-c', '-t|', '-C', '65001', '-k',
                    '-a', '32767', '-b', str(chunk_size),
                    '-m', '1',
                    '-q',
                ]
                
        self.logger.info(f"Executando BCP (v17): {' '.join(cmd).replace(self.password, '********')}")

        os.chmod(tmp_path, 0o666)
        os.chmod(err_path, 0o666)
        os.chmod(out_path, 0o666)

        proc = subprocess.run(cmd, capture_output=True, text=True)

        if proc.returncode != 0:
            # --- Bloco de Enriquecimento de Log ---
            self.logger.error("======= DEBUG BCP FALLBACK =======")
            self.logger.error(f"Return Code: {proc.returncode}")
            
            # Mostra o que o BCP cuspiu no terminal (aqui virá o erro real do SQL)
            if proc.stdout: self.logger.error(f"STDOUT: {proc.stdout}")
            if proc.stderr: self.logger.error(f"STDERR: {proc.stderr}")
            
            # Amostra do CSV gerado para validar se o delimitador '|' está correto
            try:
                with open(tmp_path, 'r') as f:
                    amostra = [next(f) for _ in range(3)]
                self.logger.error(f"Amostra do CSV (primeiras 3 linhas):\n{''.join(amostra)}")
            except Exception as e:
                self.logger.error(f"Não foi possível ler amostra do CSV: {e}")
            
            self.logger.error("==================================")

            # Limpeza antes de estourar o erro
            if os.path.exists(tmp_path): os.remove(tmp_path)
            
            raise RuntimeError(f"BCP falhou com código {proc.returncode} em {schema}.{table}. Verifique o log detalhado acima.")


        # coleta logs (não apague tmp_path ainda!)
        try:
            err_head = open(err_path, 'r', errors='ignore').read(2000)
        except Exception:
            err_head = ""
        try:
            out_head = open(out_path, 'r', errors='ignore').read(2000)
        except Exception:
            out_head = ""

        if proc.returncode != 0:
            # junta os textos disponíveis do BCP
            err_text = (err_head or "") + "\n" + (out_head or "") + "\n" + (proc.stdout or "") + "\n" + (proc.stderr or "")

            # regex simples: "Linha 1315, Coluna 32: ..." ou "Row 1315, Column 32: ..."
            rx = re.compile(r'(?:Linha|Row)\s+(\d+),\s*(?:Coluna|Column)\s+(\d+)\s*:\s*(.+)', re.IGNORECASE)

            # mapeia posição → nome da coluna (1-based), de acordo com a staging
            pos_to_col = {i+1: col for i, col in enumerate(cols_to_use)}

            matches = rx.findall(err_text)

            if matches:
                # contagem por coluna
                counts = {}
                for ln, pos, msg in matches:
                    pos_i = int(pos)
                    col_nm = pos_to_col.get(pos_i, f"?POS{pos_i}")
                    counts[col_nm] = counts.get(col_nm, 0) + 1

                # resumo das colunas com mais erros
                resumo_cols = "; ".join([f"{c} x{n}" for c, n in sorted(counts.items(), key=lambda x: -x[1])[:10]])
                self.logger.error("[BCP] code=%s em %s.%s; colunas com erro: %s",
                                proc.returncode, schema, table, resumo_cols or "(não parseado)")

                # amostras: até N erros — mostra linha, coluna, nome, categoria e valor bruto (com len)
                N = 5
                # vamos coletar as linhas que precisamos ler do CSV
                sample_specs = []
                for ln, pos, msg in matches[:N]:
                    pos_i = int(pos)
                    col_nm = pos_to_col.get(pos_i, f"?POS{pos_i}")
                    low = msg.lower().strip()
                    if "trunc" in low or "truncamento" in low:
                        categoria = "TRUNCAMENTO"
                    elif "convers" in low or "convert" in low:
                        categoria = "CONVERSAO_TIPO"
                    else:
                        categoria = "ERRO"
                    # limpa lixo do fim do texto (ex.: '@#')
                    msg_clean = re.sub(r'\s*@#\s*$', '', msg).strip()
                    sample_specs.append((int(ln), pos_i, col_nm, categoria, msg_clean))

                # leitura única do CSV para pegar os valores das linhas solicitadas
                if sample_specs:
                    wanted_lines = {x[0] for x in sample_specs}
                    values_by_line = {}
                    try:
                        with open(tmp_path, 'r', encoding='utf-8', errors='ignore') as f:
                            for i, line in enumerate(f, start=1):
                                if i in wanted_lines:
                                    values_by_line[i] = line.rstrip('\n').split('|')
                                    if len(values_by_line) == len(wanted_lines):
                                        break
                    except Exception:
                        pass

                    samples_msgs = []
                    for ln_i, pos_i, col_nm, categoria, msg_clean in sample_specs:
                        parts = values_by_line.get(ln_i)
                        raw_val = ""
                        if parts and 1 <= pos_i <= len(parts):
                            raw_val = parts[pos_i - 1]
                        # encurta valor para não poluir o log
                        show_val = (raw_val[:120] + '…') if len(raw_val) > 120 else raw_val
                        samples_msgs.append(
                            f"linha={ln_i} col={pos_i} coluna={col_nm} cat={categoria} "
                            f"len={len(raw_val)} msg='{msg_clean}' valor='{show_val}'"
                        )

                    if samples_msgs:
                        self.logger.error("[BCP] exemplos: %s", " | ".join(samples_msgs))

            else:
                # fallback: nada casou — mostra cabeçalhos e finais de stdout/stderr
                self.logger.error(
                    "[BCP] code=%s\nSTDOUT(last1k):\n%s\nSTDERR(last1k):\n%s\nERR(head):\n%s\nOUT(head):\n%s",
                    proc.returncode, (proc.stdout or "")[-1000:], (proc.stderr or "")[-1000:], err_head, out_head
                )
            
            # limpeza dos temporários **depois** do diagnóstico
            try: os.remove(tmp_path)
            except: pass
            try: os.remove(err_path)
            except: pass
            try: os.remove(out_path)
            except: pass

            raise RuntimeError(f"BCP falhou com código {proc.returncode} em {schema}.{table}. Veja logs anteriores.")

        else:
            elapsed = time.time() - t0
            rps = int(len(df2) / elapsed) if elapsed > 0 else 0
            self.logger.info(f"BCP → {stg_name}: {elapsed:.2f}s ({len(df2)} linhas, ~{rps}/s)")
            # limpeza em sucesso
            try: os.remove(tmp_path)
            except: pass
            try: os.remove(err_path)
            except: pass
            try: os.remove(out_path)
            except: pass


        return stg_name

    def _merge(
        self,
        cur: pyodbc.Cursor,
        schema: str,
        table: str,
        key_cols: List[str],
        all_cols: List[str],
        stg_name: str,
        *,
        delete_mode: str = "none",                 # "none" | "strict" | "window"
        date_col: Optional[str] = None,            # quando delete_mode="window"
        min_date: Optional[str] = None,            # "YYYY-MM-DD"
        max_date: Optional[str] = None,            # "YYYY-MM-DD"
        allow_empty_delete: bool = False,
        log_deleted_keys: bool = False
    ) -> Dict[str, Any]:
        """
        MERGE com auditoria:
        - #merge_audit: action, keys..., B__col (antes), A__col (depois)
        - #merge_log  : action, keys... (compatibilidade com o retorno anterior)
        """
        # Airbag: staging vazia + strict
        cur.execute(f"SELECT TOP (1) 1 FROM tempdb..{_quote_name(stg_name)};")
        stg_has_rows = cur.fetchone() is not None
        if delete_mode == "strict" and not stg_has_rows and not allow_empty_delete:
            raise RuntimeError("Staging vazia com delete_mode='strict'. Use allow_empty_delete=True para forçar.")

        if delete_mode == "window":
            if not (date_col and min_date and max_date):
                raise ValueError("delete_mode='window' exige date_col, min_date e max_date.")

        # Colunas base
        non_audit    = [c for c in all_cols if c.upper() != 'DT_GRAVACAO']
        compare_cols = [c for c in non_audit if c not in key_cols]
        del_cols = [c for c in all_cols if c not in key_cols]

        # Temp tables
        cols_def_keys = ', '.join(f"{_quote_name(c)} NVARCHAR(4000)" for c in key_cols)
        # b_cols_defs = ', '.join(f"{_quote_name('B__'+c)} NVARCHAR(MAX)" for c in compare_cols) or ""
        b_cols_defs = ', '.join(f"{_quote_name('B__'+c)} NVARCHAR(MAX)" for c in del_cols) or ""
        
        a_cols_defs = ', '.join(f"{_quote_name('A__'+c)} NVARCHAR(MAX)" for c in compare_cols) or ""
        extras_defs = ', '.join(filter(None, [b_cols_defs, a_cols_defs]))

        cur.execute("IF OBJECT_ID('tempdb..#merge_audit','U') IS NOT NULL DROP TABLE #merge_audit;")
        create_audit_sql = f"CREATE TABLE #merge_audit(action VARCHAR(10), {cols_def_keys}"
        if extras_defs:
            create_audit_sql += f", {extras_defs}"
        create_audit_sql += ");"
        cur.execute(create_audit_sql)
        cur.connection.commit()

        # Cláusulas
        on_clause = ' AND '.join(f"T.{_quote_name(c)}=S.{_quote_name(c)}" for c in key_cols)

        update_check = ' OR '.join(
            f"(T.{_quote_name(c)} <> S.{_quote_name(c)}) OR "
            f"(T.{_quote_name(c)} IS NULL AND S.{_quote_name(c)} IS NOT NULL) OR "
            f"(T.{_quote_name(c)} IS NOT NULL AND S.{_quote_name(c)} IS NULL)"
            for c in compare_cols
        ) or "1=0"

        set_clause = ', '.join(f"T.{_quote_name(c)} = S.{_quote_name(c)}" for c in compare_cols)
        set_clause = (set_clause + ", ") if set_clause else ""
        set_clause += f"T.DT_GRAVACAO = {self._tz_expr}"

        insert_cols = ', '.join([_quote_name(c) for c in non_audit] + ['DT_GRAVACAO'])
        insert_vals = ', '.join([f"S.{_quote_name(c)}" for c in non_audit] + [self._tz_expr])

        insert_clause = f"""
        WHEN NOT MATCHED BY TARGET THEN
        INSERT ({insert_cols}) VALUES ({insert_vals})
        """

        if delete_mode == "strict":
            delete_clause = "WHEN NOT MATCHED BY SOURCE THEN DELETE"
        elif delete_mode == "window":
            if date_col is None:
                raise ValueError("date_col cannot be None when delete_mode is 'window'.")
            delete_clause = f"""
            WHEN NOT MATCHED BY SOURCE
            AND T.{_quote_name(str(date_col))} BETWEEN '{min_date}' AND '{max_date}'  
            THEN DELETE
            """
        else:
            delete_clause = ""

        # OUTPUT: action + keys + antes/depois
        output_keys = ', '.join(
            f"COALESCE(INSERTED.{_quote_name(c)}, DELETED.{_quote_name(c)}) AS {_quote_name(c)}"
            for c in key_cols
        )
        output_before = ', '.join(
                    f"CAST(DELETED.{_quote_name(c)} AS NVARCHAR(MAX)) AS {_quote_name('B__'+c)}" for c in del_cols
                )
        output_after =  ', '.join(
            f"CAST(INSERTED.{_quote_name(c)} AS NVARCHAR(MAX)) AS {_quote_name('A__'+c)}" for c in compare_cols
        )
        output_all_list = [output_keys] + ([output_before] if output_before else []) + ([output_after] if output_after else [])
        output_all = ', '.join(output_all_list)

        into_keys = ', '.join(_quote_name(c) for c in key_cols)
        into_before = ', '.join(_quote_name('B__'+c) for c in del_cols) or ""
        into_after  = ', '.join(_quote_name('A__'+c) for c in compare_cols) or ""
        into_all_list = [into_keys] + ([into_before] if into_before else []) + ([into_after] if into_after else [])
        into_all = ', '.join(into_all_list)

        merge_sql = f"""
        MERGE INTO {_quote_name(schema)}.{_quote_name(table)} AS T
        USING tempdb..{_quote_name(stg_name)} AS S
        ON {on_clause}
        WHEN MATCHED AND ({update_check}) THEN
        UPDATE SET {set_clause}
        {insert_clause}
        {delete_clause}
        OUTPUT $action, {output_all}
        INTO #merge_audit(action, {into_all});
        """

        try:
            cur.execute(merge_sql); cur.connection.commit()
        except Exception as e:
            sql_preview = " ".join(merge_sql.split())[:240]
            self.logger.error("MERGE falhou em %s.%s: %s | SQL: %s", schema, table, str(e), sql_preview)
            raise

        # Compat: #merge_log baseado em #merge_audit
        cur.execute("IF OBJECT_ID('tempdb..#merge_log','U') IS NOT NULL DROP TABLE #merge_log;")
        cur.execute(f"CREATE TABLE #merge_log(action VARCHAR(10), {cols_def_keys});")
        cur.execute(f"INSERT INTO #merge_log(action, {into_keys}) SELECT action, {into_keys} FROM #merge_audit;")
        cur.connection.commit()

        # Coleta resultados
        cur.execute("SELECT COUNT(*) FROM #merge_log WHERE action='UPDATE';")
        updated = cur.fetchone()[0] or 0 # type: ignore
        cur.execute("SELECT COUNT(*) FROM #merge_log WHERE action='INSERT';")
        inserted = cur.fetchone()[0] or 0 # type: ignore
        cur.execute("SELECT COUNT(*) FROM #merge_log WHERE action='DELETE';")
        deleted = cur.fetchone()[0] or 0 # type: ignore

        cur.execute(f"SELECT action, {into_keys} FROM #merge_log;")
        rows = cur.fetchall()
        cur.execute("DROP TABLE #merge_log;"); cur.connection.commit()

        keys = [tuple(r[1:]) if len(r) > 2 else r[1] for r in rows]
        deleted_keys = []
        if log_deleted_keys and rows:
            for r in rows:
                if r[0] == 'DELETE':
                    k = tuple(r[1:]) if len(r) > 2 else r[1]
                    deleted_keys.append(k)

        self.logger.info(
            f"[MERGE {delete_mode.upper()}] {schema}.{table}: "
            f"upd={updated}, ins={inserted}, del={deleted}; "
            f"janela={date_col or '-'}:[{min_date or '-'}..{max_date or '-'}]"
        )

        audit_logger = self.audit
        if audit_logger:
            audit_logger.info(
                "MERGE %s.%s | run_id=%s | upd=%d | ins=%d | del=%d | delete_mode=%s | janela=%s:[%s..%s]",
                schema, table,
                getattr(self, "_last_run_id", ""),
                updated, inserted, deleted,
                delete_mode,
                (date_col or "-"), (min_date or "-"), (max_date or "-")
            )

        return {
            'updated': updated,
            'inserted': inserted,
            'deleted': deleted,
            'keys': keys,
            'deleted_keys': deleted_keys
        }

    def _replace_window(
        self,
        *,
        conn: pyodbc.Connection,
        cur: pyodbc.Cursor,
        schema: str,
        table: str,
        all_cols: List[str],
        stg_name: str,
        date_col: str,
        min_date: str,
        max_date: str,
        audit_dir: Optional[str],
        audit_set: set,
        run_id: str,
    ) -> Dict[str, Any]:
        """
        Substitui os dados do intervalo [min_date, max_date] por aquilo que veio na staging:
        1) (Opcional) Audita as linhas a deletar (snapshot completo) se 'delete' estiver habilitado.
        2) DELETE do destino para o intervalo.
        3) INSERT de tudo da staging para o destino (carimbando DT_GRAVACAO).
        Retorna {'updated':0,'inserted':X,'deleted':Y,'keys':[],'deleted_keys':[]}
        """
        # 0) Preparos
        non_audit = [c for c in all_cols if c.upper() != 'DT_GRAVACAO']
        cols_csv_nograv = ', '.join(_quote_name(c) for c in non_audit)
        date_col_q = _quote_name(date_col)
        tgt_full = f"{_quote_name(schema)}.{_quote_name(table)}"

        deleted_count = 0
        inserted_count = 0

        # 1) Auditoria de DELETE (snapshot completo) — antes de excluir
        if audit_dir and ('delete' in audit_set):
            try:
                paths = self._audit_paths(audit_dir, schema, table)
                cur.execute(
                    f"SELECT {', '.join(_quote_name(c) for c in all_cols)} "
                    f"FROM {tgt_full} "
                    f"WHERE {date_col_q} BETWEEN ? AND ?;",
                    (min_date, max_date)
                )
                colnames = [d[0] for d in cur.description]
                rows = cur.fetchall()

                now_iso = datetime.utcnow().isoformat(timespec="milliseconds") + "Z"
                del_lines = []
                for row in rows:
                    snap = {col: ("" if v is None else str(v)) for col, v in zip(colnames, row)}
                    payload = {
                        "run_id": run_id,
                        "ts": now_iso,
                        "action": "DELETE",
                        "deleted_row": snap
                    }
                    del_lines.append(json.dumps(payload, ensure_ascii=False) + "\n")

                self._audit_write_lines(paths["deletes"], del_lines)
                self.logger.info("Auditoria (replace_window) DELETE: %d linhas → %s", len(del_lines), str(paths["deletes"]))
            except Exception as e:
                self.logger.warning("Falha ao auditar deletes (replace_window) em %s.%s: %s", schema, table, e)

        # 2) DELETE
        cur.execute(
            f"DELETE FROM {tgt_full} WHERE {date_col_q} BETWEEN ? AND ?;",
            (min_date, max_date)
        )
        # rowcount pode ser -1 dependendo do driver; então confirmo por @@ROWCOUNT
        deleted_count = cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else 0
        cur.execute("SELECT @@ROWCOUNT;")
        rc = cur.fetchone()
        if rc and isinstance(rc[0], (int,)):
            deleted_count = rc[0]
        conn.commit()

        # 3) INSERT (da staging) — carimba DT_GRAVACAO
        cur.execute(
            f"INSERT INTO {tgt_full} ({cols_csv_nograv}, DT_GRAVACAO) "
            f"SELECT {cols_csv_nograv}, {self._tz_expr} "
            f"FROM tempdb..{_quote_name(stg_name)};"
        )
        inserted_count = cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else 0
        cur.execute("SELECT @@ROWCOUNT;")
        rc = cur.fetchone()
        if rc and isinstance(rc[0], (int,)):
            inserted_count = rc[0]
        conn.commit()

        # 4) Auditoria de INSERT (se habilitada) — por padrão você não audita insert, mas o modo respeita
        if audit_dir and ('insert' in audit_set):
            try:
                paths = self._audit_paths(audit_dir, schema, table)
                now_iso = datetime.utcnow().isoformat(timespec="milliseconds") + "Z"
                # Como não há PK, registramos apenas um “marcador” do refresh
                payload = {
                    "run_id": run_id,
                    "ts": now_iso,
                    "action": "INSERT",
                    "note": f"replace_window [{min_date}..{max_date}] - {inserted_count} linhas inseridas"
                }
                self._audit_write_lines(paths["inserts"], [json.dumps(payload, ensure_ascii=False) + "\n"])
                self.logger.info("Auditoria (replace_window) INSERT: %d linhas → %s", inserted_count, str(paths["inserts"]))
            except Exception as e:
                self.logger.warning("Falha ao auditar inserts (replace_window) em %s.%s: %s", schema, table, e)

        # 5) Log resumo
        self.logger.info(
            "[REPLACE_WINDOW] %s.%s: del=%d, ins=%d, janela=%s:[%s..%s]",
            schema, table, deleted_count, inserted_count, date_col, min_date, max_date
        )

        audit_logger = self.audit
        if audit_logger:
            audit_logger.info(
                "REPLACE_WINDOW %s.%s | run_id=%s | del=%d | ins=%d | janela=%s:[%s..%s]",
                schema, table, run_id, deleted_count, inserted_count, date_col, min_date, max_date
            )

        return {
            "updated": 0,
            "inserted": inserted_count,
            "deleted": deleted_count,
            "keys": [],
            "deleted_keys": []
        }

    def _replace_all(
            self,
            *,
            conn: pyodbc.Connection,
            cur: pyodbc.Cursor,
            schema: str,
            table: str,
            all_cols: List[str],
            stg_name: str,
            audit_dir: Optional[str],
            audit_set: set,
            run_id: str,
            strategy: str = "auto",   # "auto" | "truncate" | "delete"
        ) -> Dict[str, Any]:
            """
            Substitui TODO o conteúdo da tabela pelo que veio na staging:
            1) (Opcional) Audita o snapshot antes de apagar (se 'delete' estiver habilitado).
            2) TRUNCATE TABLE (mais rápido) ou DELETE (fallback).
            3) INSERT de tudo da staging no destino, carimbando DT_GRAVACAO.
            Retorna {'updated':0,'inserted':X,'deleted':Y,'keys':[],'deleted_keys':[]}.
            """
            tgt_full = f"{_quote_name(schema)}.{_quote_name(table)}"
            non_audit = [c for c in all_cols if c.upper() != 'DT_GRAVACAO']
            cols_csv_nograv = ', '.join(_quote_name(c) for c in non_audit)

            # (a) Contagem prévia (útil para reportar deleted_count quando TRUNCATE)
            prev_count = 0
            try:
                cur.execute(f"SELECT COUNT_BIG(1) FROM {tgt_full};")
                prev_count = int(cur.fetchone()[0] or 0) # type: ignore
            except Exception:
                pass

            # (b) Auditoria de DELETE (snapshot completo) — pode ser pesada em tabelas grandes!
            if audit_dir and ('delete' in audit_set):
                try:
                    paths = self._audit_paths(audit_dir, schema, table)
                    cur.execute(f"SELECT {', '.join(_quote_name(c) for c in all_cols)} FROM {tgt_full};")
                    colnames = [d[0] for d in cur.description]
                    rows = cur.fetchall()

                    now_iso = datetime.utcnow().isoformat(timespec="milliseconds") + "Z"
                    del_lines: List[str] = []
                    for row in rows:
                        snap = {col: ("" if v is None else str(v)) for col, v in zip(colnames, row)}
                        payload = {"run_id": run_id, "ts": now_iso, "action": "DELETE", "deleted_row": snap}
                        del_lines.append(json.dumps(payload, ensure_ascii=False) + "\n")

                    self._audit_write_lines(paths["deletes"], del_lines)
                    self.logger.info("Auditoria (replace_all) DELETE: %d linhas → %s", len(del_lines), str(paths["deletes"]))
                except Exception as e:
                    self.logger.warning("Falha ao auditar deletes (replace_all) em %s.%s: %s", schema, table, e)

            # (c) TRUNCATE (rápido) ou DELETE (fallback/forçado)
            deleted_count = 0
            truncated = False

            def do_delete():
                nonlocal deleted_count
                cur.execute(f"DELETE FROM {tgt_full};")
                deleted_count = cur.rowcount if (cur.rowcount is not None and cur.rowcount >= 0) else 0
                try:
                    cur.execute("SELECT @@ROWCOUNT;")
                    rc = cur.fetchone()
                    if rc and isinstance(rc[0], (int,)):
                        deleted_count = rc[0]
                except Exception:
                    pass
                conn.commit()

            if strategy == "delete":
                do_delete()
            else:
                try:
                    if strategy in ("auto", "truncate"):
                        cur.execute(f"TRUNCATE TABLE {tgt_full};")
                        conn.commit()
                        truncated = True
                        deleted_count = prev_count  # TRUNCATE não retorna rowcount
                    else:
                        do_delete()
                except Exception as e:
                    self.logger.debug("TRUNCATE falhou (%s.%s): %s — fallback para DELETE.", schema, table, e)
                    do_delete()

            # (d) INSERT (da staging) — carimba DT_GRAVACAO
            cur.execute(
                f"INSERT INTO {tgt_full} ({cols_csv_nograv}, DT_GRAVACAO) "
                f"SELECT {cols_csv_nograv}, {self._tz_expr} FROM tempdb..{_quote_name(stg_name)};"
            )
            inserted_count = cur.rowcount if (cur.rowcount is not None and cur.rowcount >= 0) else 0
            try:
                cur.execute("SELECT @@ROWCOUNT;")
                rc = cur.fetchone()
                if rc and isinstance(rc[0], (int,)):
                    inserted_count = rc[0]
            except Exception:
                pass
            conn.commit()

            # (e) Auditoria de INSERT (marcador)
            if audit_dir and ('insert' in audit_set):
                try:
                    paths = self._audit_paths(audit_dir, schema, table)
                    now_iso = datetime.utcnow().isoformat(timespec="milliseconds") + "Z"
                    payload = {
                        "run_id": run_id, "ts": now_iso, "action": "INSERT",
                        "note": f"replace_all - {inserted_count} linhas inseridas",
                        "truncated": truncated
                    }
                    self._audit_write_lines(paths["inserts"], [json.dumps(payload, ensure_ascii=False) + "\n"])
                    self.logger.info("Auditoria (replace_all) INSERT: %d linhas → %s", inserted_count, str(paths["inserts"]))
                except Exception as e:
                    self.logger.warning("Falha ao auditar inserts (replace_all) em %s.%s: %s", schema, table, e)

            # (f) Log resumo
            self.logger.info("[REPLACE_ALL] %s.%s: del=%d (truncated=%s), ins=%d",
                            schema, table, deleted_count, truncated, inserted_count)

            audit_logger = self.audit
            if audit_logger:
                audit_logger.info(
                    "REPLACE_ALL %s.%s | run_id=%s | del=%d (truncated=%s) | ins=%d",
                    schema, table, run_id, deleted_count, truncated, inserted_count
                )

            return {"updated": 0, "inserted": inserted_count, "deleted": deleted_count, "keys": [], "deleted_keys": []}

    def _rebuild_indexes(self, cur: pyodbc.Cursor, schema: str, table: str) -> None:
        """
        Rebuild de índices com MAXDOP=4 e SORT_IN_TEMPDB=ON.
        """
        t0 = time.time()
        rebuild_sql = f"""
        DECLARE @sql NVARCHAR(MAX)=N'';
        SELECT @sql +=
          'ALTER INDEX ['+i.name+'] ON [{schema}].[{table}] REBUILD'
          + CASE WHEN i.type_desc LIKE '%COLUMNSTORE%' THEN ' WITH (MAXDOP=4);'
                 ELSE ' WITH (SORT_IN_TEMPDB=ON,MAXDOP=4);' END + CHAR(13)
        FROM sys.indexes i
        JOIN sys.tables t ON i.object_id=t.object_id
        JOIN sys.schemas s ON t.schema_id=s.schema_id
        WHERE t.name='{table}' AND s.name='{schema}';
        EXEC sp_executesql @sql;
        """
        cur.execute(rebuild_sql)
        cur.connection.commit()
        elapsed = time.time() - t0
        self.logger.info(f"Rebuild índices → {schema}.{table}: {elapsed:.2f}s")
  
    def exec_procedure(self, procedure):
        conn = None
        cursor = None
        try:
            conn = self.connect_pyodbc()
            cursor = conn.cursor()
            cursor.execute(f"EXEC {procedure}")
            conn.commit()
            self.logger.info(f"Procedure '{procedure}' executada com sucesso.")
        except Exception as e:
            self.logger.info(f"Falha na operação! {e}")
        finally:
            try:
                if cursor: cursor.close()
            except Exception:
                pass
            try:
                if conn: conn.close()
            except Exception:
                pass

    def sync(
        self,
        *,
        df: pd.DataFrame,
        schema: str,
        table: str,
        mode: str = "upsert",
                     
        key_cols: Optional[List[str]] = None,

        # Janela de delete (se usar upsert_delete_window)
        date_col: Optional[str] = None,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None,

        allow_empty_delete: bool = False,
        log_deleted_keys: bool = False,

        # Auditoria externa
        audit_dir: Optional[str] = None,   # se informado, habilita escrita
        audit: Union[str, List[str], set, tuple, None] = ('update','delete'),  # quais ações auditar
        replace_all_strategy: str = "auto",  # "auto" | "truncate" | "delete"
        chunk_size: int = 5000
    ) -> Dict[str, Any]:
        """
        Função única para realizar as interações de carga ao DW.

        Parâmetros
        ----------
        - mode : str  
            Define o modo de operação. Pode assumir os valores listados abaixo.

        Modos disponíveis
        -----------------
        **Replaces** (substituem dados sem MERGE — executam `DELETE`/`TRUNCATE` + `INSERT`):

        - `replace_all`  
        Remove todas as linhas da tabela de destino e insere todos os registros do DataFrame.  
        (Tenta `TRUNCATE`; se não for possível, cai para `DELETE`.)

        - `replace_window`  
        Remove e reinsere todos os registros apenas dentro de uma janela de tempo definida  
        (parâmetros obrigatórios: `date_col`, `min_date`, `max_date`).

        **Merges** (usam `MERGE` para combinar registros: INSERT, UPDATE e DELETE):  
        Para este modo, `key_cols` é opcional; se não informado, usa a PK da tabela.

        - `upsert`  
        Insere novos registros e atualiza registros existentes.

        - `upsert_delete_all`  
        Faz o `upsert` e remove da tabela todos os registros que não estão no DataFrame fonte.

        - `upsert_delete_window`  
        Faz o `upsert` e remove registros fora da janela de tempo especificada  
        (parâmetros obrigatórios: `date_col`, `min_date`, `max_date`).

        Logs de Auditoria
        -----------------
        Se `audit_dir` for informado, a função escreve arquivos NDJSON com detalhes das operações:

        - `insert` → Marca quando houve inserções (sem detalhar linhas).
        - `update` → Detalhes de cada linha atualizada (antes e depois).
        - `delete` → Snapshot completo de cada linha deletada.
        - `all` → Ativa todas as auditorias acima.

        Retorno
        -------
        Dict[str, Any]  
            Dicionário com informações sobre a execução  
            (contagens de inserts, updates, deletes, etc.).
        """        
        self.logger.info(f"Colunas do DataFrame: {list(df.columns)} , Total de linhas: {len(df)}")

        mode = (mode or "upsert").lower().strip()
        conn, cur, all_cols, pk_cols_db = self._introspect(schema, table)
        stg = None
        run_id = self._mk_run_id()
        
        self.audit = None
        run_id = self._mk_run_id()
        self._last_run_id = run_id

        caller = inspect.stack()[1].function if len(inspect.stack()) > 1 else "sync"
        if audit_dir:
            self.audit = get_daily_path_logger_adapter(
                log_dir=audit_dir,
                base_name="audit",
                level="INFO",
                backup_count=90,
                utc=False,
                job=caller
            )

        audit_set = self._norm_audit(audit)
        want_ins = 'insert' in audit_set
        want_upd = 'update' in audit_set
        want_del = 'delete' in audit_set
        

        try:
            # Resolve chaves da busca (seja tabela pk ou seja key_cols passada na função)
            if mode in ('replace_window', 'replace_all'):
                keys = []  # replaces não usam chave
            else:
                if key_cols:
                    keys = [c.upper() for c in key_cols]
                else:
                    if not pk_cols_db:
                        raise ValueError(
                            f"{schema}.{table} não possui PK e 'key_cols' não foi informado (modo {mode})."
                        )
                    keys = pk_cols_db
                
            # Deriva janela se aplicável
            if mode in ("upsert_delete_window", "replace_window"):
                if not date_col:
                    raise ValueError("Para mode='upsert_delete_window', informe 'date_col'.")
                df2 = df.copy()
                df2[date_col] = pd.to_datetime(df2[date_col], errors='coerce')
                if df2[date_col].isna().all():
                    raise ValueError(f"Coluna {date_col} não contém datas válidas no DataFrame.")
                min_d = min_date or df2[date_col].min().strftime('%Y-%m-%d')
                max_d = max_date or df2[date_col].max().strftime('%Y-%m-%d')
            else:
                df2 = df
                min_d = min_date
                max_d = max_date

            # --- Staging

            stg = self._prepare_and_load(
                cur, df2, all_cols, schema, table, chunk_size,
                load_cols=all_cols, suffix=(
                    'ra' if mode=='replace_all' else
                    'rw' if mode=='replace_window' else
                    'up' if mode=='upsert' else
                    ('ua' if mode=='upsert_delete_all' else 'uw')
                )
            )

            # Decide e executa por modo:
            # - replace_* retornam cedo (têm auditoria própria)
            # - modos MERGE seguem para a auditoria #merge_audit logo abaixo
            if mode in ("replace_window", "replace_all"):
                if mode == "replace_window":
                    # DELETE + INSERT no intervalo, sem MERGE
                    res = self._replace_window(
                        conn=conn, cur=cur,
                        schema=schema, table=table, all_cols=all_cols,
                        stg_name=stg, date_col=date_col, min_date=min_d, max_date=max_d,  # type: ignore
                        audit_dir=audit_dir,
                        audit_set=audit_set,
                        run_id=run_id
                    )
                else:  # mode == "replace_all"
                    # TRUNCATE (ou DELETE fallback) + INSERT de tudo, sem MERGE
                    res = self._replace_all(
                        conn=conn, cur=cur,
                        schema=schema, table=table, all_cols=all_cols,
                        stg_name=stg,
                        audit_dir=audit_dir,
                        audit_set=audit_set,
                        run_id=run_id,
                        strategy=replace_all_strategy
                    )

                changed = (res.get('inserted', 0) or 0) + (res.get('deleted', 0) or 0)
                if changed > 0:
                    self._rebuild_indexes(cur, schema, table)
                    cur.connection.commit()
                return res

            # Modos baseados em MERGE (continuam para auditoria externa abaixo)
            if mode == "upsert":
                delete_mode = "none"
            elif mode == "upsert_delete_all":
                delete_mode = "strict"
            elif mode == "upsert_delete_window":
                delete_mode = "window"
            else:
                raise ValueError(
                    "mode deve ser 'upsert', 'upsert_delete_all', 'upsert_delete_window', 'replace_window' ou 'replace_all'."
                )

            res = self._merge(
                cur, schema, table, keys, all_cols, stg,
                delete_mode=delete_mode,
                date_col=date_col if delete_mode == 'window' else None,
                min_date=min_d if delete_mode == 'window' else None,
                max_date=max_d if delete_mode == 'window' else None,
                allow_empty_delete=allow_empty_delete,
                log_deleted_keys=log_deleted_keys
            )

            # ---------- Auditoria externa: ler #merge_audit e escrever NDJSON (detalhado) ----------
            # ---------- Auditoria externa: ler #merge_audit e escrever NDJSON (detalhado) ----------
            if audit_dir:
                try:
                    paths = self._audit_paths(audit_dir, schema, table)

                    # Definições de colunas
                    non_audit = [c for c in all_cols if c.upper() != 'DT_GRAVACAO']
                    compare_cols = [c for c in non_audit if c not in keys]
                    # Snapshot completo para DELETE: TODAS as não-chave (inclui DT_GRAVACAO)
                    del_cols = [c for c in all_cols if c not in keys]

                    keys_csv = ', '.join(_quote_name(c) for c in keys)
                    now_iso = datetime.utcnow().isoformat(timespec="milliseconds") + "Z"

                    ins_lines: List[str] = []
                    del_lines: List[str] = []
                    upd_lines: List[str] = []

                    # (opcional) liberar memória da temp table para ações desabilitadas
                    to_prune = []
                    if not want_ins: to_prune.append('INSERT')
                    if not want_upd: to_prune.append('UPDATE')
                    if not want_del: to_prune.append('DELETE')
                    if to_prune:
                        try:
                            qmarks = ','.join('?' for _ in to_prune)
                            cur.execute(f"DELETE FROM #merge_audit WHERE action IN ({qmarks});", to_prune)
                            cur.connection.commit()
                        except Exception:
                            pass

                    # --- DELETE (snapshot completo) ---
                    if want_del:
                        b_cols_delete = ', '.join(_quote_name('B__'+c) for c in del_cols) or ""
                        select_del = ', '.join(filter(None, ['action', keys_csv, b_cols_delete]))
                        cur.execute(f"SELECT {select_del} FROM #merge_audit WHERE action='DELETE';")
                        del_colnames = [d[0] for d in cur.description]
                        for row in cur.fetchall():
                            rec = dict(zip(del_colnames, row))
                            key_dict = {k: ("" if rec.get(k) is None else str(rec.get(k))) for k in keys}
                            key_concat = self._concat_keys(keys, key_dict)

                            deleted_row: Dict[str, Any] = {}
                            # PKs
                            for k in keys:
                                deleted_row[k] = key_dict[k]
                            # Demais colunas (valores "antes")
                            for c in del_cols:
                                val = rec.get("B__" + c)
                                deleted_row[c] = "" if val is None else str(val)

                            payload = {
                                "run_id": run_id,
                                "ts": now_iso,
                                "action": "DELETE",
                                "keys": key_dict,
                                "key_concat": key_concat,
                                "deleted_row": deleted_row
                            }
                            del_lines.append(json.dumps(payload, ensure_ascii=False) + "\n")

                    # --- UPDATE (diff por coluna) ---
                    if want_upd:
                        b_cols_upd = ', '.join(_quote_name('B__'+c) for c in compare_cols) or ""
                        a_cols_upd = ', '.join(_quote_name('A__'+c) for c in compare_cols) or ""
                        select_upd = ', '.join(filter(None, ['action', keys_csv, b_cols_upd, a_cols_upd]))
                        cur.execute(f"SELECT {select_upd} FROM #merge_audit WHERE action='UPDATE';")
                        upd_colnames = [d[0] for d in cur.description]
                        for row in cur.fetchall():
                            rec = dict(zip(upd_colnames, row))
                            key_dict = {k: ("" if rec.get(k) is None else str(rec.get(k))) for k in keys}
                            key_concat = self._concat_keys(keys, key_dict)

                            diff = {}
                            for c in compare_cols:
                                sb = "" if rec.get("B__" + c) is None else str(rec.get("B__" + c))
                                sa = "" if rec.get("A__" + c) is None else str(rec.get("A__" + c))
                                if sb != sa:
                                    diff[c] = {"before": sb, "after": sa}

                            if diff:
                                payload = {
                                    "run_id": run_id,
                                    "ts": now_iso,
                                    "action": "UPDATE",
                                    "keys": key_dict,
                                    "key_concat": key_concat,
                                    "diff": diff
                                }
                                upd_lines.append(json.dumps(payload, ensure_ascii=False) + "\n")

                    # --- INSERT (apenas chaves) ---
                    if want_ins and keys:
                        now_iso = datetime.utcnow().isoformat(timespec="milliseconds") + "Z"

                        # chaves para o JOIN A(merge_audit) ↔ S(staging)
                        join_on_stg = ' AND '.join(f"A.{_quote_name(k)} = S.{_quote_name(k)}" for k in keys)

                        # todas as colunas exceto DT_GRAVACAO (staging não tem essa coluna)
                        non_audit = [c for c in all_cols if c.upper() != 'DT_GRAVACAO']

                        # seleciona: chaves (da #merge_audit) + todas as colunas "não-auditoria" da staging
                        sel_cols = ', '.join(
                            [f"A.{_quote_name(k)} AS {_quote_name(k)}" for k in keys] +
                            [f"S.{_quote_name(c)} AS {_quote_name(c)}" for c in non_audit]
                        )

                        cur.execute(f"""
                            SELECT {sel_cols}
                            FROM #merge_audit AS A
                            JOIN tempdb..{_quote_name(stg)} AS S
                            ON {join_on_stg}
                            WHERE A.action = 'INSERT';
                        """)
                        colnames = [d[0] for d in cur.description]

                        for row in cur.fetchall():
                            rec = dict(zip(colnames, row))

                            key_dict = {k: ("" if rec.get(k) is None else str(rec.get(k))) for k in keys}
                            key_concat = self._concat_keys(keys, key_dict)

                            # linha inserida: chaves + todas as colunas não-auditoria (sem DT_GRAVACAO)
                            inserted_row = {}
                            for c in keys + non_audit:
                                val = rec.get(c)
                                inserted_row[c] = "" if val is None else str(val)

                            payload = {
                                "run_id": run_id,
                                "ts": now_iso,
                                "action": "INSERT",
                                "keys": key_dict,
                                "key_concat": key_concat,
                                "inserted_row": inserted_row,
                                "source": "staging"
                            }
                            ins_lines.append(json.dumps(payload, ensure_ascii=False) + "\n")

                    # Escrita (append)
                    if want_del: self._audit_write_lines(paths["deletes"], del_lines)
                    if want_upd: self._audit_write_lines(paths["updates"], upd_lines)
                    if want_ins: self._audit_write_lines(paths["inserts"], ins_lines)

                    # Log resumo
                    parts = []
                    if want_upd: parts.append(f"upd={len(upd_lines)} → {paths['updates']}")
                    if want_del: parts.append(f"del={len(del_lines)} → {paths['deletes']}")
                    if want_ins: parts.append(f"ins={len(ins_lines)} → {paths['inserts']}")
                    self.logger.info("Auditoria externa escrita (%s) | run_id=%s", " ; ".join(parts) or "sem ações", run_id)

                    audit_logger = self.audit
                    if audit_logger:
                        audit_logger.info(
                            "AUDIT %s.%s | run_id=%s | updates=%d | deletes=%d | inserts=%d",
                            schema, table, run_id, len(upd_lines), len(del_lines), len(ins_lines)
                        )
                        try:
                            import json as _json
                            def _sample(lines, n=3):
                                out = []
                                for l in lines[:n]:
                                    try:
                                        o = _json.loads(l)
                                        out.append(o.get("key_concat") or o.get("keys"))
                                    except Exception:
                                        out.append("<?>")
                                return out
                            uk = _sample(upd_lines); dk = _sample(del_lines); ik = _sample(ins_lines)
                            if uk: audit_logger.debug("UPD sample keys %s.%s: %s", schema, table, uk)
                            if dk: audit_logger.debug("DEL sample keys %s.%s: %s", schema, table, dk)
                            if ik: audit_logger.debug("INS sample keys %s.%s: %s", schema, table, ik)
                        except Exception:
                            pass

                except Exception as e:
                    self.logger.warning("Falha ao escrever auditoria externa (%s.%s): %s", schema, table, e)


            # pós-merge
            self._rebuild_indexes(cur, schema, table)
            cur.connection.commit()
            return res

        finally:
            try:
                if stg:
                    cur.execute(
                        f"IF OBJECT_ID(N'tempdb..{stg}','U') IS NOT NULL DROP TABLE tempdb..{_quote_name(stg)};"
                    ); cur.connection.commit()
            except Exception:
                pass
            try: cur.close()
            except: pass
            try: conn.close()
            except: pass
            # (opcional) resetar contexto do job
            # try:
            #     if job_token is not None: job_ctx.reset(job_token)
            # except Exception:
            #     pass
    
    def select(self, query):
        conn = self.connect_pyodbc()
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            resultado = cursor.fetchall()
            columns = [column[0] for column in cursor.description]
            df = pd.DataFrame.from_records(resultado, columns=columns)
            return df
        finally:
            try:
                cursor.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass 

    def insert_raw_data(self, dados, tabela):
        # dados = dados.fillna('')
        # dados = dados.replace("'", "", regex=True)
        try:
            conn = self.connect_pyodbc()
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {tabela}")
            num_linhas_anterior = cursor.fetchone()[0]  # type: ignore
            columns = ', '.join([f'[{col}]' for col in dados.columns])
            chunk_size = 1000
            for start in range(0, len(dados), chunk_size):
                chunk = dados.iloc[start:start + chunk_size]
                insert_sql = f"INSERT INTO {tabela} ({columns}) VALUES "
                values_list = []
                for _, row in chunk.iterrows():
                    values = ', '.join([f"'{str(value)}'" for value in row.values])
                    values_list.append(f"({values})")
                insert_sql += ', '.join(values_list)
                cursor.execute(insert_sql)
            conn.commit()
            cursor.execute(f"SELECT COUNT(*) FROM {tabela}")
            num_linhas_apos_insercao = cursor.fetchone()[0]  # type: ignore
            num_linhas_inseridas = num_linhas_apos_insercao - num_linhas_anterior

            self.logger.info(f"{num_linhas_inseridas} linhas foram inseridas no banco.")
        except Exception as e:
            self.logger.error(f"Falha na operação! {e}")
        finally:
            cursor.close()  # type: ignore
            conn.close()  # type: ignore
        return

class DW_Treviso_Tuning(SQLServerTuning):
    def __init__(self):
        super().__init__()

class DB2Database:
    def __init__(self,config_section):
        config = get_config()
        self.driver = config[config_section]['Driver'] #type: ignore
        self.server = config[config_section]['Server'] #type: ignore
        self.database = config[config_section]['Database'] #type: ignore
        self.username = config[config_section]['User'] #type: ignore
        self.password = config[config_section]['Password'] #type: ignore
        self.port = config[config_section]['Port'] #type: ignore
        self.conn_string = f"Driver={{{self.driver}}};System={self.server};Database={self.database};Uid={self.username};Pwd={self.password};"

    def TestConnection(self):
        try:
            conn = pyodbc.connect(self.conn_string)
            cursor = conn.cursor()
            cursor.execute("SELECT @@version;")
            row = cursor.fetchone()
            print('*' * 150 + '\n')
            print('Host: ', self.server)
            print('Conexão bem sucedida! Versão do banco de dados: ', row[0]) # type: ignore
            print('*' * 150 + '\n')
        except Exception as e:
            print("Falha na conexão!"), # type: ignore
            print('*' * 150 + '\n')
            print(e)
            print('*' * 150 + '\n')
        finally:
            conn.close() # type: ignore
        return 
    
    def select(self, querry):
        # try:
        conn = pyodbc.connect(self.conn_string)
        cursor = conn.cursor()
        cursor.execute(querry)
        resultado = cursor.fetchall()

        columns = [column[0] for column in cursor.description]
        df = pd.DataFrame.from_records(resultado, columns=columns) # type: ignore
        
        # print('*' * 150 + '\n')
        # print('Consulta bem sucedida!') # type: ignore
        # print('*' * 150 + '\n')
        # except Exception as e:
        #     print("Falha na consulta!")
        # finally:
        conn.close() # type: ignore
        return df # type: ignore

class BR_DB2(DB2Database):
    def __init__(self):
        super().__init__('BD_BR_DB2')

class BR_DB2_2(DB2Database):
    def __init__(self):
        super().__init__('BD_BR_DB2_REDUNDANCIA')
