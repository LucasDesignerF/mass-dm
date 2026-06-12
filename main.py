"""
Nexus DM Platform - Enterprise Discord Campaign Engine
Versão: 5.0.1 - Production Ready
Autor: Nexus Platforms
Licença: MIT
"""

import asyncio
import csv
import hashlib
import json
import logging
import os
import subprocess
import sys
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import TypedDict, Optional, List, Dict, Any, AsyncGenerator, Set, cast

import discord
from colorama import init, Fore, Style

try:
    import aiosqlite
except ImportError:
    print("❌ aiosqlite não encontrado. Execute: pip install aiosqlite")
    sys.exit(1)

init(autoreset=True)

# ============================================================================
# CONFIGURAÇÃO DE LOGGING
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('mass_dm.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('NexusDM')

ASCII_LOGO = f"""
{Fore.CYAN}
███╗   ██╗███████╗██╗  ██╗██╗   ██╗███████╗
████╗  ██║██╔════╝╚██╗██╔╝██║   ██║██╔════╝
██╔██╗ ██║█████╗   ╚███╔╝ ██║   ██║███████╗
██║╚██╗██║██╔══╝   ██╔██╗ ██║   ██║╚════██║
██║ ╚████║███████╗██╔╝ ██╗╚██████╔╝███████║
╚═╝  ╚═══╝╚══════╝╚═╝  ╚═╝ ╚═════╝ ╚══════╝

        N E X U S   P L A T F O R M S
{Style.RESET_ALL}
"""

# ============================================================================
# CONSTANTES
# ============================================================================

MESSAGE_FILE = Path(__file__).parent / "message.txt"
DB_FILE = Path(__file__).parent / "mass_dm.db"
CONFIG_FILE = Path(__file__).parent / "config.json"
TEMPLATES_DIR = Path(__file__).parent / "templates"
EXPORTS_DIR = Path(__file__).parent / "exports"

NUM_WORKERS = 3
MAX_WORKERS = 5
MAX_DM_PER_WINDOW = 5
RATE_WINDOW_SECONDS = 5.0
RATE_LIMIT_BACKOFF = 5.0
MAX_RETRIES = 3

SHARD_COUNT = int(os.getenv("SHARD_COUNT", "1"))
SHARD_IDS = [int(s) for s in os.getenv("SHARD_IDS", "0").split(",")] if os.getenv("SHARD_IDS") else None


# ============================================================================
# ENUMS E TIPOS
# ============================================================================

class CampaignStatus(str, Enum):
    DRAFT = "draft"
    ACTIVE = "active"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"


class DeliveryStatus(str, Enum):
    PENDING = "pending"
    SENT = "sent"
    FAILED = "failed"
    RATE_LIMITED = "rate_limited"
    DM_CLOSED = "dm_closed"
    RETRYING = "retrying"


class JoinFilter(str, Enum):
    LAST_24H = "24h"
    LAST_7D = "7d"
    LAST_30D = "30d"
    ALL = "all"


class ConfigDict(TypedDict):
    token: str
    guild_id: str
    role_id: str
    register_channel_id: str
    delay: int
    campaign_name: str
    message_template: str
    auto_save: bool
    num_workers: int
    join_filter: str


class CampaignRow(TypedDict):
    id: str
    name: str
    guild_id: int
    role_id: int
    register_channel_id: int
    message: str
    status: str
    delay: int
    workers: int
    join_filter: str
    total_members: int
    estimated_duration: float
    started_at: Optional[str]
    finished_at: Optional[str]
    created_at: str
    updated_at: str


class DeliveryRow(TypedDict):
    id: str
    campaign_id: str
    user_id: int
    username: str
    message_hash: str
    status: str
    timestamp: str
    retry_count: int
    error: Optional[str]


# ============================================================================
# UTILITÁRIOS
# ============================================================================

def format_duration(seconds: float) -> str:
    """Formata duração em segundos para string legível."""
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        return f"{seconds / 60:.1f}min"
    else:
        hours = int(seconds / 3600)
        minutes = int((seconds % 3600) / 60)
        return f"{hours}h {minutes}min"


def estimate_campaign_duration(total_members: int, delay: int, workers: int) -> float:
    """Calcula duração estimada da campanha em segundos."""
    if workers == 0:
        return 0
    return (total_members / workers) * delay


def render_message_preview(message: str, user: str = "João", server: str = "Nexus Community", register_channel: str = "#registro") -> str:
    """Renderiza uma prévia da mensagem com dados fictícios."""
    return (message
            .replace("{user}", user)
            .replace("{mention}", f"@{user}")
            .replace("{server}", server)
            .replace("{register_channel}", register_channel))


def apply_join_filter(members: List[discord.Member], join_filter: str) -> List[discord.Member]:
    """Aplica filtro por data de entrada nos membros."""
    now = datetime.now(timezone.utc)
    
    if join_filter == JoinFilter.LAST_24H:
        cutoff = now - timedelta(hours=24)
    elif join_filter == JoinFilter.LAST_7D:
        cutoff = now - timedelta(days=7)
    elif join_filter == JoinFilter.LAST_30D:
        cutoff = now - timedelta(days=30)
    else:
        return members  # ALL
    
    return [m for m in members if m.joined_at and m.joined_at.replace(tzinfo=timezone.utc) >= cutoff]


# ============================================================================
# SISTEMA DE CONFIGURAÇÃO PERSISTENTE
# ============================================================================

class ConfigManager:
    """Gerenciador de configuração persistente em JSON."""
    
    def __init__(self, config_path: Path) -> None:
        self.config_path = config_path
        self.data: Dict[str, Any] = {}
        self._load()
    
    def _load(self) -> None:
        if self.config_path.exists():
            try:
                self.data = json.loads(self.config_path.read_text(encoding="utf-8"))
                logger.info("Configuração carregada do disco")
            except Exception:
                self.data = {}
        else:
            self.data = {}
    
    def save(self) -> None:
        try:
            self.config_path.write_text(
                json.dumps(self.data, indent=2, ensure_ascii=False),
                encoding="utf-8"
            )
        except Exception as e:
            logger.error(f"Erro ao salvar configuração: {e}")
    
    def get(self, key: str, default: Any = None) -> Any:
        return self.data.get(key, default)
    
    def set(self, key: str, value: Any) -> None:
        self.data[key] = value


# ============================================================================
# SISTEMA DE TEMPLATES DE MENSAGEM
# ============================================================================

class TemplateManager:
    """Gerenciador de templates de mensagem."""
    
    def __init__(self) -> None:
        TEMPLATES_DIR.mkdir(exist_ok=True)
    
    def list_templates(self) -> List[str]:
        return [f.stem for f in TEMPLATES_DIR.glob("*.txt")]
    
    def load_template(self, name: str) -> Optional[str]:
        template_path = TEMPLATES_DIR / f"{name}.txt"
        if template_path.exists():
            return template_path.read_text(encoding="utf-8").strip()
        return None
    
    def save_template(self, name: str, content: str) -> None:
        (TEMPLATES_DIR / f"{name}.txt").write_text(content, encoding="utf-8")
        logger.info(f"Template '{name}' salvo")
    
    def delete_template(self, name: str) -> bool:
        template_path = TEMPLATES_DIR / f"{name}.txt"
        if template_path.exists():
            template_path.unlink()
            return True
        return False


# ============================================================================
# SISTEMA DE IDEMPOTÊNCIA
# ============================================================================

class IdempotencyManager:
    """Garante que mensagens não sejam duplicadas."""
    
    def __init__(self, db: 'Database') -> None:
        self.db = db
        self.sent_hashes: Dict[str, Set[str]] = {}
        self._lock = asyncio.Lock()
    
    def generate_key(self, user_id: int, campaign_id: str, message: str) -> str:
        content = f"{user_id}:{campaign_id}:{message}"
        return hashlib.sha256(content.encode()).hexdigest()
    
    async def is_duplicate(self, campaign_id: str, idempotency_key: str) -> bool:
        async with self._lock:
            if campaign_id in self.sent_hashes:
                if idempotency_key in self.sent_hashes[campaign_id]:
                    return True
        
        exists_in_db = await self.db.message_hash_exists(idempotency_key)
        if exists_in_db:
            async with self._lock:
                if campaign_id not in self.sent_hashes:
                    self.sent_hashes[campaign_id] = set()
                self.sent_hashes[campaign_id].add(idempotency_key)
            return True
        return False
    
    async def mark_sent(self, campaign_id: str, idempotency_key: str) -> None:
        async with self._lock:
            if campaign_id not in self.sent_hashes:
                self.sent_hashes[campaign_id] = set()
            self.sent_hashes[campaign_id].add(idempotency_key)


# ============================================================================
# BANCO DE DADOS ASYNC - V5 COM CAMPOS EXPANDIDOS
# ============================================================================

class Database:
    """Gerenciador de banco de dados async com schema V5."""
    
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self._db: Optional[aiosqlite.Connection] = None
    
    async def _get_db(self) -> aiosqlite.Connection:
        if self._db is None:
            self._db = await aiosqlite.connect(str(self.db_path))
            await self._db.execute("PRAGMA journal_mode=WAL")
            await self._db.execute("PRAGMA foreign_keys=ON")
            self._db.row_factory = aiosqlite.Row
        return self._db
    
    async def close(self) -> None:
        if self._db is not None:
            await self._db.close()
            self._db = None
    
    async def initialize(self) -> None:
        """Inicializa banco com schema V5 expandido."""
        db = await self._get_db()
        
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS campaigns (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                guild_id INTEGER NOT NULL,
                role_id INTEGER NOT NULL,
                register_channel_id INTEGER DEFAULT 0,
                message TEXT NOT NULL,
                status TEXT DEFAULT 'draft',
                delay INTEGER DEFAULT 15,
                workers INTEGER DEFAULT 3,
                join_filter TEXT DEFAULT 'all',
                total_members INTEGER DEFAULT 0,
                estimated_duration REAL DEFAULT 0,
                started_at TIMESTAMP,
                finished_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE TABLE IF NOT EXISTS deliveries (
                id TEXT PRIMARY KEY,
                campaign_id TEXT NOT NULL,
                user_id INTEGER NOT NULL,
                username TEXT NOT NULL,
                message_hash TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                retry_count INTEGER DEFAULT 0,
                error TEXT,
                FOREIGN KEY (campaign_id) REFERENCES campaigns(id) ON DELETE CASCADE
            );
            
            CREATE INDEX IF NOT EXISTS idx_deliveries_campaign ON deliveries(campaign_id, user_id);
            CREATE INDEX IF NOT EXISTS idx_deliveries_status ON deliveries(status);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_message_hash ON deliveries(message_hash);
        """)
        
        await self._migrate_v4_to_v5(db)
        await db.commit()
        logger.info("Banco de dados V5 inicializado")
    
    async def _migrate_v4_to_v5(self, db: aiosqlite.Connection) -> None:
        """Migra schema v4 para v5 sem perder dados."""
        v5_columns = {
            "register_channel_id": "INTEGER DEFAULT 0",
            "workers": "INTEGER DEFAULT 3",
            "join_filter": "TEXT DEFAULT 'all'",
            "estimated_duration": "REAL DEFAULT 0",
            "started_at": "TIMESTAMP",
            "finished_at": "TIMESTAMP"
        }
        
        for col_name, col_def in v5_columns.items():
            try:
                await db.execute(f"ALTER TABLE campaigns ADD COLUMN {col_name} {col_def}")
            except aiosqlite.OperationalError:
                pass
    
    @asynccontextmanager
    async def atomic_write(self) -> AsyncGenerator[aiosqlite.Connection, None]:
        db = await self._get_db()
        await db.execute("BEGIN IMMEDIATE")
        try:
            yield db
            await db.commit()
        except Exception:
            await db.rollback()
            raise
    
    async def create_campaign(self, campaign_id: str, name: str, guild_id: int,
                             role_id: int, register_channel_id: int, message: str,
                             delay: int, workers: int, join_filter: str,
                             estimated_duration: float) -> None:
        """Cria nova campanha com schema V5."""
        db = await self._get_db()
        await db.execute(
            """INSERT INTO campaigns (id, name, guild_id, role_id, register_channel_id,
               message, delay, workers, join_filter, estimated_duration, status)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (campaign_id, name, guild_id, role_id, register_channel_id,
             message, delay, workers, join_filter, estimated_duration, CampaignStatus.DRAFT)
        )
        await db.commit()
    
    async def get_campaign(self, campaign_id: str) -> Optional[CampaignRow]:
        db = await self._get_db()
        cursor = await db.execute("SELECT * FROM campaigns WHERE id = ?", (campaign_id,))
        row = await cursor.fetchone()
        return cast(CampaignRow, dict(row)) if row else None
    
    async def update_campaign_status(self, campaign_id: str, status: str) -> None:
        db = await self._get_db()
        now = datetime.now(timezone.utc).isoformat()
        if status == CampaignStatus.ACTIVE:
            await db.execute(
                "UPDATE campaigns SET status = ?, started_at = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (status, now, campaign_id)
            )
        elif status in (CampaignStatus.COMPLETED, CampaignStatus.FAILED):
            await db.execute(
                "UPDATE campaigns SET status = ?, finished_at = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (status, now, campaign_id)
            )
        else:
            await db.execute(
                "UPDATE campaigns SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (status, campaign_id)
            )
        await db.commit()
    
    async def update_campaign_total(self, campaign_id: str, total: int) -> None:
        db = await self._get_db()
        await db.execute("UPDATE campaigns SET total_members = ? WHERE id = ?", (total, campaign_id))
        await db.commit()
    
    async def update_campaign_estimated_duration(self, campaign_id: str, estimated_duration: float) -> None:
        """Atualiza duração estimada da campanha. Método público."""
        db = await self._get_db()
        await db.execute(
            "UPDATE campaigns SET estimated_duration = ? WHERE id = ?",
            (estimated_duration, campaign_id)
        )
        await db.commit()
    
    async def get_campaign_stats(self, campaign_id: str) -> Dict[str, int]:
        db = await self._get_db()
        cursor = await db.execute(
            """SELECT COUNT(*) as total,
                   COALESCE(SUM(CASE WHEN status = 'sent' THEN 1 ELSE 0 END), 0) as sent,
                   COALESCE(SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END), 0) as failed,
                   COALESCE(SUM(CASE WHEN status = 'rate_limited' THEN 1 ELSE 0 END), 0) as rate_limited
               FROM deliveries WHERE campaign_id = ?""",
            (campaign_id,)
        )
        row = await cursor.fetchone()
        if row is not None:
            return {
                "total": row[0] if row[0] is not None else 0,
                "sent": row[1] if row[1] is not None else 0,
                "failed": row[2] if row[2] is not None else 0,
                "rate_limited": row[3] if row[3] is not None else 0
            }
        return {"total": 0, "sent": 0, "failed": 0, "rate_limited": 0}
    
    async def get_global_stats(self) -> Dict[str, Any]:
        db = await self._get_db()
        cursor = await db.execute(
            """SELECT COUNT(DISTINCT campaign_id) as total_campaigns,
                   COUNT(*) as total_deliveries,
                   SUM(CASE WHEN status = 'sent' THEN 1 ELSE 0 END) as total_sent,
                   SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as total_failed
               FROM deliveries"""
        )
        row = await cursor.fetchone()
        if row is not None:
            return {
                "total_campaigns": row[0] if row[0] is not None else 0,
                "total_deliveries": row[1] if row[1] is not None else 0,
                "total_sent": row[2] if row[2] is not None else 0,
                "total_failed": row[3] if row[3] is not None else 0
            }
        return {"total_campaigns": 0, "total_deliveries": 0, "total_sent": 0, "total_failed": 0}
    
    async def get_campaign_history(self) -> List[CampaignRow]:
        """Retorna histórico completo de campanhas para auditoria."""
        db = await self._get_db()
        cursor = await db.execute("SELECT * FROM campaigns ORDER BY created_at DESC")
        rows = await cursor.fetchall()
        return [cast(CampaignRow, dict(row)) for row in rows]
    
    async def user_processed_in_campaign(self, user_id: int, campaign_id: str) -> bool:
        db = await self._get_db()
        cursor = await db.execute(
            "SELECT 1 FROM deliveries WHERE user_id = ? AND campaign_id = ? AND status = 'sent'",
            (user_id, campaign_id)
        )
        return await cursor.fetchone() is not None
    
    async def message_hash_exists(self, message_hash: str) -> bool:
        db = await self._get_db()
        cursor = await db.execute(
            "SELECT 1 FROM deliveries WHERE message_hash = ? AND status = 'sent'",
            (message_hash,)
        )
        return await cursor.fetchone() is not None
    
    async def record_delivery(self, delivery_id: str, campaign_id: str, user_id: int,
                             username: str, message_hash: str, status: str,
                             error: Optional[str] = None) -> bool:
        db = await self._get_db()
        try:
            await db.execute(
                """INSERT INTO deliveries (id, campaign_id, user_id, username, message_hash, status, error)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (delivery_id, campaign_id, user_id, username, message_hash, status, error)
            )
            await db.commit()
            return True
        except aiosqlite.IntegrityError:
            return False
    
    async def get_pending_retries(self, campaign_id: str) -> List[DeliveryRow]:
        db = await self._get_db()
        cursor = await db.execute(
            """SELECT * FROM deliveries 
               WHERE campaign_id = ? AND status IN ('pending', 'rate_limited', 'retrying')
               AND retry_count < ?""",
            (campaign_id, MAX_RETRIES)
        )
        rows = await cursor.fetchall()
        return [cast(DeliveryRow, dict(row)) for row in rows]
    
    async def increment_retry(self, delivery_id: str) -> None:
        db = await self._get_db()
        await db.execute(
            """UPDATE deliveries SET retry_count = retry_count + 1, status = 'retrying' WHERE id = ?""",
            (delivery_id,)
        )
        await db.commit()
    
    async def list_campaigns(self, status_filter: Optional[str] = None) -> List[CampaignRow]:
        db = await self._get_db()
        if status_filter:
            cursor = await db.execute(
                "SELECT * FROM campaigns WHERE status = ? ORDER BY created_at DESC",
                (status_filter,)
            )
        else:
            cursor = await db.execute("SELECT * FROM campaigns ORDER BY created_at DESC")
        rows = await cursor.fetchall()
        return [cast(CampaignRow, dict(row)) for row in rows]
    
    async def delete_campaign(self, campaign_id: str) -> bool:
        db = await self._get_db()
        await db.execute("DELETE FROM deliveries WHERE campaign_id = ?", (campaign_id,))
        await db.execute("DELETE FROM campaigns WHERE id = ?", (campaign_id,))
        await db.commit()
        return True
    
    async def export_campaign_csv(self, campaign_id: str) -> Path:
        """Exporta campanha completa para CSV com todos os campos."""
        EXPORTS_DIR.mkdir(exist_ok=True)
        db = await self._get_db()
        
        cursor = await db.execute(
            "SELECT campaign_id, user_id, username, status, timestamp, retry_count, error FROM deliveries WHERE campaign_id = ?",
            (campaign_id,)
        )
        rows = await cursor.fetchall()
        
        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        export_path = EXPORTS_DIR / f"campaign_export_{timestamp_str}.csv"
        
        with open(export_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(["campaign_id", "user_id", "username", "status", "timestamp", "retry_count", "error"])
            for row in rows:
                writer.writerow([row[0], row[1], row[2], row[3], row[4], row[5], row[6] or ""])
        
        logger.info(f"CSV exportado: {export_path}")
        return export_path


# ============================================================================
# RATE LIMITER
# ============================================================================

class RateLimiter:
    def __init__(self, max_requests: int = MAX_DM_PER_WINDOW, window: float = RATE_WINDOW_SECONDS) -> None:
        self.max_requests = max_requests
        self.window = window
        self.timestamps: List[float] = []
        self._lock = asyncio.Lock()
    
    async def acquire(self) -> float:
        async with self._lock:
            now = time.time()
            self.timestamps = [t for t in self.timestamps if now - t < self.window]
            wait_time = 0.0
            if len(self.timestamps) >= self.max_requests:
                wait_time = self.timestamps[0] + self.window - now
                if wait_time < 0:
                    wait_time = 0.0
            if wait_time == 0.0:
                self.timestamps.append(time.time())
        if wait_time > 0:
            await asyncio.sleep(wait_time)
            return await self.acquire()
        return wait_time


# ============================================================================
# WORKER SYSTEM
# ============================================================================

@dataclass
class WorkItem:
    member: discord.Member
    campaign_id: str
    message: str
    guild_name: str
    register_channel_mention: str
    idempotency_key: str


class WorkerPool:
    def __init__(self, db: Database, client: discord.Client, 
                 rate_limiter: RateLimiter, idempotency: IdempotencyManager,
                 num_workers: int = NUM_WORKERS) -> None:
        self.db = db
        self.client = client
        self.rate_limiter = rate_limiter
        self.idempotency = idempotency
        self.num_workers = num_workers
        self.queue: asyncio.Queue[WorkItem] = asyncio.Queue()
        self.workers: List[asyncio.Task[None]] = []
        self.is_running = False
        self._stats_lock = asyncio.Lock()
        self.processed_count = 0
        self.success_count = 0
        self.failed_count = 0
        self._all_items_added = asyncio.Event()
        self.start_time: Optional[float] = None
    
    async def start(self) -> None:
        self.is_running = True
        self.start_time = time.time()
        self.workers = [asyncio.create_task(self._worker(i), name=f"worker-{i}") for i in range(self.num_workers)]
        logger.info(f"WorkerPool iniciado com {self.num_workers} workers")
    
    async def stop(self) -> None:
        self._all_items_added.set()
        remaining = self.queue.qsize()
        if remaining > 0:
            logger.info(f"Aguardando {remaining} itens...")
        await self.queue.join()
        self.is_running = False
        for worker in self.workers:
            worker.cancel()
        await asyncio.gather(*self.workers, return_exceptions=True)
        elapsed = time.time() - (self.start_time or time.time())
        logger.info(f"WorkerPool parado. {self.processed_count} processados em {elapsed:.1f}s")
    
    async def add_work(self, item: WorkItem) -> None:
        await self.queue.put(item)
    
    def mark_all_added(self) -> None:
        self._all_items_added.set()
    
    def is_all_items_added(self) -> bool:
        return self._all_items_added.is_set()
    
    def get_eta(self) -> float:
        if self.processed_count == 0:
            return 0
        remaining = self.queue.qsize()
        elapsed = time.time() - (self.start_time or time.time())
        rate = self.processed_count / elapsed if elapsed > 0 else 0
        return remaining / rate if rate > 0 else 0
    
    def get_velocity(self) -> float:
        """Retorna velocidade em DMs por minuto."""
        if self.processed_count == 0:
            return 0
        elapsed = time.time() - (self.start_time or time.time())
        return (self.processed_count / elapsed) * 60 if elapsed > 0 else 0
    
    def get_success_rate(self) -> float:
        """Retorna taxa de sucesso em porcentagem."""
        if self.processed_count == 0:
            return 100.0
        return (self.success_count / self.processed_count) * 100
    
    async def _increment_stats(self, success: bool) -> None:
        async with self._stats_lock:
            self.processed_count += 1
            if success:
                self.success_count += 1
            else:
                self.failed_count += 1
    
    async def _worker(self, worker_id: int) -> None:
        while self.is_running:
            try:
                item = await asyncio.wait_for(self.queue.get(), timeout=1.0)
                await self._process_item(item, worker_id)
                self.queue.task_done()
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Worker {worker_id} erro: {e}")
    
    async def _process_item(self, item: WorkItem, worker_id: int) -> None:
        if await self.idempotency.is_duplicate(item.campaign_id, item.idempotency_key):
            return
        
        await self.rate_limiter.acquire()
        
        personalized = (item.message
                       .replace("{user}", item.member.name)
                       .replace("{mention}", item.member.mention)
                       .replace("{server}", item.guild_name)
                       .replace("{register_channel}", item.register_channel_mention))
        
        delivery_id = f"del_{uuid.uuid4().hex[:16]}"
        
        try:
            await item.member.send(personalized)
            await self.idempotency.mark_sent(item.campaign_id, item.idempotency_key)
            recorded = await self.db.record_delivery(
                delivery_id, item.campaign_id, item.member.id,
                str(item.member), item.idempotency_key, DeliveryStatus.SENT
            )
            if recorded:
                await self._increment_stats(success=True)
                logger.info(f"✅ Worker {worker_id}: DM enviada para {item.member}")
        except discord.Forbidden:
            await self.db.record_delivery(
                delivery_id, item.campaign_id, item.member.id,
                str(item.member), item.idempotency_key, DeliveryStatus.DM_CLOSED, "DM fechada"
            )
            await self._increment_stats(success=False)
        except discord.HTTPException as e:
            if e.status == 429:
                await self.db.record_delivery(
                    delivery_id, item.campaign_id, item.member.id,
                    str(item.member), item.idempotency_key, DeliveryStatus.RATE_LIMITED, "Rate limit"
                )
                await asyncio.sleep(RATE_LIMIT_BACKOFF)
                await self.queue.put(item)
            else:
                await self.db.record_delivery(
                    delivery_id, item.campaign_id, item.member.id,
                    str(item.member), item.idempotency_key, DeliveryStatus.FAILED, f"HTTP {e.status}"
                )
                await self._increment_stats(success=False)
        except Exception as e:
            await self.db.record_delivery(
                delivery_id, item.campaign_id, item.member.id,
                str(item.member), item.idempotency_key, DeliveryStatus.FAILED, str(e)
            )
            await self._increment_stats(success=False)


# ============================================================================
# CAMPAIGN MANAGER V5
# ============================================================================

class CampaignManager:
    def __init__(self, db: Database, client: discord.Client) -> None:
        self.db = db
        self.client = client
        self.active_campaigns: Dict[str, WorkerPool] = {}
        self.idempotency = IdempotencyManager(db)
    
    async def create_campaign(self, name: str, guild_id: int, role_id: int,
                             register_channel_id: int, message: str, delay: int,
                             workers: int, join_filter: str) -> str:
        """Cria nova campanha com schema V5."""
        campaign_id = f"campaign_{uuid.uuid4().hex[:12]}"
        estimated_duration = 0.0
        
        async with self.db.atomic_write() as conn:
            await conn.execute(
                """INSERT INTO campaigns (id, name, guild_id, role_id, register_channel_id,
                   message, delay, workers, join_filter, estimated_duration, status)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (campaign_id, name, guild_id, role_id, register_channel_id,
                 message, delay, workers, join_filter, estimated_duration, CampaignStatus.DRAFT)
            )
        logger.info(f"Campanha criada: {campaign_id}")
        return campaign_id
    
    async def start_campaign(self, campaign_id: str) -> None:
        campaign = await self.db.get_campaign(campaign_id)
        if not campaign:
            raise ValueError(f"Campanha {campaign_id} não encontrada")
        if campaign["status"] != CampaignStatus.DRAFT:
            raise ValueError("Campanha não está em rascunho")
        
        guild = self.client.get_guild(campaign["guild_id"])
        if not guild:
            raise ValueError("Servidor não encontrado")
        role = guild.get_role(campaign["role_id"])
        if not role:
            raise ValueError("Cargo não encontrado")
        
        register_channel = guild.get_channel(campaign.get("register_channel_id", 0))
        register_channel_mention = register_channel.mention if register_channel else "#registro"
        
        all_members = [m for m in role.members if not m.bot]
        total_before_filter = len(all_members)
        
        members = apply_join_filter(all_members, campaign.get("join_filter", "all"))
        total_after_filter = len(members)
        
        if not members:
            raise ValueError("Nenhum membro encontrado após aplicar filtro")
        
        workers = campaign.get("workers", NUM_WORKERS)
        estimated_duration = estimate_campaign_duration(len(members), campaign["delay"], workers)
        
        await self.db.update_campaign_status(campaign_id, CampaignStatus.ACTIVE)
        await self.db.update_campaign_total(campaign_id, len(members))
        await self.db.update_campaign_estimated_duration(campaign_id, estimated_duration)
        
        rate_limiter = RateLimiter()
        worker_pool = WorkerPool(self.db, self.client, rate_limiter, self.idempotency, workers)
        self.active_campaigns[campaign_id] = worker_pool
        await worker_pool.start()
        
        for member in members:
            if await self.db.user_processed_in_campaign(member.id, campaign_id):
                continue
            idempotency_key = self.idempotency.generate_key(member.id, campaign_id, campaign["message"])
            if await self.idempotency.is_duplicate(campaign_id, idempotency_key):
                continue
            await worker_pool.add_work(WorkItem(
                member=member, campaign_id=campaign_id,
                message=campaign["message"], guild_name=guild.name,
                register_channel_mention=register_channel_mention,
                idempotency_key=idempotency_key
            ))
        
        worker_pool.mark_all_added()
        logger.info(f"Campanha {campaign_id}: {total_before_filter} total, {total_after_filter} após filtro, {workers} workers, ~{format_duration(estimated_duration)}")
        asyncio.create_task(self._monitor_campaign(campaign_id, worker_pool))
    
    async def _monitor_campaign(self, campaign_id: str, worker_pool: WorkerPool) -> None:
        last_count = 0
        while worker_pool.is_running:
            await asyncio.sleep(5)
            if worker_pool.processed_count > last_count:
                last_count = worker_pool.processed_count
            if worker_pool.is_all_items_added() and worker_pool.queue.empty():
                await asyncio.sleep(5)
                if worker_pool.queue.empty():
                    await worker_pool.stop()
                    await self.db.update_campaign_status(campaign_id, CampaignStatus.COMPLETED)
                    logger.info(f"✅ Campanha {campaign_id} concluída!")
                    self.active_campaigns.pop(campaign_id, None)
                    break
    
    async def stop_campaign(self, campaign_id: str) -> None:
        if campaign_id in self.active_campaigns:
            await self.active_campaigns[campaign_id].stop()
            await self.db.update_campaign_status(campaign_id, CampaignStatus.PAUSED)
            self.active_campaigns.pop(campaign_id, None)
            logger.info(f"Campanha {campaign_id} pausada")
    
    async def resume_campaign(self, campaign_id: str) -> None:
        campaign = await self.db.get_campaign(campaign_id)
        if not campaign:
            raise ValueError(f"Campanha {campaign_id} não encontrada")
        if campaign["status"] != CampaignStatus.PAUSED:
            raise ValueError("Campanha não está pausada")
        await self.start_campaign(campaign_id)


# ============================================================================
# INTERFACE CLI AVANÇADA V5
# ============================================================================

config = ConfigManager(CONFIG_FILE)
templates = TemplateManager()

CONFIG: ConfigDict = {
    "token": config.get("token", ""),
    "guild_id": config.get("guild_id", ""),
    "role_id": config.get("role_id", ""),
    "register_channel_id": config.get("register_channel_id", ""),
    "delay": config.get("delay", 15),
    "campaign_name": config.get("campaign_name", "default"),
    "message_template": config.get("message_template", ""),
    "auto_save": config.get("auto_save", True),
    "num_workers": config.get("num_workers", 3),
    "join_filter": config.get("join_filter", "all")
}


def save_config() -> None:
    for key in CONFIG:
        config.set(key, CONFIG[key])
    config.save()
    logger.info("Configuração salva no disco")


async def async_input(prompt: str) -> str:
    return await asyncio.to_thread(input, prompt)


def clear_screen() -> None:
    subprocess.run("cls" if os.name == "nt" else "clear", shell=True)


def load_message() -> Optional[str]:
    try:
        if MESSAGE_FILE.exists():
            content = MESSAGE_FILE.read_text(encoding="utf-8").strip()
            return content if content else None
        else:
            MESSAGE_FILE.write_text("Olá {user}! Bem-vindo ao {server}!\nAcesse {register_channel} para liberar seu acesso.", encoding="utf-8")
            return None
    except Exception as e:
        logger.error(f"Erro ao ler message.txt: {e}")
        return None


def print_separator(char: str = "━", length: int = 50) -> None:
    """Imprime linha separadora."""
    print(Fore.CYAN + char * length + Style.RESET_ALL)


# ============================================================================
# MELHORIA 10: PREVIEW AVANÇADO
# ============================================================================

def show_advanced_preview(message: str) -> None:
    """Exibe preview renderizado da mensagem com dados fictícios."""
    clear_screen()
    print(ASCII_LOGO)
    print(Fore.CYAN + "📝 PREVIEW DA MENSAGEM RENDERIZADA\n")
    print_separator()
    rendered = render_message_preview(message)
    print(Fore.GREEN + rendered)
    print_separator()
    print(Fore.YELLOW + "\nVariáveis disponíveis: {user}, {mention}, {server}, {register_channel}")
    print(Fore.CYAN + "Dados fictícios usados: João, Nexus Community, #registro\n")


# ============================================================================
# MELHORIA 5: DASHBOARD EM TEMPO REAL
# ============================================================================

async def show_live_dashboard(campaign_id: str, db: Database, campaign_manager: CampaignManager) -> None:
    """Exibe dashboard em tempo real da campanha ativa."""
    campaign = await db.get_campaign(campaign_id)
    if not campaign:
        return
    
    worker_pool = campaign_manager.active_campaigns.get(campaign_id)
    if not worker_pool:
        return
    
    start_time = time.time()
    campaign_name = campaign.get("name", "N/A")
    
    while worker_pool and worker_pool.is_running:
        clear_screen()
        print(ASCII_LOGO)
        print(Fore.CYAN + "📊 DASHBOARD EM TEMPO REAL\n")
        print_separator()
        
        stats = await db.get_campaign_stats(campaign_id)
        pending = stats["total"] - stats["sent"] - stats["failed"]
        elapsed = time.time() - start_time
        velocity = worker_pool.get_velocity()
        eta = worker_pool.get_eta()
        success_rate = worker_pool.get_success_rate()
        campaign_status = campaign.get("status", "unknown")
        
        print(Fore.YELLOW + f"Campanha: {campaign_name}")
        print(Fore.GREEN + f"Status: {campaign_status.upper()}")
        print()
        print(Fore.WHITE + f"Enviados: {stats['sent']}")
        print(Fore.RED + f"Falhas: {stats['failed']}")
        print(Fore.CYAN + f"Pendentes: {pending}")
        print()
        print(Fore.GREEN + f"Taxa de sucesso: {success_rate:.1f}%")
        print(Fore.CYAN + f"Velocidade: {velocity:.1f} DM/min")
        print(Fore.YELLOW + f"Tempo decorrido: {format_duration(elapsed)}")
        print(Fore.MAGENTA + f"Tempo restante: {format_duration(eta)}")
        
        print_separator()
        print(Fore.YELLOW + "\nPressione Ctrl+C para voltar ao menu...")
        
        try:
            await asyncio.sleep(3)
        except asyncio.CancelledError:
            break
    
    await async_input("\nEnter para continuar...")


# ============================================================================
# MELHORIA 3: CONFIRMAÇÃO DE CAMPANHA
# ============================================================================

async def show_campaign_confirmation(campaign_id: str, db: Database, client: discord.Client) -> bool:
    """Exibe tela de confirmação antes de iniciar campanha."""
    campaign = await db.get_campaign(campaign_id)
    if not campaign:
        return False
    
    guild = client.get_guild(campaign["guild_id"])
    role = guild.get_role(campaign["role_id"]) if guild else None
    register_channel = guild.get_channel(campaign.get("register_channel_id", 0)) if guild else None
    
    members = [m for m in role.members if not m.bot] if role else []
    total_before = len(members)
    filtered = apply_join_filter(members, campaign.get("join_filter", "all"))
    total_after = len(filtered)
    
    estimated = estimate_campaign_duration(total_after, campaign["delay"], campaign.get("workers", NUM_WORKERS))
    
    # Extrai valores com fallback para evitar Optional subscript
    campaign_name: str = campaign.get("name", "N/A")
    campaign_message: str = campaign.get("message", "")
    campaign_delay: int = campaign.get("delay", 15)
    campaign_workers: int = campaign.get("workers", NUM_WORKERS)
    campaign_join_filter: str = campaign.get("join_filter", "all")
    
    clear_screen()
    print(ASCII_LOGO)
    print(Fore.CYAN + "📋 CONFIRMAÇÃO DE CAMPANHA\n")
    print_separator()
    
    print(Fore.YELLOW + f"Campanha: {campaign_name}")
    print()
    print(Fore.WHITE + f"Servidor: {guild.name if guild else 'N/A'}")
    print(Fore.WHITE + f"Cargo: {role.name if role else 'N/A'}")
    print(Fore.WHITE + f"Canal de registro: {register_channel.mention if register_channel else 'N/A'}")
    print()
    print(Fore.CYAN + f"Membros encontrados: {total_before}")
    if campaign_join_filter != "all":
        print(Fore.YELLOW + f"Após filtro ({campaign_join_filter}): {total_after}")
    print()
    print(Fore.WHITE + f"Delay: {campaign_delay}s")
    print(Fore.WHITE + f"Workers: {campaign_workers}")
    print()
    print(Fore.MAGENTA + f"Tempo estimado: {format_duration(estimated)}")
    print()
    
    print(Fore.CYAN + "Prévia da mensagem:")
    print(Fore.WHITE + "-" * 40)
    preview = render_message_preview(campaign_message)
    print(Fore.GREEN + preview[:200])
    print(Fore.WHITE + "-" * 40)
    
    print_separator()
    print(Fore.YELLOW + "\nDeseja iniciar a campanha?")
    print(Fore.GREEN + "[S] Sim")
    print(Fore.RED + "[N] Não")
    
    confirm = (await async_input("> ")).strip().lower()
    return confirm == 's'


# ============================================================================
# MELHORIA 9: HISTÓRICO DETALHADO
# ============================================================================

async def show_campaign_history(db: Database) -> None:
    """Exibe histórico detalhado de campanhas."""
    clear_screen()
    print(ASCII_LOGO)
    print(Fore.CYAN + "📜 HISTÓRICO DE CAMPANHAS\n")
    
    campaigns = await db.get_campaign_history()
    
    if not campaigns:
        print(Fore.YELLOW + "⚠ Nenhuma campanha registrada")
        await async_input("\nEnter para continuar...")
        return
    
    print_separator()
    for i, camp in enumerate(campaigns, 1):
        camp_name: str = camp.get("name", "N/A")
        camp_status: str = camp.get("status", "unknown")
        camp_started_at: Optional[str] = camp.get("started_at")
        camp_finished_at: Optional[str] = camp.get("finished_at")
        camp_estimated: float = camp.get("estimated_duration", 0)
        camp_workers: int = camp.get("workers", 0)
        
        stats = await db.get_campaign_stats(camp["id"])
        icon = "✅" if camp_status == "completed" else "🔄" if camp_status == "active" else "⏸" if camp_status == "paused" else "❌" if camp_status == "failed" else "📝"
        
        print(f"{Fore.WHITE}{i}. {icon} {camp_name}")
        print(f"   Status: {camp_status} | Enviados: {stats['sent']} | Falhas: {stats['failed']}")
        
        if stats['total'] > 0:
            rate = (stats['sent'] / stats['total']) * 100
            print(f"   Taxa: {rate:.1f}%", end="")
        
        if camp_started_at:
            print(f" | Início: {camp_started_at[:19]}", end="")
        if camp_finished_at:
            print(f" | Fim: {camp_finished_at[:19]}", end="")
        print()
        
        if camp_estimated > 0:
            print(f"   Workers: {camp_workers or 'N/A'} | Duração est.: {format_duration(camp_estimated)}")
        print()
    
    print_separator()
    
    try:
        idx = int((await async_input("\nNúmero para detalhes (0 para voltar): ")).strip()) - 1
        if 0 <= idx < len(campaigns):
            await show_campaign_detail(db, campaigns[idx]["id"])
    except ValueError:
        pass


async def show_campaign_detail(db: Database, campaign_id: str) -> None:
    """Exibe detalhes completos de uma campanha."""
    campaign = await db.get_campaign(campaign_id)
    if not campaign:
        return
    
    # Extrai todos os valores antes de usar
    campaign_name: str = campaign.get("name", "N/A")
    campaign_id_str: str = campaign.get("id", "N/A")
    campaign_status: str = campaign.get("status", "unknown")
    campaign_guild_id: int = campaign.get("guild_id", 0)
    campaign_role_id: int = campaign.get("role_id", 0)
    campaign_register_channel: int = campaign.get("register_channel_id", 0)
    campaign_delay: int = campaign.get("delay", 0)
    campaign_workers: int = campaign.get("workers", 0)
    campaign_join_filter: str = campaign.get("join_filter", "all")
    campaign_total: int = campaign.get("total_members", 0)
    campaign_started_at: Optional[str] = campaign.get("started_at")
    campaign_finished_at: Optional[str] = campaign.get("finished_at")
    campaign_estimated: float = campaign.get("estimated_duration", 0)
    
    stats = await db.get_campaign_stats(campaign_id_str)
    
    clear_screen()
    print(ASCII_LOGO)
    print(Fore.CYAN + f"📋 DETALHES: {campaign_name}\n")
    print_separator()
    
    print(Fore.YELLOW + f"ID: {campaign_id_str}")
    print(f"Status: {campaign_status}")
    print(f"Servidor ID: {campaign_guild_id}")
    print(f"Cargo ID: {campaign_role_id}")
    print(f"Canal Registro ID: {campaign_register_channel or 'N/A'}")
    print(f"Delay: {campaign_delay}s")
    print(f"Workers: {campaign_workers or 'N/A'}")
    print(f"Filtro: {campaign_join_filter}")
    print()
    print(Fore.GREEN + f"Total membros: {campaign_total}")
    print(f"Enviados: {stats['sent']}")
    print(f"Falhas: {stats['failed']}")
    if stats['total'] > 0:
        print(f"Taxa: {(stats['sent']/stats['total'])*100:.1f}%")
    print()
    
    if campaign_started_at:
        print(f"Início: {campaign_started_at[:19]}")
    if campaign_finished_at:
        print(f"Término: {campaign_finished_at[:19]}")
    if campaign_estimated > 0:
        print(f"Duração estimada: {format_duration(campaign_estimated)}")
    
    print_separator()
    await async_input("\nEnter para continuar...")


# ============================================================================
# MENUS
# ============================================================================

async def message_menu(db: Database) -> None:
    """Submenu de gerenciamento de mensagem - V5 com preview avançado."""
    while True:
        clear_screen()
        print(ASCII_LOGO)
        print(Fore.CYAN + "📝 GERENCIADOR DE MENSAGEM\n")
        
        message = load_message()
        if message:
            preview = message[:80] + "..." if len(message) > 80 else message
            print(Fore.GREEN + f"Mensagem atual: {preview}\n")
        else:
            print(Fore.RED + "Nenhuma mensagem configurada\n")
        
        print(Fore.YELLOW + "1 - Visualizar mensagem completa")
        print("2 - Editar mensagem (abrir arquivo)")
        print("3 - Digitar nova mensagem direto")
        print("4 - Preview renderizado")
        print("5 - Carregar template")
        print("6 - Salvar como template")
        print("0 - Voltar\n")
        
        choice = (await async_input(Fore.YELLOW + "Escolha: ")).strip()
        
        match choice:
            case "1":
                clear_screen()
                print(ASCII_LOGO)
                msg = load_message()
                if msg:
                    print(Fore.CYAN + "📝 MENSAGEM COMPLETA\n")
                    print_separator()
                    print(Fore.GREEN + msg)
                    print_separator()
                await async_input("\nEnter para continuar...")
            case "2":
                try:
                    if not MESSAGE_FILE.exists():
                        MESSAGE_FILE.write_text("Olá {user}! Bem-vindo ao {server}!\nAcesse {register_channel} para liberar seu acesso.", encoding="utf-8")
                    if sys.platform == "win32":
                        os.startfile(MESSAGE_FILE)
                    elif sys.platform == "darwin":
                        subprocess.run(["open", MESSAGE_FILE])
                    else:
                        subprocess.run(["xdg-open", MESSAGE_FILE])
                    print(Fore.GREEN + f"\n📝 Arquivo aberto: {MESSAGE_FILE}")
                    await async_input("Enter para continuar...")
                except Exception as e:
                    print(Fore.RED + f"❌ Erro: {e}")
                    await async_input("Enter para continuar...")
            case "3":
                print(Fore.CYAN + "\nDigite a mensagem (use {user}, {mention}, {server}, {register_channel}):")
                new_msg = await async_input("> ")
                if new_msg.strip():
                    MESSAGE_FILE.write_text(new_msg.strip(), encoding="utf-8")
                    print(Fore.GREEN + "✅ Mensagem salva!")
                await async_input("Enter para continuar...")
            case "4":
                msg = load_message()
                if msg:
                    show_advanced_preview(msg)
                else:
                    print(Fore.RED + "❌ Nenhuma mensagem configurada")
                await async_input("Enter para continuar...")
            case "5":
                available = templates.list_templates()
                if available:
                    print(Fore.CYAN + "\nTemplates disponíveis:")
                    for i, t in enumerate(available, 1):
                        print(f"  {i}. {t}")
                    try:
                        idx = int((await async_input("Número: ")).strip()) - 1
                        if 0 <= idx < len(available):
                            content = templates.load_template(available[idx])
                            if content:
                                MESSAGE_FILE.write_text(content, encoding="utf-8")
                                print(Fore.GREEN + f"✅ Template '{available[idx]}' carregado!")
                    except ValueError:
                        pass
                else:
                    print(Fore.YELLOW + "⚠ Nenhum template salvo")
                await async_input("Enter para continuar...")
            case "6":
                current = load_message()
                if current:
                    name = (await async_input("Nome do template: ")).strip()
                    if name:
                        templates.save_template(name, current)
                        print(Fore.GREEN + f"✅ Template '{name}' salvo!")
                await async_input("Enter para continuar...")
            case "0":
                break
            case _:
                pass


async def show_header(db: Database) -> None:
    """Exibe cabeçalho com status do sistema."""
    clear_screen()
    print(ASCII_LOGO)
    
    token_status = "✅" if CONFIG["token"] else "❌"
    guild_status = "✅" if CONFIG["guild_id"] else "❌"
    role_status = "✅" if CONFIG["role_id"] else "❌"
    channel_status = "✅" if CONFIG["register_channel_id"] else "❌"
    
    print(Fore.CYAN + f"⚙️  Token:{token_status} Guild:{guild_status} Role:{role_status} Channel:{channel_status} Delay:{CONFIG['delay']}s Workers:{CONFIG['num_workers']}\n")
    
    campaigns = await db.list_campaigns()
    if campaigns:
        print(Fore.GREEN + "📋 Campanhas recentes:")
        for camp in campaigns[:3]:
            stats = await db.get_campaign_stats(camp["id"])
            icon = "✅" if camp["status"] == "completed" else "🔄" if camp["status"] == "active" else "⏸" if camp["status"] == "paused" else "📝"
            print(f"{Fore.WHITE}  {icon} {camp['name']} - {stats['sent']}/{camp['total_members']} enviados")
    else:
        print(Fore.YELLOW + "📋 Nenhuma campanha registrada")
    print()


async def main_menu(client: discord.Client, db: Database) -> None:
    """Loop principal do menu V5."""
    campaign_manager = CampaignManager(db, client)
    
    while True:
        await show_header(db)
        
        print(Fore.CYAN + "📌 MENU PRINCIPAL V5\n")
        print(Fore.YELLOW + "1  - Definir Token")
        print("2  - Definir Guild ID")
        print("3  - Definir Role ID")
        print("4  - Definir Canal de Registro")
        print("5  - Gerenciar Mensagem")
        print("6  - Configurar Workers")
        print("7  - Configurar Filtro de Data")
        print("8  - Criar campanha")
        print("9  - Iniciar campanha")
        print("10 - Dashboard ao vivo")
        print("11 - Parar campanha")
        print("12 - Resumir campanha")
        print("13 - Estatísticas")
        print("14 - Exportar campanha (CSV)")
        print("15 - Histórico detalhado")
        print("16 - Deletar campanha")
        print("17 - Salvar configuração")
        print("0  - Sair\n")
        
        choice = (await async_input(Fore.YELLOW + "Escolha: ")).strip()
        
        match choice:
            case "1":
                CONFIG["token"] = (await async_input("Token: ")).strip()
                if CONFIG["auto_save"]: save_config()
                print(Fore.GREEN + "✅ Token atualizado!")
            case "2":
                CONFIG["guild_id"] = (await async_input("Guild ID: ")).strip()
                if CONFIG["auto_save"]: save_config()
                print(Fore.GREEN + "✅ Guild ID atualizado!")
            case "3":
                CONFIG["role_id"] = (await async_input("Role ID: ")).strip()
                if CONFIG["auto_save"]: save_config()
                print(Fore.GREEN + "✅ Role ID atualizado!")
            case "4":
                CONFIG["register_channel_id"] = (await async_input("Canal de Registro ID: ")).strip()
                if CONFIG["auto_save"]: save_config()
                print(Fore.GREEN + "✅ Canal de Registro atualizado!")
            case "5":
                await message_menu(db)
            case "6":
                try:
                    w = int((await async_input(f"Workers (1-{MAX_WORKERS}, atual={CONFIG['num_workers']}): ")).strip())
                    CONFIG["num_workers"] = max(1, min(MAX_WORKERS, w))
                    if CONFIG["auto_save"]: save_config()
                    print(Fore.GREEN + f"✅ Workers: {CONFIG['num_workers']}")
                except ValueError:
                    print(Fore.RED + "❌ Número inválido")
            case "7":
                print(Fore.CYAN + "\nFiltro por data de entrada:")
                print("1 - Últimas 24 horas")
                print("2 - Últimos 7 dias")
                print("3 - Últimos 30 dias")
                print("4 - Todos os membros")
                f = (await async_input("Escolha: ")).strip()
                filters = {"1": "24h", "2": "7d", "3": "30d", "4": "all"}
                CONFIG["join_filter"] = filters.get(f, "all")
                if CONFIG["auto_save"]: save_config()
                print(Fore.GREEN + f"✅ Filtro: {CONFIG['join_filter']}")
            case "8":
                name = (await async_input("Nome da campanha: ")).strip()
                message = load_message()
                if not message:
                    print(Fore.RED + "❌ Configure a mensagem primeiro (opção 5)")
                elif CONFIG["guild_id"] and CONFIG["role_id"]:
                    try:
                        campaign_id = await campaign_manager.create_campaign(
                            name=name,
                            guild_id=int(CONFIG["guild_id"]),
                            role_id=int(CONFIG["role_id"]),
                            register_channel_id=int(CONFIG["register_channel_id"] or "0"),
                            message=message,
                            delay=CONFIG["delay"],
                            workers=CONFIG["num_workers"],
                            join_filter=CONFIG["join_filter"]
                        )
                        print(Fore.GREEN + f"✅ Campanha {campaign_id} criada!")
                    except Exception as e:
                        print(Fore.RED + f"❌ Erro: {e}")
                else:
                    print(Fore.RED + "❌ Configure Guild ID e Role ID primeiro")
            case "9":
                campaigns = await db.list_campaigns("draft")
                if campaigns:
                    print(Fore.CYAN + "Campanhas em rascunho:")
                    for i, c in enumerate(campaigns, 1):
                        print(f"  {i}. {c['name']}")
                    try:
                        idx = int((await async_input("Número: ")).strip()) - 1
                        if 0 <= idx < len(campaigns):
                            if await show_campaign_confirmation(campaigns[idx]["id"], db, client):
                                await campaign_manager.start_campaign(campaigns[idx]["id"])
                                print(Fore.GREEN + "✅ Campanha iniciada!")
                                show = (await async_input("Exibir dashboard ao vivo? (s/n): ")).strip().lower()
                                if show == 's':
                                    await show_live_dashboard(campaigns[idx]["id"], db, campaign_manager)
                            else:
                                print(Fore.YELLOW + "⚠ Campanha cancelada pelo usuário")
                    except ValueError:
                        print(Fore.RED + "❌ Número inválido")
                else:
                    print(Fore.YELLOW + "⚠ Nenhuma campanha em rascunho")
            case "10":
                if campaign_manager.active_campaigns:
                    for cid in list(campaign_manager.active_campaigns.keys()):
                        camp = await db.get_campaign(cid)
                        camp_name = camp.get("name", "N/A") if camp else "N/A"
                        print(f"  {cid}: {camp_name}")
                    cid = (await async_input("Campaign ID: ")).strip()
                    if cid in campaign_manager.active_campaigns:
                        await show_live_dashboard(cid, db, campaign_manager)
                else:
                    print(Fore.YELLOW + "⚠ Nenhuma campanha ativa")
            case "11":
                if campaign_manager.active_campaigns:
                    for cid in list(campaign_manager.active_campaigns.keys()):
                        print(f"ID: {cid}")
                    cid = (await async_input("Campaign ID: ")).strip()
                    await campaign_manager.stop_campaign(cid)
                    print(Fore.GREEN + "✅ Campanha parada!")
                else:
                    print(Fore.YELLOW + "⚠ Nenhuma campanha ativa")
            case "12":
                paused = await db.list_campaigns("paused")
                if paused:
                    for i, c in enumerate(paused, 1):
                        print(f"  {i}. {c['name']}")
                    try:
                        idx = int((await async_input("Número: ")).strip()) - 1
                        if 0 <= idx < len(paused):
                            await campaign_manager.resume_campaign(paused[idx]["id"])
                            print(Fore.GREEN + "✅ Campanha resumida!")
                    except ValueError:
                        print(Fore.RED + "❌ Número inválido")
                else:
                    print(Fore.YELLOW + "⚠ Nenhuma campanha pausada")
            case "13":
                clear_screen()
                print(ASCII_LOGO)
                print(Fore.CYAN + "📊 ESTATÍSTICAS\n")
                global_stats = await db.get_global_stats()
                print(Fore.GREEN + "📈 Global:")
                print(f"  Campanhas: {global_stats['total_campaigns']}")
                print(f"  Entregas: {global_stats['total_deliveries']}")
                print(f"  Sucesso: {global_stats['total_sent']}")
                print(f"  Falhas: {global_stats['total_failed']}")
                if global_stats['total_deliveries'] > 0:
                    print(f"  Taxa: {(global_stats['total_sent']/global_stats['total_deliveries'])*100:.1f}%")
            case "14":
                campaigns = await db.list_campaigns()
                if campaigns:
                    for i, c in enumerate(campaigns, 1):
                        print(f"  {i}. {c['name']} [{c['status']}]")
                    try:
                        idx = int((await async_input("Número: ")).strip()) - 1
                        if 0 <= idx < len(campaigns):
                            path = await db.export_campaign_csv(campaigns[idx]["id"])
                            print(Fore.GREEN + f"✅ Exportado: {path}")
                    except ValueError:
                        print(Fore.RED + "❌ Número inválido")
                else:
                    print(Fore.YELLOW + "⚠ Nenhuma campanha")
            case "15":
                await show_campaign_history(db)
            case "16":
                campaigns = await db.list_campaigns()
                if campaigns:
                    for i, c in enumerate(campaigns, 1):
                        print(f"  {i}. {c['name']} [{c['status']}]")
                    try:
                        idx = int((await async_input("Número: ")).strip()) - 1
                        if 0 <= idx < len(campaigns):
                            confirm = await async_input(Fore.RED + f"Deletar '{campaigns[idx]['name']}'? (s/n): ")
                            if confirm.strip().lower() == 's':
                                await db.delete_campaign(campaigns[idx]["id"])
                                print(Fore.GREEN + "✅ Campanha deletada!")
                    except ValueError:
                        print(Fore.RED + "❌ Número inválido")
            case "17":
                save_config()
                print(Fore.GREEN + "✅ Configuração salva!")
            case "0":
                for cid in list(campaign_manager.active_campaigns.keys()):
                    await campaign_manager.stop_campaign(cid)
                if CONFIG["auto_save"]: save_config()
                await db.close()
                await client.close()
                break
            case _:
                print(Fore.RED + "❌ Opção inválida")
        
        if choice not in ["5", "9", "10", "13", "15"]:
            await async_input("\nEnter para continuar...")


# ============================================================================
# CLIENTE DISCORD
# ============================================================================

class MassDMBot(discord.Client):
    def __init__(self, db: Database, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.db = db
    
    async def on_ready(self) -> None:
        shard_info = f" [Shard {self.shard_id}]" if self.shard_id is not None else ""
        logger.info(f"✅ Bot conectado como {self.user}{shard_info}")
        await main_menu(self, self.db)
    
    async def on_shard_ready(self, shard_id: int) -> None:
        logger.info(f"Shard {shard_id} pronto")


# ============================================================================
# ENTRYPOINT
# ============================================================================

async def main() -> None:
    logger.info("🚀 Iniciando Nexus DM Platform v5.0.1")
    
    if SHARD_COUNT > 1:
        logger.info(f"Modo Shard: {SHARD_COUNT} shards")
    
    EXPORTS_DIR.mkdir(exist_ok=True)
    
    db = Database(DB_FILE)
    await db.initialize()
    
    token = CONFIG["token"]
    if not token:
        token = (await async_input("Token do bot: ")).strip()
        CONFIG["token"] = token
        if CONFIG["auto_save"]: save_config()
    
    intents = discord.Intents.default()
    intents.members = True
    
    bot = MassDMBot(
        db, intents=intents,
        shard_count=SHARD_COUNT if SHARD_COUNT > 1 else None,
        shard_ids=SHARD_IDS if SHARD_IDS else None
    )
    
    try:
        await bot.start(token)
    except Exception as e:
        logger.error(f"Erro fatal: {e}")
        raise
    finally:
        if not bot.is_closed():
            await bot.close()
        await db.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Encerrado pelo usuário")
        sys.exit(0)
    except discord.LoginFailure:
        logger.error("Token inválido")
        print(Fore.RED + "❌ Token inválido.")
        sys.exit(1)
    except discord.PrivilegedIntentsRequired:
        logger.error("Intents privilegiados necessários")
        print(Fore.RED + "❌ Habilite 'Server Members Intent' no Discord Developer Portal")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Erro fatal: {e}")
        sys.exit(1)