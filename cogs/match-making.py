import os
import time
import random
import json
import io
import asyncio
import math
from collections import defaultdict
from contextlib import asynccontextmanager
from typing import Optional, Tuple, List, Dict, Any

import discord
from discord import app_commands
from discord.ext import commands, tasks
import aiosqlite

# Logging removed

DB_DIR = "database"
DB_PATH = os.path.join(DB_DIR, "matchmaker.sqlite")
os.makedirs(DB_DIR, exist_ok=True)

EMBED_COLOR = 0x2F3136
SKIP_BLOCK_MINUTES = 1440  # 24 hours
MAX_DB_CANDIDATES = 2000
SCAN_WINDOW = 200

# ----- Database Connection Pool -----

class DatabasePool:
    def __init__(self, path: str, size: int = 3):
        self.path = path
        self.size = size
        self._pool: list[aiosqlite.Connection] = []
        self._lock = asyncio.Lock()
        self._initialized = False

    async def initialize(self):
        if self._initialized:
            return
        async with self._lock:
            if not self._initialized:
                for _ in range(self.size):
                    conn = await aiosqlite.connect(self.path)
                    await conn.execute("PRAGMA foreign_keys=ON;")
                    await conn.execute("PRAGMA journal_mode=WAL;")
                    await conn.execute("PRAGMA synchronous=NORMAL;")
                    await conn.execute("PRAGMA busy_timeout=30000;")
                    conn.row_factory = aiosqlite.Row
                    self._pool.append(conn)
                self._initialized = True

    async def close(self):
        async with self._lock:
            while self._pool:
                try:
                    conn = self._pool.pop()
                    await conn.close()
                except Exception:
                    pass
            self._initialized = False

    @asynccontextmanager
    async def get(self):
        await self.initialize()
        async with self._lock:
            if self._pool:
                conn = self._pool.pop()
            else:
                conn = await aiosqlite.connect(self.path)
                await conn.execute("PRAGMA foreign_keys=ON;")
                await conn.execute("PRAGMA journal_mode=WAL;")
                await conn.execute("PRAGMA synchronous=NORMAL;")
                await conn.execute("PRAGMA busy_timeout=30000;")
                conn.row_factory = aiosqlite.Row
        try:
            yield conn
        finally:
            async with self._lock:
                if len(self._pool) < self.size:
                    self._pool.append(conn)
                else:
                    await conn.close()

db_pool = DatabasePool(DB_PATH)

# ----- Safe Reply -----

async def safe_reply(interaction: discord.Interaction, content: Optional[str] = None, embed: Optional[discord.Embed] = None, ephemeral: bool = True):
    try:
        if not interaction.response.is_done():
            if embed:
                await interaction.response.send_message(embed=embed, ephemeral=ephemeral)
            else:
                await interaction.response.send_message(content, ephemeral=ephemeral)
        else:
            if embed:
                await interaction.followup.send(embed=embed, ephemeral=ephemeral)
            else:
                await interaction.followup.send(content, ephemeral=ephemeral)
    except Exception:
        try:
            if interaction.channel:
                if embed:
                    await interaction.channel.send(f"{interaction.user.mention}", embed=embed, delete_after=15 if ephemeral else None)
                else:
                    await interaction.channel.send(f"{interaction.user.mention} {content}", delete_after=15 if ephemeral else None)
        except Exception:
            pass

# ----- Notification Manager -----

class NotificationManager:
    def __init__(self):
        self.prefs: dict[int, dict] = {}

    async def send(self, user: discord.abc.User, content: str, notif_type: str):
        # Respect user DM preferences (global per user). Default: enabled
        try:
            uid = int(user.id)
        except Exception:
            uid = None
        allow = True
        try:
            if uid is not None:
                async with db_pool.get() as db:
                    row = await (await db.execute("SELECT dm_enabled FROM user_prefs WHERE user_id=?", (uid,))).fetchone()
                if row is not None and int(row["dm_enabled"]) == 0:
                    allow = False
        except Exception:
            # On DB error, fail open (send)
            allow = True

        if not allow:
            return
        try:
            msg = await user.send(content)

            # Persist for post-restart cleanup
            delete_after = int(time.time()) + 60
            try:
                async with db_pool.get() as db:
                    await db.execute(
                        """
                        INSERT INTO dm_messages(message_id, channel_id, user_id, delete_after)
                        VALUES(?, ?, ?, ?)
                        ON CONFLICT(message_id) DO UPDATE SET delete_after=excluded.delete_after
                        """,
                        (int(msg.id), int(msg.channel.id), int(user.id), delete_after)
                    )
                    await db.commit()
            except Exception:
                pass

            async def _delete_later():
                try:
                    await asyncio.sleep(60)
                    try:
                        await msg.delete()
                    finally:
                        try:
                            async with db_pool.get() as db:
                                await db.execute("DELETE FROM dm_messages WHERE message_id=?", (int(msg.id),))
                                await db.commit()
                        except Exception:
                            pass
                except Exception:
                    pass
            asyncio.create_task(_delete_later())
        except Exception:
            pass

notif_manager = NotificationManager()

# ----- Member Roles Cache (to minimize fetch_member in big servers) -----

class MemberRoleCache:
    def __init__(self, max_size: int = 5000, ttl_seconds: int = 300):
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        self._cache: Dict[int, Tuple[int, set[int]]] = {}
        self._order: List[int] = []
        self._lock = asyncio.Lock()

    async def get_roles(self, guild: discord.Guild, user_id: int) -> Optional[set[int]]:
        now = int(time.time())
        async with self._lock:
            item = self._cache.get(user_id)
            if item and now - item[0] <= self.ttl_seconds:
                # move to end
                try:
                    self._order.remove(user_id)
                except ValueError:
                    pass
                self._order.append(user_id)
                return set(item[1])
        # Not cached or expired — fetch minimal info
        try:
            member = guild.get_member(user_id)
            if member is None:
                try:
                    member = await guild.fetch_member(user_id)
                except Exception:
                    member = None
            roles = {r.id for r in member.roles} if isinstance(member, discord.Member) else None
        except Exception:
            roles = None
        if roles is None:
            return None
        async with self._lock:
            self._cache[user_id] = (now, set(roles))
            self._order.append(user_id)
            if len(self._order) > self.max_size:
                # evict oldest
                try:
                    oldest = self._order.pop(0)
                    self._cache.pop(oldest, None)
                except Exception:
                    pass
        return set(roles)

# ----- UI Views -----

class MatchPanel(discord.ui.View):
    def __init__(self, cog: "Matchmaker"):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(label="Get A Match", style=discord.ButtonStyle.secondary, custom_id="mm:join")
    async def start_match(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            # Check if user is in a thread
            if isinstance(interaction.channel, discord.Thread):
                embed = discord.Embed(title="Cannot Join Queue", color=0xE74C3C)
                embed.description = "You cannot join the queue while in a match thread. Please leave your current match first."
                return await safe_reply(interaction, embed=embed, ephemeral=True)

            # Check if bot has required permissions
            if not interaction.guild.me.guild_permissions.create_private_threads:
                embed = discord.Embed(title="Bot Missing Permissions", color=0xE74C3C)
                embed.description = "The bot needs permission to create private threads to function properly."
                return await safe_reply(interaction, embed=embed, ephemeral=True)

            # Attempt to enqueue
            await self.cog.enqueue(interaction.guild.id, interaction.user.id)
            pos, total, eta = await self.cog.get_position_and_eta(interaction.guild.id, interaction.user.id)
            minutes = eta // 60
            seconds = eta % 60
            embed = discord.Embed(title="Queue Position", color=EMBED_COLOR)
            embed.description = f"⏰ You are currently **#{pos}** in the chat queue\n👥 **Total Users Waiting:** {total}\n\nEstimated wait: {minutes}m {seconds}s"
            await safe_reply(interaction, embed=embed, ephemeral=True)
            
        except ValueError as e:
            # Handle known errors like already in queue
            embed = discord.Embed(title="Cannot Join Queue", color=0xE74C3C)
            embed.description = str(e)
            await safe_reply(interaction, embed=embed, ephemeral=True)
            
        except aiosqlite.Error as e:
            # Handle database-specific errors
            embed = discord.Embed(title="Queue Error", color=0xE74C3C)
            embed.description = "There was a problem with the queue database. Please try again in a few moments."
            await safe_reply(interaction, embed=embed, ephemeral=True)
            
        except Exception as e:
            # Handle unexpected errors
            embed = discord.Embed(title="Error queuing. Please try again.", color=0xE74C3C)
            embed.description = "An unexpected error occurred. Please try again."
            await safe_reply(interaction, embed=embed, ephemeral=True)

class ThreadControls(discord.ui.View):
    def __init__(self, cog: "Matchmaker", guild_id: int, thread_id: int):
        super().__init__(timeout=None)
        self.cog = cog
        self.guild_id = guild_id
        self.thread_id = thread_id

        # Use unique custom_ids per thread to ensure persistent view routing correctness after restarts
        skip_btn = discord.ui.Button(label="Skip", style=discord.ButtonStyle.secondary, custom_id=f"mm:thread:{thread_id}:skip")
        leave_btn = discord.ui.Button(label="Leave", style=discord.ButtonStyle.secondary, custom_id=f"mm:thread:{thread_id}:leave")
        report_btn = discord.ui.Button(label="Report", style=discord.ButtonStyle.danger, custom_id=f"mm:thread:{thread_id}:report")

        async def on_skip(interaction: discord.Interaction):
            await self._on_skip(interaction)

        async def on_leave(interaction: discord.Interaction):
            await self._on_leave(interaction)

        async def on_report(interaction: discord.Interaction):
            await self._on_report(interaction)

        skip_btn.callback = on_skip
        leave_btn.callback = on_leave
        report_btn.callback = on_report

        self.add_item(skip_btn)
        self.add_item(leave_btn)
        self.add_item(report_btn)

    async def _on_skip(self, interaction: discord.Interaction):
        try:
            thread = await self.cog._safe_get_thread(interaction.guild, self.thread_id)
            if not thread:
                return await safe_reply(interaction, embed=discord.Embed(title="Thread not found.", color=0xE74C3C))

            # Find other participant
            other_id = self.cog._get_other_id(self.thread_id, interaction.user.id)
            if not other_id:
                return await safe_reply(interaction, embed=discord.Embed(title="No active match found.", color=0xE74C3C))

            # Remove from thread
            try:
                await thread.remove_user(interaction.user)
            except Exception:
                pass

            # Block this pair to avoid immediate rematch
            await self.cog.block_pair(self.guild_id, interaction.user.id, other_id)

            # Re-queue
            await self.cog.enqueue(self.guild_id, interaction.user.id)

            # DM notify
            await notif_manager.send(
                interaction.user,
                "Skipped your current match. You've been re-queued for a new match.",
                "skip"
            )

            # Notify the other participant
            try:
                other_member = interaction.guild.get_member(other_id)
                if other_member:
                    await notif_manager.send(other_member, "Your partner skipped. You'll stay in the room or you can leave and re-queue.", "skip_notice")
            except Exception:
                pass

            embed = discord.Embed(title="You skipped. Re-queued for a new match.", color=EMBED_COLOR)
            await safe_reply(interaction, embed=embed, ephemeral=True)

            # If both participants press Skip, close this thread and re-queue both (rematch-like flow)
            try:
                meta = self.cog.match_meta.get(self.thread_id)
                if meta is not None:
                    votes: set[int] = meta.setdefault("skip_votes", set())
                    votes.add(int(interaction.user.id))
                    pair_ids = meta.get("pairs", [])
                    other_id2 = next((uid for uid in pair_ids if uid != interaction.user.id), None)
                    if other_id2 and int(other_id2) in votes:
                        # Both have skipped: close thread but don't automatically re-queue anyone
                        try:
                            await thread.delete(reason="Both participants skipped")
                        except Exception:
                            pass
                        await self.cog._close_match_row(self.thread_id)
                        self.cog.match_meta.pop(self.thread_id, None)
                        try:
                            async with db_pool.get() as db:
                                await db.execute("DELETE FROM pending_deletions WHERE thread_id=?", (self.thread_id,))
                                await db.commit()
                        except Exception:
                            pass
                        # Best-effort notify the clicker
                        await safe_reply(interaction, embed=discord.Embed(title="Both users skipped. Use the matchmaking panel to find a new match.", color=EMBED_COLOR), ephemeral=True)
            except Exception:
                pass
        except Exception:
            embed = discord.Embed(title="Error processing skip. Please try again.", color=0xE74C3C)
            await safe_reply(interaction, embed=embed)

    async def _on_leave(self, interaction: discord.Interaction):
        try:
            thread = await self.cog._safe_get_thread(interaction.guild, self.thread_id)
            if not thread:
                return await safe_reply(interaction, embed=discord.Embed(title="Thread not found.", color=0xE74C3C))

            other_id = self.cog._get_other_id(self.thread_id, interaction.user.id)

            # Remove the leaver from the thread immediately
            try:
                await thread.remove_user(interaction.user)
            except Exception:
                pass

            # Block the pair to prevent immediate rematch
            if other_id:
                await self.cog.block_pair(self.guild_id, interaction.user.id, other_id)

            # Just notify the remaining participant
            try:
                if other_id:
                    other_member = interaction.guild.get_member(other_id)
                    if other_member:
                        await notif_manager.send(other_member, "Your partner left the match. Press Leave to find a new match.", "left_notice")
            except Exception:
                pass

            # Acknowledge
            embed = discord.Embed(title="You left the match. This room will be deleted in 2 minutes.", color=EMBED_COLOR)
            await safe_reply(interaction, embed=embed, ephemeral=True)

            async def _delete_later():
                await asyncio.sleep(120)
                try:
                    t = await self.cog._safe_get_thread(interaction.guild, self.thread_id)
                    if t:
                        await t.delete(reason="User left; scheduled cleanup after 2 minutes")
                except Exception:
                    pass
                await self.cog._close_match_row(self.thread_id)
                self.cog.match_meta.pop(self.thread_id, None)
                try:
                    async with db_pool.get() as db:
                        await db.execute("DELETE FROM pending_deletions WHERE thread_id=?", (self.thread_id,))
                        await db.commit()
                except Exception:
                    pass

            asyncio.create_task(_delete_later())
            
            # Persist deletion schedule so restarts still clean up
            try:
                async with db_pool.get() as db:
                    await db.execute(
                        "INSERT INTO pending_deletions(thread_id, guild_id, delete_after) VALUES(?,?,?)\n                         ON CONFLICT(thread_id) DO UPDATE SET delete_after=excluded.delete_after",
                        (self.thread_id, self.guild_id, int(time.time()) + 120)
                    )
                    await db.commit()
            except Exception:
                pass
        except Exception:
            embed = discord.Embed(title="Error processing leave. Please try again.", color=0xE74C3C)
            await safe_reply(interaction, embed=embed)

    async def _on_report(self, interaction: discord.Interaction):
        try:
            meta = self.cog.match_meta.get(self.thread_id)
            if not meta:
                return await safe_reply(interaction, embed=discord.Embed(title="No active match found.", color=0xE74C3C))
            other_id = next((uid for uid in meta.get("pairs", []) if uid != interaction.user.id), None)
            if not other_id:
                return await safe_reply(interaction, embed=discord.Embed(title="Could not find the other participant.", color=0xE74C3C))

            modal = ReportModal(self.cog, self.thread_id, other_id)
            await interaction.response.send_modal(modal)
        except Exception:
            embed = discord.Embed(title="Error processing report. Please try again.", color=0xE74C3C)
            await safe_reply(interaction, embed=embed)

# ----- Report Modal -----

class ReportModal(discord.ui.Modal):
    def __init__(self, cog: "Matchmaker", thread_id: int, reported_user_id: int):
        super().__init__(title="Report User", timeout=300)
        self.cog = cog
        self.thread_id = thread_id
        self.reported_user_id = reported_user_id

        self.reason: discord.ui.TextInput = discord.ui.TextInput(
            label="Reason for report",
            placeholder="Please describe the issue...",
            style=discord.TextStyle.short,
            required=True,
            max_length=100
        )
        self.add_item(self.reason)

        self.details: discord.ui.TextInput = discord.ui.TextInput(
            label="Additional details (optional)",
            placeholder="Provide any additional context...",
            style=discord.TextStyle.long,
            required=False,
            max_length=1000
        )
        self.add_item(self.details)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            # First send a confirmation that we received the report
            try:
                await interaction.response.send_message("Processing your report...", ephemeral=True)
            except discord.errors.NotFound:
                return  # Interaction already expired
                
            cfg = await self.cog.get_config(interaction.guild.id)
            if not cfg or not cfg["report_channel_id"]:
                try:
                    await interaction.edit_original_response(content="⚠️ Report channel not configured.")
                except discord.errors.NotFound:
                    pass
                return

            meta = self.cog.match_meta.get(self.thread_id)
            if not meta:
                try:
                    await interaction.edit_original_response(content="⚠️ No active match found.")
                except discord.errors.NotFound:
                    pass
                return

            report_ch = interaction.guild.get_channel(cfg["report_channel_id"])
            if not isinstance(report_ch, (discord.TextChannel, discord.Thread, discord.ForumChannel)):
                try:
                    await interaction.edit_original_response(content="⚠️ Report channel not found.")
                except discord.errors.NotFound:
                    pass
                return

            # Prepare report embed
            embed = discord.Embed(title="New Matchmaking Report", color=0xE74C3C)
            embed.add_field(name="Reporter", value=interaction.user.mention, inline=True)
            embed.add_field(name="Reported", value=f"<@{self.reported_user_id}>", inline=True)
            embed.add_field(name="Thread", value=f"<#{self.thread_id}>", inline=True)
            embed.add_field(name="Reason", value=str(self.reason.value), inline=False)
            if self.details.value:
                embed.add_field(name="Details", value=str(self.details.value), inline=False)

            # Generate transcript in background
            transcript_file = None
            try:
                thread = interaction.guild.get_thread(self.thread_id)
                if thread:
                    lines: List[str] = []
                    async for m in thread.history(limit=None, oldest_first=True):
                        if not m.author.bot:
                            content = m.content or ""
                            if m.attachments:
                                att_text = " ".join(f"[{att.filename}]({att.url})" for att in m.attachments)
                                content = f"{content} {att_text}".strip()
                            lines.append(f"[{m.created_at.isoformat()}] {m.author} : {content}")
                    if lines:
                        transcript = "\n".join(lines)
                        transcript_file = discord.File(
                            fp=io.BytesIO(transcript.encode("utf-8")),
                            filename=f"transcript_{self.thread_id}.txt"
                        )
            except Exception as e:
                pass

            # Send report with transcript if available
            try:
                if transcript_file:
                    await report_ch.send(embed=embed, file=transcript_file)
                else:
                    await report_ch.send(embed=embed)
            except Exception as e:
                try:
                    await interaction.edit_original_response(content="⚠️ Error sending report to staff. Please contact a moderator.")
                except discord.errors.NotFound:
                    pass
                return

            # Confirm success to user
            try:
                await interaction.edit_original_response(content="✅ Report submitted successfully. Thank you for helping keep the community safe.")
            except discord.errors.NotFound:
                pass

        except Exception as e:
            try:
                await interaction.edit_original_response(content="❌ Error submitting report. Please try again.")
            except discord.errors.NotFound:
                pass

# ----- Main Cog -----

class Matchmaker(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._locks: dict[int, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._paused: set[int] = set()
        self._watch: dict[int, Tuple[set[int], asyncio.Event, asyncio.Task]] = {}
        self.match_meta: dict[int, dict] = {}
        self._initialized = False
        self._cleanup_lock = asyncio.Lock()
        self._roles_cache = MemberRoleCache(max_size=10000, ttl_seconds=300)
        self._on_ready_done = False

    def cog_unload(self):
        # Stop background loops safely
        try:
            if self.match_loop.is_running():
                self.match_loop.cancel()
        except Exception:
            pass
        try:
            if self.queue_panel_loop.is_running():
                self.queue_panel_loop.cancel()
        except Exception:
            pass
        try:
            if self.cleanup_loop.is_running():
                self.cleanup_loop.cancel()
        except Exception:
            pass
        try:
            if self.dm_cleanup_loop.is_running():
                self.dm_cleanup_loop.cancel()
        except Exception:
            pass
        # Close DB connections (fire-and-forget; aiosqlite close is async, run in task)
        try:
            asyncio.create_task(db_pool.close())
        except Exception:
            # If no loop available, ignore; process exit will reclaim
            pass

    # ----- Helpers for thread actions -----
    def _get_thread_meta(self, thread_id: int):
        return self.match_meta.get(thread_id)

    def _get_other_id(self, thread_id: int, user_id: int) -> Optional[int]:
        meta = self._get_thread_meta(thread_id)
        if not meta:
            return None
        pairs = meta.get("pairs", [])
        other = next((uid for uid in pairs if uid != user_id), None)
        return other

    async def _safe_get_thread(self, guild: discord.Guild, thread_id: int) -> Optional[discord.Thread]:
        th = guild.get_thread(thread_id)
        if isinstance(th, discord.Thread):
            return th
        # Fallback to fetch when not cached
        try:
            ch = await guild.fetch_channel(thread_id)
            return ch if isinstance(ch, discord.Thread) else None
        except Exception:
            return None

    async def _close_match_row(self, thread_id: int):
        try:
            async with db_pool.get() as db:
                await db.execute(
                    "UPDATE matches SET closed_at=?, status='closed' WHERE thread_id=?",
                    (int(time.time()), thread_id)
                )
                await db.commit()
        except Exception:
            pass

    # Don't start the match loop until database is ready
    @commands.Cog.listener()
    async def on_ready(self):
        try:
            # Prevent double init in case on_ready fires multiple times
            if self._on_ready_done:
                return
            # Initialize database pool first
            await db_pool.initialize()
            async with db_pool.get() as db:
                await db.execute("""
                CREATE TABLE IF NOT EXISTS guild_config(
                    guild_id INTEGER PRIMARY KEY,
                    parent_channel_id INTEGER,
                    report_channel_id INTEGER,
                    next_room_number INTEGER DEFAULT 1
                )""")
                await db.execute("""
                CREATE TABLE IF NOT EXISTS waiting_queue(
                    guild_id INTEGER,
                    user_id INTEGER,
                    enqueued_at INTEGER,
                    priority_score INTEGER DEFAULT 0,
                    boost_until INTEGER,
                    PRIMARY KEY(guild_id, user_id)
                )""")
                await db.execute("CREATE INDEX IF NOT EXISTS idx_waiting_queue_guild_enqueued ON waiting_queue(guild_id, enqueued_at)")
                await db.execute("CREATE INDEX IF NOT EXISTS idx_waiting_queue_guild_priority ON waiting_queue(guild_id, priority_score DESC)")
                await db.execute("""
                CREATE TABLE IF NOT EXISTS matches(
                    thread_id INTEGER PRIMARY KEY,
                    guild_id INTEGER,
                    user1_id INTEGER,
                    user2_id INTEGER,
                    created_at INTEGER,
                    last_activity INTEGER,
                    closed_at INTEGER,
                    status TEXT,
                    close_reason TEXT
                )""")
                
                # Add close_reason column if it doesn't exist (for existing installations)
                try:
                    await db.execute("ALTER TABLE matches ADD COLUMN close_reason TEXT")
                except Exception:
                    pass  # Column probably already exists
                await db.execute("""
                CREATE TABLE IF NOT EXISTS recent_blocks(
                    guild_id INTEGER,
                    user1_id INTEGER,
                    user2_id INTEGER,
                    blocked_until INTEGER,
                    PRIMARY KEY(guild_id, user1_id, user2_id)
                )""")
                await db.execute("CREATE INDEX IF NOT EXISTS idx_recent_blocks_guild_until ON recent_blocks(guild_id, blocked_until)")
                await db.execute("""
                CREATE TABLE IF NOT EXISTS match_skips(
                    guild_id INTEGER,
                    thread_id INTEGER,
                    user_id INTEGER,
                    skipped_at INTEGER,
                    PRIMARY KEY(guild_id, thread_id, user_id)
                )""")
                await db.execute("CREATE INDEX IF NOT EXISTS idx_match_skips_guild ON match_skips(guild_id, skipped_at)")
                await db.execute("""
                CREATE TABLE IF NOT EXISTS queue_history(
                    guild_id INTEGER,
                    user_id INTEGER,
                    enqueued_at INTEGER,
                    matched_at INTEGER
                )""")
                await db.execute("""
                CREATE TABLE IF NOT EXISTS queue_panels(
                    guild_id INTEGER PRIMARY KEY,
                    channel_id INTEGER,
                    message_id INTEGER
                )""")
                await db.execute("""
                CREATE TABLE IF NOT EXISTS pending_deletions(
                    thread_id INTEGER PRIMARY KEY,
                    guild_id INTEGER,
                    delete_after INTEGER
                )""")
                await db.execute("CREATE INDEX IF NOT EXISTS idx_pending_deletions_time ON pending_deletions(delete_after)")
                await db.execute("""
                CREATE TABLE IF NOT EXISTS user_prefs(
                    user_id INTEGER PRIMARY KEY,
                    dm_enabled INTEGER DEFAULT 1
                )""")
                await db.execute("""
                CREATE TABLE IF NOT EXISTS dm_messages(
                    message_id INTEGER PRIMARY KEY,
                    channel_id INTEGER,
                    user_id INTEGER,
                    delete_after INTEGER
                )""")
                await db.execute("CREATE INDEX IF NOT EXISTS idx_dm_messages_time ON dm_messages(delete_after)")
                await db.commit()

            # Register persistent views
            self.bot.add_view(MatchPanel(self))
            # Re-attach thread controls for open matches
            try:
                async with db_pool.get() as db:
                    open_rows = await (await db.execute("SELECT thread_id, guild_id FROM matches WHERE status='open'")).fetchall()
                for r in open_rows or []:
                    tid = int(r["thread_id"]) if isinstance(r, aiosqlite.Row) else int(r[0])
                    gid = int(r["guild_id"]) if isinstance(r, aiosqlite.Row) else int(r[1])
                    guild = self.bot.get_guild(gid)
                    if guild:
                        th = await self._safe_get_thread(guild, tid)
                        if th:
                            self.bot.add_view(ThreadControls(self, gid, tid))
            except Exception:
                pass

            # Start the match loop only after database is ready
            self._initialized = True
            if not self.match_loop.is_running():
                self.match_loop.start()
            self._on_ready_done = True
        except Exception:
            raise

    # ----- Queue Helpers -----

    async def calculate_priority(self, guild_id: int, user_id: int) -> int:
        async with db_pool.get() as db:
            cur = await db.execute(
                "SELECT enqueued_at, boost_until FROM waiting_queue WHERE guild_id=? AND user_id=?",
                (guild_id, user_id)
            )
            row = await cur.fetchone()
            if not row:
                return 0
            wait = int(time.time()) - int(row["enqueued_at"])
            score = wait // 60
            return int(score)

    async def enqueue(self, guild_id: int, user_id: int):
        # Input validation
        if not isinstance(guild_id, int) or guild_id <= 0:
            raise ValueError(f"Invalid guild_id: {guild_id}")
        if not isinstance(user_id, int) or user_id <= 0:
            raise ValueError(f"Invalid user_id: {user_id}")

        ts = int(time.time())
        max_retries = 3
        last_error = None
        timeout = aiosqlite.Connection.timeout = 5.0  # 5 second timeout for operations
        
        # First check if user is already in queue
        try:
            async with db_pool.get() as db:
                cur = await db.execute(
                    "SELECT 1 FROM waiting_queue WHERE guild_id=? AND user_id=?",
                    (guild_id, user_id)
                )
                if await cur.fetchone():
                    raise ValueError("You are already in the queue!")
        except ValueError:
            raise
        except Exception as e:
            pass
            
        for attempt in range(max_retries):
            try:
                async with db_pool.get() as db:
                    try:
                        await db.execute("BEGIN TRANSACTION")
                        wait = 0  # New entry starts with 0 wait time
                        score = wait // 60
                        await db.execute("""
                        INSERT INTO waiting_queue(guild_id, user_id, enqueued_at, priority_score)
                        VALUES(?, ?, ?, ?)
                        ON CONFLICT(guild_id, user_id) DO UPDATE SET
                            enqueued_at=excluded.enqueued_at,
                            priority_score=excluded.priority_score
                        """, (guild_id, user_id, ts, score))
                        await db.commit()
                        return
                    except Exception as e:
                        await db.execute("ROLLBACK")
                        raise e
            except aiosqlite.Error as e:
                last_error = e
                if attempt < max_retries - 1:
                    await asyncio.sleep(0.1 * (attempt + 1))
                continue
            except Exception as e:
                last_error = e
                if attempt < max_retries - 1:
                    await asyncio.sleep(0.1 * (attempt + 1))
                continue
        
        raise last_error

    async def get_position_and_eta(self, guild_id: int, user_id: int) -> Tuple[int, int, int]:
        # returns: (position, total_waiting, eta_seconds)
        async with db_pool.get() as db:
            rows = await (await db.execute(
                "SELECT user_id, enqueued_at, priority_score FROM waiting_queue WHERE guild_id=?",
                (guild_id,)
            )).fetchall()
        if not rows:
            return (0, 0, 0)
        cands: List[Dict[str, Any]] = [dict(r) for r in rows]
        # Sort by priority score and enqueued time
        cands.sort(key=lambda x: (-int(x["priority_score"]), int(x["enqueued_at"])))
        total = len(cands)
        position = next((i + 1 for i, r in enumerate(cands) if int(r["user_id"]) == int(user_id)), 0)
        if position == 0:
            # Not in queue
            return (0, total, 0)
        # crude ETA: assume ~10s per match attempt cycle and 2 users per match
        # users ahead divided by 2 gives rough number of pairs before this user
        ahead = max(0, position - 1)
        pair_slots = max(1, math.ceil(ahead / 2))
        eta_seconds = pair_slots * 20  # 20 seconds per pairing window rough
        return (position, total, eta_seconds)

    async def update_priority(self, guild_id: int, user_id: int):
        score = await self.calculate_priority(guild_id, user_id)
        async with db_pool.get() as db:
            await db.execute("""
            UPDATE waiting_queue
               SET priority_score=?
             WHERE guild_id=? AND user_id=?
            """, (score, guild_id, user_id))
            await db.commit()

    async def dequeue_pair(self, guild: discord.Guild):
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Get candidates
                async with db_pool.get() as db:
                    rows = await db.execute(
                        "SELECT user_id, enqueued_at, boost_until FROM waiting_queue WHERE guild_id=? ORDER BY enqueued_at ASC LIMIT ?",
                        (guild.id, MAX_DB_CANDIDATES)
                    )
                    candidates = await rows.fetchall()
                # candidates fetched
                if len(candidates) < 2:
                    return None

                cands: List[Dict[str, Any]] = [dict(r) for r in candidates]
                # Compute priority inline to avoid per-candidate DB calls
                now_ts = int(time.time())
                # Get recent skips to boost their priority
                skips = await (await db.execute("""
                    SELECT user_id, MAX(skipped_at) as last_skip
                    FROM match_skips 
                    WHERE guild_id=? AND skipped_at>?
                    GROUP BY user_id
                """, (guild.id, now_ts - 300))).fetchall()  # Last 5 minutes
                
                recent_skips = {int(r["user_id"]): int(r["last_skip"]) for r in skips}
                
                for c in cands:
                    user_id = int(c["user_id"])
                    waited = max(0, now_ts - int(c["enqueued_at"]))
                    base_score = waited // 60
                    
                    # Give very high priority to users who just skipped
                    if user_id in recent_skips:
                        # Add large bonus (equivalent to waiting 2 hours)
                        skip_bonus = 120
                        c["priority_score"] = base_score + skip_bonus
                    else:
                        c["priority_score"] = base_score
                
                # Sort by priority score first, then by enqueue time
                cands.sort(key=lambda x: (-x["priority_score"], x["enqueued_at"]))

                # Clean expired blocks and load blocks
                now = int(time.time())
                async with db_pool.get() as db:
                    await db.execute("DELETE FROM recent_blocks WHERE guild_id=? AND blocked_until<=?", (guild.id, now))
                    await db.commit()
                    blk = await (await db.execute(
                        "SELECT user1_id, user2_id FROM recent_blocks WHERE guild_id=? AND blocked_until>?",
                        (guild.id, now)
                    )).fetchall()
                    blocks = {(int(r["user1_id"]), int(r["user2_id"])) for r in blk}
                    
                    # Get users who have been matched in the last 24 hours and didn't skip
                    day_ago = now - (24 * 60 * 60)
                    matched = await (await db.execute("""
                        SELECT user1_id, user2_id, status FROM matches 
                        WHERE guild_id=? AND created_at>? AND status='open'
                        AND thread_id NOT IN (
                            SELECT thread_id FROM match_skips 
                            WHERE guild_id=? AND skipped_at>?
                        )
                        """, (guild.id, day_ago, guild.id, day_ago))).fetchall()
                    recently_matched = set()
                    for match in matched:
                        recently_matched.add(int(match["user1_id"]))
                        recently_matched.add(int(match["user2_id"]))

                async def get_roles(uid: int) -> Optional[set[int]]:
                    return await self._roles_cache.get_roles(guild, int(uid))

                def ok(pref: str, roles: Optional[set[int]]) -> bool:
                        # All matches are now random, no role preferences
                        return roles is not None

                # Find matching pair
                # searching for matches
                for i, u1 in enumerate(cands[:SCAN_WINDOW]):
                    for u2 in cands[i + 1:i + 1 + SCAN_WINDOW]:
                        pair_sorted = tuple(sorted((int(u1["user_id"]), int(u2["user_id"])) ))
                        if pair_sorted in blocks:
                            continue
                        # Skip if either user has been matched in the last 24 hours
                        if int(u1["user_id"]) in recently_matched or int(u2["user_id"]) in recently_matched:
                            continue
                        r1 = await get_roles(int(u1["user_id"]))
                        r2 = await get_roles(int(u2["user_id"]))
                        if not r1 or not r2:
                            continue
                        # All matches are random, so just check both have roles
                        if ok("random", r2) and ok("random", r1):
                            return (u1, u2)
                return None
            except Exception:
                if attempt == max_retries - 1:
                    return None
                await asyncio.sleep(0.1 * (attempt + 1))

    async def _ensure_thread_perms(self, channel: discord.TextChannel, member: discord.Member):
        try:
            perms = channel.permissions_for(member)
            # If they can't send in threads, grant per-user override for this channel
            if hasattr(perms, "send_messages_in_threads") and perms.send_messages_in_threads is False:
                try:
                    await channel.set_permissions(member, send_messages_in_threads=True, view_channel=True)
                except Exception:
                    pass
        except Exception:
            pass

    async def _grant_thread_overwrites(self, thread: discord.Thread, member: discord.Member):
        try:
            ow = discord.PermissionOverwrite()
            # Ensure visibility and full messaging/media privileges inside the thread
            for name in [
                "view_channel",
                "send_messages",
                "attach_files",
                "embed_links",
                "add_reactions",
                "use_external_emojis",
                "use_external_stickers",
                "send_voice_messages",
                "send_tts_messages",
                "use_application_commands",
            ]:
                try:
                    setattr(ow, name, True)
                except Exception:
                    pass
            await thread.set_permissions(member, overwrite=ow)
        except Exception:
            pass

    async def block_pair(self, guild_id: int, a: int, b: int, minutes: int = SKIP_BLOCK_MINUTES):
        u1, u2 = sorted((int(a), int(b)))
        until = int(time.time()) + minutes * 60
        max_retries = 3
        for attempt in range(max_retries):
            try:
                async with db_pool.get() as db:
                    await db.execute("""
                    INSERT INTO recent_blocks(guild_id, user1_id, user2_id, blocked_until)
                    VALUES(?, ?, ?, ?)
                    ON CONFLICT(guild_id, user1_id, user2_id)
                    DO UPDATE SET blocked_until=excluded.blocked_until
                    """, (guild_id, u1, u2, until))
                    await db.commit()
                return
            except Exception:
                if attempt == max_retries - 1:
                    raise
                await asyncio.sleep(0.1 * (attempt + 1))

    # ----- Matching Loop -----

    @tasks.loop(seconds=5)
    async def match_loop(self):
        if not self._initialized:
            return
        for guild in list(self.bot.guilds):
            if guild.id in self._paused:
                continue
            lock = self._locks[guild.id]
            if lock.locked():
                continue
            asyncio.create_task(self._attempt_match(guild))

    @tasks.loop(minutes=1)  # Check every minute for more precise timing
    async def cleanup_inactive_threads(self):
        """Cleanup inactive threads after exactly 30 minutes of inactivity"""
        if not self._initialized:
            return
            
        try:
            now = int(time.time())
            inactive_threshold = now - (30 * 60)  # Exactly 30 minutes of inactivity
            
            for guild in self.bot.guilds:
                try:
                    async with db_pool.get() as db:
                        # Get inactive threads that have been inactive for exactly 30 minutes
                        rows = await (await db.execute("""
                            SELECT m.thread_id, m.guild_id, m.user1_id, m.user2_id, m.last_activity
                            FROM matches m
                            WHERE m.status = 'open' 
                            AND m.last_activity < ?
                            AND m.last_activity > ?  -- Ensure we haven't tried to delete it before
                            AND m.thread_id NOT IN (
                                SELECT thread_id FROM pending_deletions
                            )
                        """, (inactive_threshold, inactive_threshold - 60))).fetchall()
                        
                        for row in rows:
                            thread_id = int(row["thread_id"])
                            try:
                                # Get the thread
                                thread = await self._safe_get_thread(guild, thread_id)
                                if thread:
                                    # Notify users before deleting
                                    try:
                                        warning_embed = discord.Embed(
                                            title="Thread Closed - Inactivity",
                                            description="This thread has been closed due to 30 minutes of inactivity.",
                                            color=0xE74C3C
                                        )
                                        await thread.send(embed=warning_embed)
                                        
                                        # Try to notify the users via DM
                                        for user_id in [int(row["user1_id"]), int(row["user2_id"])]:
                                            try:
                                                member = await guild.fetch_member(user_id)
                                                if member:
                                                    await notif_manager.send(
                                                        member,
                                                        "Your match thread was closed due to 30 minutes of inactivity. Feel free to queue again!",
                                                        "inactive"
                                                    )
                                            except Exception:
                                                continue
                                                
                                        # Delete the thread
                                        await thread.delete(reason="Inactive for 30 minutes")
                                    except Exception:
                                        # If we can't send the message, just delete the thread
                                        await thread.delete(reason="Inactive for 30 minutes")
                                
                                # Update the match status
                                await db.execute("""
                                    UPDATE matches 
                                    SET status = 'closed', 
                                        closed_at = ?,
                                        close_reason = 'inactivity'
                                    WHERE thread_id = ?
                                """, (now, thread_id))
                                
                                # Clean up metadata
                                if thread_id in self.match_meta:
                                    self.match_meta.pop(thread_id, None)
                                
                            except Exception as e:
                                continue
                        
                        await db.commit()
                        
                except Exception as e:
                    continue
                    
        except asyncio.CancelledError:
            # Task cancelled during reload/shutdown - expected
            raise
        except Exception as e:
            pass

    @cleanup_inactive_threads.before_loop
    async def before_cleanup_loop(self):
        await self.bot.wait_until_ready()

    @match_loop.before_loop
    async def before_loop(self):
        await self.bot.wait_until_ready()
        if not self.queue_panel_loop.is_running():
            self.queue_panel_loop.start()
        if not self.cleanup_loop.is_running():
            self.cleanup_loop.start()
        if not self.dm_cleanup_loop.is_running():
            self.dm_cleanup_loop.start()
        if not self.cleanup_inactive_threads.is_running():
            self.cleanup_inactive_threads.start()

    async def _attempt_match(self, guild: discord.Guild):
        async with self._locks[guild.id]:
            try:
                cfg = await self.get_config(guild.id)
                if not cfg or not cfg.get("parent_channel_id"):
                    return
                channel = guild.get_channel(int(cfg["parent_channel_id"]))
                if not isinstance(channel, discord.TextChannel):
                    return

                pair = await self.dequeue_pair(guild)
                if not pair:
                    return

                u1, u2 = pair  # dicts
                # Member fetch with fallback
                try:
                    m1 = guild.get_member(int(u1["user_id"])) or await guild.fetch_member(int(u1["user_id"]))
                except Exception:
                    m1 = None
                try:
                    m2 = guild.get_member(int(u2["user_id"])) or await guild.fetch_member(int(u2["user_id"]))
                except Exception:
                    m2 = None
                if not m1 or not m2:
                    return

                room_no = await self.consume_room_number(guild.id)
                # Ensure they can type in threads under this channel
                try:
                    await self._ensure_thread_perms(channel, m1)
                    await self._ensure_thread_perms(channel, m2)
                except Exception:
                    pass
                thread = await channel.create_thread(
                    name=f"Room {room_no}",
                    type=discord.ChannelType.private_thread,
                    auto_archive_duration=1440,  # 1 day
                    invitable=False
                )

                try:
                    await thread.add_user(m1)
                except Exception:
                    pass
                try:
                    await thread.add_user(m2)
                except Exception:
                    pass

                # Grant comprehensive thread permissions for both users
                try:
                    await self._grant_thread_overwrites(thread, m1)
                    await self._grant_thread_overwrites(thread, m2)
                except Exception:
                    pass

                self.match_meta[thread.id] = {
                    "guild_id": guild.id,
                    "pairs": [int(u1["user_id"]), int(u2["user_id"])],
                    "room_no": room_no,
                }

                async with db_pool.get() as db:
                    now_ts = int(time.time())
                    await db.execute("""
                    INSERT INTO matches(thread_id, guild_id, user1_id, user2_id, created_at, last_activity, status)
                    VALUES(?,?,?,?,?,?, 'open')
                    """, (thread.id, guild.id, u1["user_id"], u2["user_id"], now_ts, now_ts))
                    # Remove from queue and write history now that thread exists
                    await db.execute(
                        "DELETE FROM waiting_queue WHERE guild_id=? AND user_id IN (?, ?)",
                        (guild.id, u1["user_id"], u2["user_id"])
                    )
                    now_ts = int(time.time())
                    await db.execute(
                        "INSERT INTO queue_history(guild_id, user_id, enqueued_at, matched_at) VALUES(?, ?, ?, ?)",
                        (guild.id, u1["user_id"], u1["enqueued_at"], now_ts)
                    )
                    await db.execute(
                        "INSERT INTO queue_history(guild_id, user_id, enqueued_at, matched_at) VALUES(?, ?, ?, ?)",
                        (guild.id, u2["user_id"], u2["enqueued_at"], now_ts)
                    )
                    await db.commit()

                embed = discord.Embed(
                    title=f"Room {room_no}",
                    description=f"This private room is just for you two. Be respectful, have fun, and enjoy your chat!",
                    color=EMBED_COLOR
                )
                # Register a persistent view instance for these controls so it won't disable on restart
                controls = ThreadControls(self, guild.id, thread.id)
                try:
                    self.bot.add_view(controls)
                except Exception:
                    pass
                
                # Send embed without banner in thread
                try:
                    await thread.send(embed=embed, view=controls)
                except Exception:
                    # Fallback
                    await thread.send(embed=embed, view=controls)

                # DM both users to notify about the match (ignored if DMs are closed)
                try:
                    await notif_manager.send(m1, "Match found! Check your new thread.", "match")
                except Exception:
                    pass
                try:
                    await notif_manager.send(m2, "Match found! Check your new thread.", "match")
                except Exception:
                    pass

            except Exception:
                return

    # ----- Queue Panel Helpers -----

    async def _get_queue_counts(self, guild_id: int) -> Dict[str, int]:
        async with db_pool.get() as db:
            rows = await (await db.execute(
                "SELECT COUNT(*) AS c FROM waiting_queue WHERE guild_id=?",
                (guild_id,)
            )).fetchall()
        count = int(rows[0]["c"]) if rows else 0
        return {"total": count}

    def _build_queue_embed(self, guild: discord.Guild, counts: Dict[str, int]) -> discord.Embed:
        embed = discord.Embed(title=f"Queue — {guild.name}", color=EMBED_COLOR)
        embed.add_field(name="Users Waiting", value=str(counts.get("total", 0)), inline=True)
        embed.set_footer(text="Updates every ~20s")
        return embed

    # ----- Cleanup Loop -----

    @tasks.loop(seconds=60)
    async def cleanup_loop(self):
        if not self._initialized:
            return
        async with self._cleanup_lock:
            try:
                now_ts = int(time.time())
                inactivity_threshold = now_ts - (10 * 60)  # 10 minutes ago
                
                async with db_pool.get() as db:
                    # Check for scheduled deletions
                    rows = await (await db.execute(
                        "SELECT thread_id, guild_id, delete_after FROM pending_deletions WHERE delete_after<=?",
                        (now_ts,)
                    )).fetchall()
                    
                    # Check for inactive threads
                    inactive_rows = await (await db.execute("""
                        SELECT m.thread_id, m.guild_id, m.user1_id, m.user2_id 
                        FROM matches m 
                        WHERE m.status = 'open' 
                        AND m.last_activity < ?
                        """, (inactivity_threshold,)
                    )).fetchall()
                    
                    # Add inactive threads to the rows for deletion
                    rows.extend(inactive_rows)
                for r in rows or []:
                    tid = int(r["thread_id"]) if isinstance(r, aiosqlite.Row) else int(r[0])
                    gid = int(r["guild_id"]) if isinstance(r, aiosqlite.Row) else int(r[1])
                    guild = self.bot.get_guild(gid)
                    if not guild:
                        continue
                    thread = guild.get_thread(tid)
                    if not isinstance(thread, discord.Thread):
                        try:
                            ch = await guild.fetch_channel(tid)
                            thread = ch if isinstance(ch, discord.Thread) else None
                        except Exception:
                            thread = None
                    if thread:
                        try:
                            await thread.delete(reason="Scheduled cleanup (persistent)")
                        except Exception:
                            pass
                    try:
                        await self._close_match_row(tid)
                    except Exception:
                        pass
                    self.match_meta.pop(tid, None)
                    try:
                        async with db_pool.get() as db:
                            await db.execute("DELETE FROM pending_deletions WHERE thread_id=?", (tid,))
                            await db.commit()
                    except Exception:
                        pass
            except asyncio.CancelledError:
                raise
            except Exception:
                return

    @tasks.loop(seconds=30)
    async def dm_cleanup_loop(self):
        if not self._initialized:
            return
        try:
            now_ts = int(time.time())
            async with db_pool.get() as db:
                rows = await (await db.execute(
                    "SELECT message_id, channel_id FROM dm_messages WHERE delete_after<=?",
                    (now_ts,)
                )).fetchall()
            for r in rows or []:
                mid = int(r[0]) if not isinstance(r, aiosqlite.Row) else int(r["message_id"])
                chid = int(r[1]) if not isinstance(r, aiosqlite.Row) else int(r["channel_id"])
                # Resolve DM channel via cache or fetch
                chan = self.bot.get_channel(chid)
                if chan is None:
                    try:
                        chan = await self.bot.fetch_channel(chid)
                    except Exception:
                        chan = None
                if isinstance(chan, discord.DMChannel):
                    try:
                        msg = await chan.fetch_message(mid)
                        await msg.delete()
                    except Exception:
                        pass
                try:
                    async with db_pool.get() as db:
                        await db.execute("DELETE FROM dm_messages WHERE message_id=?", (mid,))
                        await db.commit()
                except Exception:
                    pass
        except asyncio.CancelledError:
            raise
        except Exception:
            return

    async def _get_queue_panel_row(self, guild_id: int) -> Optional[dict]:
        async with db_pool.get() as db:
            cur = await db.execute("SELECT * FROM queue_panels WHERE guild_id=?", (guild_id,))
            row = await cur.fetchone()
            return dict(row) if row else None

    async def _upsert_queue_panel_row(self, guild_id: int, channel_id: int, message_id: int):
        async with db_pool.get() as db:
            await db.execute(
                "INSERT INTO queue_panels(guild_id, channel_id, message_id) VALUES(?,?,?)\n                 ON CONFLICT(guild_id) DO UPDATE SET channel_id=excluded.channel_id, message_id=excluded.message_id",
                (guild_id, channel_id, message_id)
            )
            await db.commit()

    async def _delete_queue_panel_row(self, guild_id: int):
        async with db_pool.get() as db:
            await db.execute("DELETE FROM queue_panels WHERE guild_id=?", (guild_id,))
            await db.commit()

    @tasks.loop(seconds=20)
    async def queue_panel_loop(self):
        if not self._initialized:
            return
        try:
            for guild in list(self.bot.guilds):
                try:
                    row = await self._get_queue_panel_row(guild.id)
                    if not row:
                        continue
                    channel = guild.get_channel(int(row["channel_id"]))
                    if not isinstance(channel, discord.TextChannel):
                        continue
                    try:
                        msg = await channel.fetch_message(int(row["message_id"]))
                    except Exception:
                        await self._delete_queue_panel_row(guild.id)
                        continue
                    counts = await self._get_queue_counts(guild.id)
                    embed = self._build_queue_embed(guild, counts)
                    try:
                        await msg.edit(embed=embed)
                    except Exception:
                        pass
                except Exception:
                    pass
        except asyncio.CancelledError:
            raise
        except Exception:
            pass

    # ----- Configuration -----

    async def get_config(self, guild_id: int) -> Optional[dict]:
        async with db_pool.get() as db:
            cur = await db.execute("SELECT * FROM guild_config WHERE guild_id=?", (guild_id,))
            row = await cur.fetchone()
            return dict(row) if row else None

    async def set_parent_channel(self, gid: int, ch: int):
        async with db_pool.get() as db:
            await db.execute("INSERT OR IGNORE INTO guild_config(guild_id) VALUES(?)", (gid,))
            await db.execute("UPDATE guild_config SET parent_channel_id=? WHERE guild_id=?", (ch, gid))
            await db.commit()

    async def set_report_channel(self, gid: int, rc: int):
        async with db_pool.get() as db:
            await db.execute("INSERT OR IGNORE INTO guild_config(guild_id) VALUES(?)", (gid,))
            await db.execute("UPDATE guild_config SET report_channel_id=? WHERE guild_id=?", (rc, gid))
            await db.commit()

    async def consume_room_number(self, gid: int) -> int:
        cfg = await self.get_config(gid)
        num = int(cfg["next_room_number"]) if cfg and cfg.get("next_room_number") else 1
        async with db_pool.get() as db:
            # Ensure row exists before updating, to avoid no-op updates
            await db.execute("INSERT OR IGNORE INTO guild_config(guild_id) VALUES(?)", (gid,))
            await db.execute("UPDATE guild_config SET next_room_number=? WHERE guild_id=?", (num + 1, gid))
            await db.commit()
        return num

    # ----- Admin Commands -----

    @app_commands.guild_only()
    @app_commands.command(name="mm", description="Configure matchmaking")
    @app_commands.describe(
        action="setup/configure/report_channel/clear/pause/resume/stats/queue/queue_panel",
        channel="Text Channel",
        report="Report Channel",
        days="Stat Days"
    )
    @app_commands.choices(action=[
        app_commands.Choice(name="setup", value="setup"),
        app_commands.Choice(name="configure", value="configure"),
        app_commands.Choice(name="report_channel", value="report_channel"),
        app_commands.Choice(name="clear", value="clear"),
        app_commands.Choice(name="pause", value="pause"),
        app_commands.Choice(name="resume", value="resume"),
        app_commands.Choice(name="stats", value="stats"),
        app_commands.Choice(name="queue", value="queue"),
        app_commands.Choice(name="queue_panel", value="queue_panel"),
    ])
    async def mm(self,
                 interaction: discord.Interaction,
                 action: app_commands.Choice[str],
                 channel: Optional[discord.TextChannel] = None,
                 report: Optional[discord.TextChannel] = None,
                 days: int = 7):
        try:
            # Admin check
            if not interaction.user.guild_permissions.administrator:
                return await interaction.response.send_message("You need administrator permissions to use this command.", ephemeral=True)

            if interaction.response.is_done() is False:
                await interaction.response.defer(ephemeral=True)

            act = action.value
            guild = interaction.guild
        except Exception:
            try:
                if not interaction.response.is_done():
                    await interaction.response.send_message("An error occurred. Please try again.", ephemeral=True)
            except Exception:
                pass
            return

        if act == "setup":
            if not all([channel, report]):
                return await interaction.edit_original_response(content="Provide channel and report channel.")
            await self.set_parent_channel(guild.id, channel.id)
            await self.set_report_channel(guild.id, report.id)

            embed = discord.Embed(
                title="Private Rooms",
                description="Get paired with a random stranger in a private room for a one-on-one chat.",
                color=EMBED_COLOR
            )
            # Optional banner
            try:
                if os.path.exists("banner.gif"):
                    embed.set_image(url="attachment://banner.gif")
                    with open("banner.gif", "rb") as f:
                        banner_file = discord.File(f, filename="banner.gif")
                        await channel.send(embed=embed, view=MatchPanel(self), file=banner_file)
                else:
                    await channel.send(embed=embed, view=MatchPanel(self))
            except Exception:
                await channel.send(embed=embed, view=MatchPanel(self))

            return await interaction.edit_original_response(content="✅ Setup complete! Matchmaking panel created.")

        if act == "configure":
            if not channel:
                return await interaction.edit_original_response(content="Provide channel.")
            await self.set_parent_channel(guild.id, channel.id)

            embed = discord.Embed(
                title="Matchmaking Panel",
                description="Click the button below to find your match.",
                color=EMBED_COLOR
            )
            try:
                if os.path.exists("banner.gif"):
                    embed.set_image(url="attachment://banner.gif")
                    with open("banner.gif", "rb") as f:
                        banner_file = discord.File(f, filename="banner.gif")
                        await channel.send(embed=embed, view=MatchPanel(self), file=banner_file)
                else:
                    await channel.send(embed=embed, view=MatchPanel(self))
            except Exception:
                await channel.send(embed=embed, view=MatchPanel(self))

            return await interaction.edit_original_response(content=f"✅ Configured to {channel.mention}")

        if act == "report_channel":
            if not report:
                return await interaction.edit_original_response(content="Provide a report channel.")
            await self.set_report_channel(guild.id, report.id)
            return await interaction.edit_original_response(content=f"✅ Report channel set to {report.mention}")

        if act == "clear":
            async with db_pool.get() as db:
                await db.execute("DELETE FROM waiting_queue WHERE guild_id=?", (guild.id,))
                await db.execute("DELETE FROM recent_blocks WHERE guild_id=?", (guild.id,))
                await db.commit()
            return await interaction.edit_original_response(content="🧹 Cleared waiting queue and recent blocks.")

        if act == "pause":
            self._paused.add(guild.id)
            return await interaction.edit_original_response(content="⏸️ Matchmaking paused for this server.")

        if act == "resume":
            self._paused.discard(guild.id)
            return await interaction.edit_original_response(content="▶️ Matchmaking resumed for this server.")

        if act == "stats":
            try:
                since = int(time.time()) - days * 86400
                async with db_pool.get() as db:
                    cur1 = await db.execute(
                        "SELECT COUNT(*) as c FROM matches WHERE guild_id=? AND created_at>=?",
                        (guild.id, since),
                    )
                    total_matches = int((await cur1.fetchone())["c"])
                    cur2 = await db.execute(
                        "SELECT COUNT(*) as c FROM waiting_queue WHERE guild_id=?",
                        (guild.id,),
                    )
                    queued_now = int((await cur2.fetchone())["c"])
                embed = discord.Embed(title="Matchmaking Stats", color=EMBED_COLOR)
                embed.add_field(name="Days", value=str(days), inline=True)
                embed.add_field(name="Matches Created", value=str(total_matches), inline=True)
                embed.add_field(name="Queued Now", value=str(queued_now), inline=True)
                return await interaction.edit_original_response(embed=embed)
            except Exception:
                return await interaction.edit_original_response(content="❌ Failed to compute stats.")

        if act == "queue":
            async with db_pool.get() as db:
                rows = await (await db.execute(
                    "SELECT COUNT(*) as c FROM waiting_queue WHERE guild_id=?",
                    (guild.id,),
                )).fetchall()
            count = int(rows[0]["c"]) if rows else 0
            embed = discord.Embed(title="Current Queue", color=EMBED_COLOR)
            embed.add_field(name="Users Waiting", value=str(count), inline=True)
            return await interaction.edit_original_response(embed=embed)

        if act == "queue_panel":
            if not channel:
                return await interaction.edit_original_response(content="Provide channel to host the queue panel.")
            counts = await self._get_queue_counts(guild.id)
            embed = self._build_queue_embed(guild, counts)
            # Try to reuse existing message if present in same channel
            row = await self._get_queue_panel_row(guild.id)
            panel_msg = None
            if row:
                if int(row["channel_id"]) == channel.id:
                    try:
                        panel_msg = await channel.fetch_message(int(row["message_id"]))
                    except Exception:
                        panel_msg = None
            if panel_msg:
                await panel_msg.edit(embed=embed)
                await self._upsert_queue_panel_row(guild.id, channel.id, panel_msg.id)
                return await interaction.edit_original_response(content=f"✅ Queue panel updated in {channel.mention}.")
            else:
                sent = await channel.send(embed=embed)
                await self._upsert_queue_panel_row(guild.id, channel.id, sent.id)
                return await interaction.edit_original_response(content=f"✅ Queue panel created in {channel.mention}.")

        return await interaction.edit_original_response(content="Unknown action.")

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if not isinstance(message.channel, discord.Thread):
            return
            
        # Check if this is one of our match threads
        thread_id = message.channel.id
        if thread_id not in self.match_meta:
            return
            
        # Update last activity timestamp
        now_ts = int(time.time())
        async with db_pool.get() as db:
            await db.execute("""
                UPDATE matches 
                SET last_activity = ? 
                WHERE thread_id = ? AND status = 'open'
                """, (now_ts, thread_id))
            await db.commit()

async def setup(bot: commands.Bot):
    await bot.add_cog(Matchmaker(bot))
