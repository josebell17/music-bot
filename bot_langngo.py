import os
import asyncio
import logging
import time
from dataclasses import dataclass
from collections import OrderedDict, defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

import discord
from discord.ext import commands
import yt_dlp


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [Workstation-Core] %(message)s",
)

logger = logging.getLogger("WorkstationProductionBot")


# ============================================================
# DISCORD
# ============================================================

intents = discord.Intents.default()
intents.message_content = True
intents.voice_states = True
intents.guilds = True
intents.guild_messages = True

bot = commands.Bot(
    command_prefix="!",
    intents=intents,
)


# ============================================================
# LOW RESOURCE CONFIG
# ============================================================

MAX_QUEUE_SIZE = 10
MAX_FAVORITES = 10

# Cache nhỏ để tiết kiệm RAM.
STREAM_CACHE_SIZE = 50
STREAM_CACHE_TTL = 240

# Với 0.1 CPU: 1 worker là hợp lý hơn 4 worker.
WORKER_COUNT = 1

EXTRACTION_TIMEOUT = 12
PLAYBACK_RETRY_COUNT = 2

IDLE_DISCONNECT_SECONDS = 60

# Không prefetch quá nhiều trên máy yếu.
ENABLE_PREFETCH = False


# ============================================================
# SINGLE WORKER
# ============================================================

WORKSTATION_POOL = ThreadPoolExecutor(
    max_workers=WORKER_COUNT,
    thread_name_prefix="YTWorker",
)


# ============================================================
# MEMORY
# ============================================================

class LimitedCache(OrderedDict):

    def __init__(self, maxsize=50):
        super().__init__()
        self.maxsize = maxsize

    def __getitem__(self, key):
        value = super().__getitem__(key)
        self.move_to_end(key)
        return value

    def get(self, key, default=None):
        if key not in self:
            return default

        value = super().__getitem__(key)
        self.move_to_end(key)
        return value

    def __setitem__(self, key, value):
        if key in self:
            self.move_to_end(key)

        super().__setitem__(key, value)

        while len(self) > self.maxsize:
            self.popitem(last=False)


@dataclass
class CachedStream:
    url: str
    title: str
    created_at: float


@dataclass
class Track:
    query: str
    title: str
    stream_url: str = ""


URL_CACHE = LimitedCache(STREAM_CACHE_SIZE)

queues = defaultdict(list)
volumes = defaultdict(lambda: 0.5)
user_collections = defaultdict(list)

guild_locks = defaultdict(asyncio.Lock)

sleep_tasks = {}
idle_tasks = {}

current_tracks = {}

# Generation/token giúp callback cũ không phá playback mới.
playback_generation = defaultdict(int)

track_retry_counter = defaultdict(int)


# ============================================================
# YT-DLP
# ============================================================

YTDL_OPTIONS = {
    "format": "bestaudio/best",
    "noplaylist": True,

    "quiet": True,
    "no_warnings": True,

    "nocheckcertificate": True,

    "socket_timeout": 10,

    "retries": 1,
    "fragment_retries": 1,

    "skip_unavailable_fragments": True,

    "cachedir": False,

    "ignoreerrors": False,

    # SoundCloud
    "extract_flat": False,
}


def create_ytdl():
    return yt_dlp.YoutubeDL(YTDL_OPTIONS)


# ============================================================
# FFMPEG
# ============================================================

FFMPEG_OPTIONS = {
    "before_options": (
        "-reconnect 1 "
        "-reconnect_streamed 1 "
        "-reconnect_delay_max 5 "
        "-nostdin"
    ),
    "options": (
        "-vn "
        "-sn "
        "-dn "
        "-bufsize 256k "
        "-ar 48000 "
        "-ac 2"
    ),
}


# ============================================================
# SECURITY
# ============================================================

class SecuritySanitizer:

    @staticmethod
    def sanitize_input(text: str) -> str:

        if not text:
            return ""

        text = str(text).strip()

        cleaned = "".join(
            ch
            for ch in text
            if ch.isalnum()
            or ch in " -_./:?&=+@%#(),[]'!"
        )

        return cleaned[:200].strip()


# ============================================================
# STREAM ENGINE
# ============================================================

class SelfHealingEngine:

    @staticmethod
    def cache_valid(item: CachedStream) -> bool:

        return (
            time.monotonic() - item.created_at
        ) < STREAM_CACHE_TTL


    @staticmethod
    async def resolve_stream(
        query: str,
        force_refresh: bool = False,
    ):

        safe_query = SecuritySanitizer.sanitize_input(query)

        if not safe_query:
            raise ValueError("Từ khóa không hợp lệ.")

        # ----------------------------------------------------
        # CACHE
        # ----------------------------------------------------

        cached = URL_CACHE.get(safe_query)

        if (
            cached
            and not force_refresh
            and SelfHealingEngine.cache_valid(cached)
        ):
            return cached.url, cached.title

        # ----------------------------------------------------
        # QUERY
        # ----------------------------------------------------

        if safe_query.startswith(("http://", "https://")):
            search_query = safe_query
        else:
            search_query = f"scsearch1:{safe_query}"

        loop = asyncio.get_running_loop()

        def worker():

            ytdl = create_ytdl()

            try:

                data = ytdl.extract_info(
                    search_query,
                    download=False,
                )

                if not data:
                    return None

                if "entries" in data:

                    entries = data.get("entries") or []

                    data = next(
                        (
                            entry
                            for entry in entries
                            if entry
                        ),
                        None,
                    )

                    if not data:
                        return None

                stream_url = data.get("url")
                title = data.get("title") or safe_query

                if not stream_url:
                    return None

                return stream_url, title

            except Exception as exc:

                logger.warning(
                    "[yt-dlp] %s",
                    exc,
                )

                return None

            finally:

                try:
                    ytdl.close()
                except Exception:
                    pass

        try:

            result = await asyncio.wait_for(
                loop.run_in_executor(
                    WORKSTATION_POOL,
                    worker,
                ),
                timeout=EXTRACTION_TIMEOUT,
            )

        except asyncio.TimeoutError:

            logger.warning(
                "[yt-dlp] Timeout: %s",
                safe_query,
            )

            raise RuntimeError(
                "SoundCloud phản hồi quá chậm."
            )

        if not result:

            raise RuntimeError(
                f"Không tìm thấy bài hát: {safe_query}"
            )

        stream_url, title = result

        URL_CACHE[safe_query] = CachedStream(
            url=stream_url,
            title=title,
            created_at=time.monotonic(),
        )

        return stream_url, title


    @staticmethod
    def purge_cache(query: str):

        query = SecuritySanitizer.sanitize_input(query)

        URL_CACHE.pop(query, None)


# ============================================================
# TASK HELPERS
# ============================================================

def cancel_task(task_dict, guild_id):

    task = task_dict.pop(guild_id, None)

    if task and not task.done():
        task.cancel()


def cancel_idle(guild_id):

    cancel_task(
        idle_tasks,
        guild_id,
    )


def cancel_sleep(guild_id):

    cancel_task(
        sleep_tasks,
        guild_id,
    )


# ============================================================
# DISCONNECT
# ============================================================

async def disconnect_guild(
    guild: discord.Guild,
):

    guild_id = guild.id

    cancel_idle(guild_id)
    cancel_sleep(guild_id)

    queues[guild_id].clear()

    current_tracks.pop(
        guild_id,
        None,
    )

    track_retry_counter.pop(
        guild_id,
        None,
    )

    playback_generation[guild_id] += 1

    vc = guild.voice_client

    if vc:

        try:

            if vc.is_playing() or vc.is_paused():
                vc.stop()

        except Exception:
            pass

        try:

            await vc.disconnect(
                force=True,
            )

        except Exception as exc:

            logger.warning(
                "[Voice] Disconnect: %s",
                exc,
            )


# ============================================================
# PLAYER
# ============================================================

class MusicPlayer:

    def __init__(self, guild_id):

        self.guild_id = guild_id


    async def create_source(
        self,
        track: Track,
        force_refresh=False,
    ):

        # Nếu track đã có stream URL thì KHÔNG resolve lại.
        if not track.stream_url:

            stream_url, title = (
                await SelfHealingEngine.resolve_stream(
                    track.query,
                    force_refresh=force_refresh,
                )
            )

            track.stream_url = stream_url
            track.title = title

        source = discord.FFmpegPCMAudio(
            track.stream_url,
            **FFMPEG_OPTIONS,
        )

        return discord.PCMVolumeTransformer(
            source,
            volume=volumes[self.guild_id],
        )


# ============================================================
# PLAYBACK
# ============================================================

async def play_track(
    guild: discord.Guild,
    track: Track,
    announce_channel=None,
):

    guild_id = guild.id

    vc = guild.voice_client

    if not vc or not vc.is_connected():
        return False

    cancel_idle(guild_id)

    # Generation mới.
    playback_generation[guild_id] += 1

    generation = playback_generation[guild_id]

    for attempt in range(
        PLAYBACK_RETRY_COUNT + 1
    ):

        try:

            player = MusicPlayer(guild_id)

            source = await player.create_source(
                track,
                force_refresh=(attempt > 0),
            )

            current_tracks[guild_id] = track

            track_retry_counter[guild_id] = 0

            def after_playing(error):

                async def callback():

                    await handle_track_finished(
                        guild,
                        track,
                        generation,
                        error,
                        announce_channel,
                    )

                try:

                    asyncio.run_coroutine_threadsafe(
                        callback(),
                        bot.loop,
                    )

                except Exception as exc:

                    logger.debug(
                        "[Callback] %s",
                        exc,
                    )

            if vc.is_playing() or vc.is_paused():

                vc.stop()

                await asyncio.sleep(0.05)

            vc.play(
                source,
                after=after_playing,
            )

            logger.info(
                "[Playback] %s -> %s",
                guild_id,
                track.title,
            )

            if announce_channel:

                embed = discord.Embed(
                    title="✧ ── ✦ 𝕸𝖚𝖘𝖎𝖈 𝖑𝖆𝖓𝖌𝖓𝖌𝖔 ✦ ── ✧",
                    description=(
                        "🔮 **Đang phát:**\n"
                        f"**{discord.utils.escape_markdown(track.title)}**"
                    ),
                    color=discord.Color.from_rgb(
                        88,
                        24,
                        131,
                    ),
                )

                try:

                    await announce_channel.send(
                        embed=embed,
                        view=MusicControlView(),
                    )

                except Exception:
                    pass

            return True

        except Exception as exc:

            logger.warning(
                "[Playback] Attempt %s/%s: %s",
                attempt + 1,
                PLAYBACK_RETRY_COUNT + 1,
                exc,
            )

            SelfHealingEngine.purge_cache(
                track.query,
            )

            if attempt < PLAYBACK_RETRY_COUNT:

                await asyncio.sleep(0.7)

            else:

                current_tracks.pop(
                    guild_id,
                    None,
                )

                return False

    return False


# ============================================================
# TRACK FINISHED
# ============================================================

async def handle_track_finished(
    guild,
    track,
    generation,
    error,
    announce_channel=None,
):

    guild_id = guild.id

    # Callback cũ.
    if generation != playback_generation[guild_id]:
        return

    # Track cũ.
    if current_tracks.get(guild_id) is not track:
        return

    # --------------------------------------------------------
    # ERROR
    # --------------------------------------------------------

    if error:

        logger.warning(
            "[FFmpeg] Guild %s: %s",
            guild_id,
            error,
        )

        retry = track_retry_counter[guild_id]

        if retry < PLAYBACK_RETRY_COUNT:

            track_retry_counter[guild_id] += 1

            SelfHealingEngine.purge_cache(
                track.query,
            )

            await asyncio.sleep(0.7)

            if guild.voice_client:

                await play_track(
                    guild,
                    track,
                    announce_channel,
                )

            return

    track_retry_counter[guild_id] = 0

    # --------------------------------------------------------
    # GET NEXT
    # --------------------------------------------------------

    async with guild_locks[guild_id]:

        # Callback này chỉ được phép consume current track.
        if current_tracks.get(guild_id) is not track:
            return

        current_tracks.pop(
            guild_id,
            None,
        )

        if not queues[guild_id]:

            idle_task = asyncio.create_task(
                idle_disconnect(guild),
            )

            idle_tasks[guild_id] = idle_task

            return

        next_track = queues[guild_id].pop(0)

    # Không giữ lock trong playback.
    await asyncio.sleep(0.05)

    await play_track(
        guild,
        next_track,
        announce_channel,
    )


# ============================================================
# IDLE DISCONNECT
# ============================================================

async def idle_disconnect(
    guild: discord.Guild,
):

    guild_id = guild.id

    try:

        await asyncio.sleep(
            IDLE_DISCONNECT_SECONDS,
        )

        async with guild_locks[guild_id]:

            vc = guild.voice_client

            if not vc:
                return

            if (
                vc.is_connected()
                and not vc.is_playing()
                and not vc.is_paused()
                and not queues[guild_id]
                and guild_id not in current_tracks
            ):

                logger.info(
                    "[Voice] Guild %s idle.",
                    guild_id,
                )

                await disconnect_guild(
                    guild,
                )

    except asyncio.CancelledError:
        pass

    finally:

        if idle_tasks.get(guild_id) is asyncio.current_task():

            idle_tasks.pop(
                guild_id,
                None,
            )


# ============================================================
# MUSIC CONTROL
# ============================================================

class MusicControlView(discord.ui.View):

    def __init__(self):

        super().__init__(
            timeout=None,
        )


    @discord.ui.button(
        style=discord.ButtonStyle.secondary,
        emoji="⏸️",
        custom_id="music:pause",
    )
    async def pause(
        self,
        interaction,
        button,
    ):

        vc = (
            interaction.guild.voice_client
            if interaction.guild
            else None
        )

        if vc and vc.is_playing():

            vc.pause()

            await interaction.response.send_message(
                "💤 Đã tạm dừng.",
                ephemeral=True,
            )

        else:

            await interaction.response.send_message(
                "⚠️ Không có nhạc đang phát.",
                ephemeral=True,
            )


    @discord.ui.button(
        style=discord.ButtonStyle.secondary,
        emoji="▶️",
        custom_id="music:resume",
    )
    async def resume(
        self,
        interaction,
        button,
    ):

        vc = (
            interaction.guild.voice_client
            if interaction.guild
            else None
        )

        if vc and vc.is_paused():

            vc.resume()

            await interaction.response.send_message(
                "🔮 Đã tiếp tục.",
                ephemeral=True,
            )

        else:

            await interaction.response.send_message(
                "⚠️ Nhạc không ở trạng thái tạm dừng.",
                ephemeral=True,
            )


    @discord.ui.button(
        style=discord.ButtonStyle.secondary,
        emoji="⏭️",
        custom_id="music:skip",
    )
    async def skip(
        self,
        interaction,
        button,
    ):

        vc = (
            interaction.guild.voice_client
            if interaction.guild
            else None
        )

        if vc and (
            vc.is_playing()
            or vc.is_paused()
        ):

            vc.stop()

            await interaction.response.send_message(
                "📿 Đã chuyển bài.",
                ephemeral=True,
            )

        else:

            await interaction.response.send_message(
                "⚠️ Không có bài đang phát.",
                ephemeral=True,
            )


    @discord.ui.button(
        style=discord.ButtonStyle.secondary,
        emoji="🖤",
        custom_id="music:save",
    )
    async def save(
        self,
        interaction,
        button,
    ):

        guild = interaction.guild

        if not guild:
            return

        track = current_tracks.get(
            guild.id,
        )

        if not track:

            return await interaction.response.send_message(
                "⚠️ Không có bài đang phát.",
                ephemeral=True,
            )

        favs = user_collections[
            interaction.user.id
        ]

        if len(favs) >= MAX_FAVORITES:

            return await interaction.response.send_message(
                f"⚠️ Bộ sưu tập đã đủ {MAX_FAVORITES} bài.",
                ephemeral=True,
            )

        if any(
            item["query"] == track.query
            for item in favs
        ):

            return await interaction.response.send_message(
                "💠 Bài đã có trong bộ sưu tập.",
                ephemeral=True,
            )

        favs.append(
            {
                "query": track.query,
                "title": track.title,
            }
        )

        await interaction.response.send_message(
            f"✨ Đã lưu **{track.title}**.",
            ephemeral=True,
        )


    @discord.ui.button(
        style=discord.ButtonStyle.danger,
        emoji="⏹️",
        custom_id="music:stop",
    )
    async def stop(
        self,
        interaction,
        button,
    ):

        guild = interaction.guild

        if not guild:
            return

        async with guild_locks[guild.id]:

            await disconnect_guild(
                guild,
            )

        await interaction.response.send_message(
            "🌌 Đã dừng và ngắt kết nối.",
            ephemeral=True,
        )


# ============================================================
# COLLECTION MODALS
# ============================================================

class RemoveCollectionModal(
    discord.ui.Modal,
    title="🗑️ Xóa Bài Hát",
):

    index_str = discord.ui.TextInput(
        label="Số thứ tự bài cần xóa",
        placeholder="Ví dụ: 1",
        required=True,
    )

    async def on_submit(
        self,
        interaction,
    ):

        favs = user_collections[
            interaction.user.id
        ]

        try:

            index = int(
                self.index_str.value,
            )

            if not (
                1 <= index <= len(favs)
            ):
                raise ValueError

            removed = favs.pop(
                index - 1,
            )

            await interaction.response.send_message(
                f"🗑️ Đã xóa **{removed['title']}**.",
                ephemeral=True,
            )

        except ValueError:

            await interaction.response.send_message(
                "⚠️ Số thứ tự không hợp lệ.",
                ephemeral=True,
            )


class ReorderCollectionModal(
    discord.ui.Modal,
    title="🔄 Sắp Xếp Bộ Sưu Tập",
):

    from_pos = discord.ui.TextInput(
        label="Vị trí cũ",
        placeholder="Ví dụ: 3",
        required=True,
    )

    to_pos = discord.ui.TextInput(
        label="Vị trí mới",
        placeholder="Ví dụ: 1",
        required=True,
    )

    async def on_submit(
        self,
        interaction,
    ):

        favs = user_collections[
            interaction.user.id
        ]

        try:

            old = int(
                self.from_pos.value,
            )

            new = int(
                self.to_pos.value,
            )

            if not (
                1 <= old <= len(favs)
                and 1 <= new <= len(favs)
            ):
                raise ValueError

            item = favs.pop(
                old - 1,
            )

            favs.insert(
                new - 1,
                item,
            )

            await interaction.response.send_message(
                f"🔄 Đã chuyển **{item['title']}** "
                f"sang vị trí `{new}`.",
                ephemeral=True,
            )

        except ValueError:

            await interaction.response.send_message(
                "⚠️ Vị trí không hợp lệ.",
                ephemeral=True,
            )


class CollectionView(discord.ui.View):

    def __init__(
        self,
        user_id,
    ):

        super().__init__(
            timeout=120,
        )

        self.user_id = user_id


    @discord.ui.button(
        style=discord.ButtonStyle.success,
        emoji="📥",
        label="Lưu bài đang phát",
    )
    async def save_current(
        self,
        interaction,
        button,
    ):

        guild = interaction.guild

        if not guild:
            return

        track = current_tracks.get(
            guild.id,
        )

        if not track:

            return await interaction.response.send_message(
                "⚠️ Không có bài đang phát.",
                ephemeral=True,
            )

        favs = user_collections[
            self.user_id
        ]

        if len(favs) >= MAX_FAVORITES:

            return await interaction.response.send_message(
                "⚠️ Kho đã đạt giới hạn 10 bài.",
                ephemeral=True,
            )

        if any(
            x["query"] == track.query
            for x in favs
        ):

            return await interaction.response.send_message(
                "💠 Bài hát đã có.",
                ephemeral=True,
            )

        favs.append(
            {
                "query": track.query,
                "title": track.title,
            }
        )

        await interaction.response.send_message(
            f"✨ Đã lưu **{track.title}**.",
            ephemeral=True,
        )


    @discord.ui.button(
        style=discord.ButtonStyle.primary,
        emoji="📂",
        label="Xem danh sách",
    )
    async def view_list(
        self,
        interaction,
        button,
    ):

        favs = user_collections[
            self.user_id
        ]

        if not favs:

            return await interaction.response.send_message(
                "📭 Kho lưu trữ trống.",
                ephemeral=True,
            )

        lines = [
            f"`{i:02d}.` {song['title']}"
            for i, song in enumerate(
                favs,
                1,
            )
        ]

        embed = discord.Embed(
            title=(
                f"🔮 Kho Của "
                f"{interaction.user.display_name}"
            ),
            description="\n".join(lines),
            color=discord.Color.from_rgb(
                88,
                24,
                131,
            ),
        )

        await interaction.response.send_message(
            embed=embed,
            ephemeral=True,
        )


    @discord.ui.button(
        style=discord.ButtonStyle.danger,
        emoji="🗑️",
        label="Xóa bài",
    )
    async def remove_song(
        self,
        interaction,
        button,
    ):

        await interaction.response.send_modal(
            RemoveCollectionModal(),
        )


    @discord.ui.button(
        style=discord.ButtonStyle.secondary,
        emoji="🔄",
        label="Sắp xếp",
    )
    async def reorder_song(
        self,
        interaction,
        button,
    ):

        await interaction.response.send_modal(
            ReorderCollectionModal(),
        )


# ============================================================
# READY
# ============================================================

@bot.event
async def on_ready():

    if not getattr(
        bot,
        "_commands_synced",
        False,
    ):

        try:

            synced = await bot.tree.sync()

            bot._commands_synced = True

            logger.info(
                "Đã sync %s commands.",
                len(synced),
            )

        except Exception as exc:

            logger.error(
                "[Discord] Sync: %s",
                exc,
            )

    if not getattr(
        bot,
        "_persistent_view_added",
        False,
    ):

        bot.add_view(
            MusicControlView(),
        )

        bot._persistent_view_added = True

    try:

        await bot.change_presence(
            status=discord.Status.online,
            activity=discord.Activity(
                type=discord.ActivityType.listening,
                name="/help | SoundCloud Music",
            ),
        )

    except Exception:
        pass

    logger.info(
        "Bot online: %s",
        bot.user,
    )


# ============================================================
# PLAY
# ============================================================

@bot.tree.command(
    name="play",
    description="🔮 Phát nhạc từ SoundCloud",
)
@discord.app_commands.describe(
    query="Tên bài hát hoặc link SoundCloud",
)
async def play(
    interaction,
    query: str,
):

    await interaction.response.defer()

    guild = interaction.guild

    if not guild:

        return await interaction.followup.send(
            "⚠️ Lệnh này chỉ dùng trong server.",
            ephemeral=True,
        )

    if not interaction.user.voice:

        return await interaction.followup.send(
            "🥀 Bạn cần vào voice channel trước.",
            ephemeral=True,
        )

    safe_query = SecuritySanitizer.sanitize_input(
        query,
    )

    if not safe_query:

        return await interaction.followup.send(
            "⚠️ Từ khóa không hợp lệ.",
            ephemeral=True,
        )

    guild_id = guild.id

    # --------------------------------------------------------
    # RESOLVE TRƯỚC
    # Không giữ guild lock khi yt-dlp chạy.
    # --------------------------------------------------------

    try:

        stream_url, title = (
            await SelfHealingEngine.resolve_stream(
                safe_query,
            )
        )

    except Exception as exc:

        return await interaction.followup.send(
            f"⚠️ Không thể tìm bài hát:\n`{exc}`",
            ephemeral=True,
        )

    track = Track(
        query=safe_query,
        title=title,
        stream_url=stream_url,
    )

    # --------------------------------------------------------
    # QUEUE / VOICE
    # --------------------------------------------------------

    async with guild_locks[guild_id]:

        if len(queues[guild_id]) >= MAX_QUEUE_SIZE:

            return await interaction.followup.send(
                f"⚠️ Hàng đợi đã đầy ({MAX_QUEUE_SIZE}).",
                ephemeral=True,
            )

        vc = guild.voice_client

        channel = interaction.user.voice.channel

        if not vc:

            try:

                vc = await channel.connect(
                    self_deaf=True,
                )

            except Exception as exc:

                logger.error(
                    "[Voice] Connect: %s",
                    exc,
                )

                return await interaction.followup.send(
                    "⚠️ Không thể kết nối voice.",
                    ephemeral=True,
                )

        elif vc.channel != channel:

            if not vc.is_playing():

                try:
                    await vc.move_to(channel)
                except Exception:
                    pass

        # ----------------------------------------------------
        # PLAY NOW
        # ----------------------------------------------------

        if (
            not vc.is_playing()
            and not vc.is_paused()
            and guild_id not in current_tracks
        ):

            play_now = True

        else:

            play_now = False

            queues[guild_id].append(
                track,
            )

            position = len(
                queues[guild_id],
            )

    # --------------------------------------------------------
    # PLAY OUTSIDE LOCK
    # --------------------------------------------------------

    if play_now:

        cancel_idle(guild_id)

        success = await play_track(
            guild,
            track,
            interaction.channel,
        )

        if not success:

            return await interaction.followup.send(
                "⚠️ Không thể phát bài này.",
                ephemeral=True,
            )

        return await interaction.followup.send(
            embed=discord.Embed(
                title="✧ ── ✦ 𝕸𝖚𝖘𝖎𝖈 𝖑𝖆𝖓𝖌𝖓𝖌𝖔 ✦ ── ✧",
                description=(
                    "🔮 **Đang chuẩn bị phát:**\n"
                    f"**{title}**"
                ),
                color=discord.Color.from_rgb(
                    88,
                    24,
                    131,
                ),
            ),
            view=MusicControlView(),
        )

    return await interaction.followup.send(
        embed=discord.Embed(
            title="💠 Đã thêm vào hàng đợi",
            description=(
                f"**{title}**\n"
                f"Vị trí: `{position}/{MAX_QUEUE_SIZE}`"
            ),
            color=discord.Color.from_rgb(
                45,
                10,
                75,
            ),
        ),
    )


# ============================================================
# MY FAVORITE
# ============================================================

@bot.tree.command(
    name="myfavorite",
    description="🖤 Nạp bộ sưu tập cá nhân",
)
async def myfavorite(
    interaction,
):

    if not interaction.user.voice:

        return await interaction.response.send_message(
            "🥀 Bạn cần vào voice channel trước.",
            ephemeral=True,
        )

    favs = user_collections[
        interaction.user.id
    ]

    if not favs:

        return await interaction.response.send_message(
            "📭 Kho lưu trữ trống.",
            ephemeral=True,
        )

    await interaction.response.defer()

    guild = interaction.guild
    guild_id = guild.id

    # Snapshot.
    selected = list(
        favs[:MAX_QUEUE_SIZE],
    )

    added = 0
    first_track = None

    for item in selected:

        async with guild_locks[guild_id]:

            if (
                len(queues[guild_id])
                >= MAX_QUEUE_SIZE
            ):
                break

            vc = guild.voice_client

            if not vc:

                try:

                    vc = await (
                        interaction.user
                        .voice.channel
                        .connect(
                            self_deaf=True,
                        )
                    )

                except Exception:

                    break

        try:

            # Resolve ngoài lock.
            stream_url, title = (
                await SelfHealingEngine.resolve_stream(
                    item["query"],
                )
            )

            track = Track(
                query=item["query"],
                title=title,
                stream_url=stream_url,
            )

        except Exception as exc:

            logger.debug(
                "[Favorite] %s: %s",
                item.get("title"),
                exc,
            )

            continue

        async with guild_locks[guild_id]:

            vc = guild.voice_client

            if (
                first_track is None
                and vc
                and not vc.is_playing()
                and not vc.is_paused()
                and guild_id not in current_tracks
            ):

                first_track = track

            elif len(queues[guild_id]) < MAX_QUEUE_SIZE:

                queues[guild_id].append(
                    track,
                )

            else:

                break

            added += 1

    if first_track:

        await play_track(
            guild,
            first_track,
            interaction.channel,
        )

    await interaction.followup.send(
        embed=discord.Embed(
            title="🖤 Bộ Sưu Tập",
            description=(
                f"Đã nạp **{added} bài**."
            ),
            color=discord.Color.from_rgb(
                88,
                24,
                131,
            ),
        ),
        view=MusicControlView(),
    )


# ============================================================
# QUEUE
# ============================================================

@bot.tree.command(
    name="queue",
    description="📜 Xem hàng đợi",
)
async def queue_cmd(
    interaction,
):

    if not interaction.guild:

        return await interaction.response.send_message(
            "⚠️ Lệnh chỉ dùng trong server.",
            ephemeral=True,
        )

    guild_id = interaction.guild.id

    current = current_tracks.get(
        guild_id,
    )

    q = queues[guild_id]

    if not current and not q:

        return await interaction.response.send_message(
            "📭 Hàng đợi trống.",
            ephemeral=True,
        )

    parts = []

    if current:

        parts.append(
            "🔮 **Đang phát:**\n"
            f"`▶` **{current.title}**"
        )

    if q:

        lines = [
            f"`{i:02d}.` {track.title}"
            for i, track in enumerate(q, 1)
        ]

        parts.append(
            "📜 **Tiếp theo:**\n"
            + "\n".join(lines)
        )

    total = (
        len(q)
        + (1 if current else 0)
    )

    embed = discord.Embed(
        title=f"⚡ Danh Sách Phát ({total} bài)",
        description="\n\n".join(parts),
        color=discord.Color.from_rgb(
            60,
            15,
            100,
        ),
    )

    await interaction.response.send_message(
        embed=embed,
        ephemeral=True,
    )


# ============================================================
# REMOVE
# ============================================================

@bot.tree.command(
    name="remove",
    description="🗑️ Xóa bài khỏi hàng đợi",
)
@discord.app_commands.describe(
    index="Vị trí bắt đầu",
    count="Số lượng bài cần xóa",
)
async def remove(
    interaction,
    index: int,
    count: int = 1,
):

    guild_id = interaction.guild.id

    async with guild_locks[guild_id]:

        q = queues[guild_id]

        if not q:

            return await interaction.response.send_message(
                "📭 Hàng đợi trống.",
                ephemeral=True,
            )

        if not (
            1 <= index <= len(q)
        ):

            return await interaction.response.send_message(
                "⚠️ Vị trí không hợp lệ.",
                ephemeral=True,
            )

        count = max(
            1,
            min(
                count,
                len(q) - index + 1,
            ),
        )

        removed = q[
            index - 1:index - 1 + count
        ]

        del q[
            index - 1:index - 1 + count
        ]

    titles = ", ".join(
        f"**{x.title}**"
        for x in removed
    )

    await interaction.response.send_message(
        f"🗑️ Đã xóa **{len(removed)} bài**:\n{titles}",
        ephemeral=True,
    )


# ============================================================
# MOVE
# ============================================================

@bot.tree.command(
    name="move",
    description="🔄 Đổi vị trí bài hát",
)
@discord.app_commands.describe(
    from_pos="Vị trí cũ",
    to_pos="Vị trí mới",
)
async def move(
    interaction,
    from_pos: int,
    to_pos: int,
):

    guild_id = interaction.guild.id

    async with guild_locks[guild_id]:

        q = queues[guild_id]

        if not q:

            return await interaction.response.send_message(
                "📭 Hàng đợi trống.",
                ephemeral=True,
            )

        if not (
            1 <= from_pos <= len(q)
            and 1 <= to_pos <= len(q)
        ):

            return await interaction.response.send_message(
                "⚠️ Vị trí không hợp lệ.",
                ephemeral=True,
            )

        track = q.pop(
            from_pos - 1,
        )

        q.insert(
            to_pos - 1,
            track,
        )

    await interaction.response.send_message(
        f"🔄 Đã chuyển **{track.title}** "
        f"`{from_pos}` → `{to_pos}`.",
        ephemeral=True,
    )


# ============================================================
# DUPLICATE
# ============================================================

@bot.tree.command(
    name="duplicate",
    description="🧬 Nhân bản bài hát",
)
@discord.app_commands.describe(
    index="0 = bài đang phát, 1-10 = queue",
    amount="Số bản sao từ 1 đến 5",
)
async def duplicate(
    interaction,
    index: int,
    amount: int,
):

    guild_id = interaction.guild.id

    if not (
        1 <= amount <= 5
    ):

        return await interaction.response.send_message(
            "⚠️ Amount phải từ 1 đến 5.",
            ephemeral=True,
        )

    async with guild_locks[guild_id]:

        q = queues[guild_id]

        if len(q) + amount > MAX_QUEUE_SIZE:

            return await interaction.response.send_message(
                "⚠️ Hàng đợi không đủ chỗ.",
                ephemeral=True,
            )

        if index == 0:

            target = current_tracks.get(
                guild_id,
            )

            if not target:

                return await interaction.response.send_message(
                    "⚠️ Không có bài đang phát.",
                    ephemeral=True,
                )

        else:

            if not (
                1 <= index <= len(q)
            ):

                return await interaction.response.send_message(
                    "⚠️ Vị trí không hợp lệ.",
                    ephemeral=True,
                )

            target = q[index - 1]

        copies = [
            Track(
                query=target.query,
                title=target.title,
            )
            for _ in range(amount)
        ]

        insert_position = (
            0
            if index == 0
            else index
        )

        for offset, copy in enumerate(copies):

            q.insert(
                insert_position + offset,
                copy,
            )

    await interaction.response.send_message(
        f"🧬 Đã thêm **{amount} bản sao** "
        f"của **{target.title}**.",
        ephemeral=True,
    )


# ============================================================
# VOLUME
# ============================================================

@bot.tree.command(
    name="volume",
    description="🔊 Chỉnh âm lượng",
)
@discord.app_commands.describe(
    level="Mức âm lượng 1-100",
)
async def volume(
    interaction,
    level: int,
):

    if not (
        1 <= level <= 100
    ):

        return await interaction.response.send_message(
            "⚠️ Âm lượng phải từ 1 đến 100.",
            ephemeral=True,
        )

    guild_id = interaction.guild.id

    volumes[guild_id] = (
        level / 100
    )

    vc = interaction.guild.voice_client

    if vc and vc.source:

        if isinstance(
            vc.source,
            discord.PCMVolumeTransformer,
        ):

            vc.source.volume = (
                level / 100
            )

    await interaction.response.send_message(
        f"🔊 Âm lượng: **{level}%**",
        ephemeral=True,
    )


# ============================================================
# SLEEP
# ============================================================

@bot.tree.command(
    name="sleep",
    description="🌙 Hẹn giờ ngắt bot",
)
@discord.app_commands.describe(
    minutes="1-180 phút",
)
async def sleep(
    interaction,
    minutes: int,
):

    if not (
        1 <= minutes <= 180
    ):

        return await interaction.response.send_message(
            "⚠️ Thời gian từ 1 đến 180 phút.",
            ephemeral=True,
        )

    guild = interaction.guild
    guild_id = guild.id

    cancel_sleep(
        guild_id,
    )

    async def timer():

        try:

            await asyncio.sleep(
                minutes * 60,
            )

            async with guild_locks[guild_id]:

                await disconnect_guild(
                    guild,
                )

            try:

                await interaction.channel.send(
                    "🌙 Hẹn giờ kết thúc. "
                    "Bot đã tự động ngắt kết nối."
                )

            except Exception:
                pass

        except asyncio.CancelledError:
            pass

        finally:

            if sleep_tasks.get(guild_id) is asyncio.current_task():

                sleep_tasks.pop(
                    guild_id,
                    None,
                )

    sleep_tasks[guild_id] = (
        asyncio.create_task(timer())
    )

    await interaction.response.send_message(
        f"⏰ Bot sẽ tự ngắt sau **{minutes} phút**.",
        ephemeral=True,
    )


# ============================================================
# COLLECTION
# ============================================================

@bot.tree.command(
    name="collection",
    description="💼 Quản lý bộ sưu tập cá nhân",
)
async def collection(
    interaction,
):

    embed = discord.Embed(
        title="🌌 Kho Lưu Trữ Cá Nhân",
        description=(
            "Sử dụng các nút bên dưới để quản lý "
            "bộ sưu tập của bạn."
        ),
        color=discord.Color.from_rgb(
            88,
            24,
            131,
        ),
    )

    await interaction.response.send_message(
        embed=embed,
        view=CollectionView(
            interaction.user.id,
        ),
        ephemeral=True,
    )


# ============================================================
# DELETE DATA
# ============================================================

@bot.tree.command(
    name="delete-my-data",
    description="🛡️ Xóa dữ liệu cá nhân",
)
async def delete_my_data(
    interaction,
):

    user_id = interaction.user.id

    existed = (
        user_id in user_collections
    )

    user_collections.pop(
        user_id,
        None,
    )

    if existed:

        message = (
            "🛡️ Đã xóa toàn bộ dữ liệu "
            "bộ sưu tập cá nhân."
        )

    else:

        message = (
            "📭 Không có dữ liệu cá nhân."
        )

    await interaction.response.send_message(
        message,
        ephemeral=True,
    )


# ============================================================
# HELP
# ============================================================

@bot.tree.command(
    name="help",
    description="🚀 Hướng dẫn sử dụng bot",
)
async def help_cmd(
    interaction,
):

    embed = discord.Embed(
        title="✧ ── ✦ WORKSTATION MUSIC BOT ✦ ── ✧",
        color=discord.Color.from_rgb(
            88,
            24,
            131,
        ),
    )

    commands_list = [
        ("🔮 `/play`", "Phát nhạc SoundCloud"),
        ("🖤 `/myfavorite`", "Nạp bộ sưu tập"),
        ("📜 `/queue`", "Xem hàng đợi"),
        ("🗑️ `/remove`", "Xóa bài trong queue"),
        ("🧬 `/duplicate`", "Nhân bản bài"),
        ("🔄 `/move`", "Đổi vị trí bài"),
        ("💼 `/collection`", "Quản lý bộ sưu tập"),
        ("🔊 `/volume`", "Chỉnh âm lượng"),
        ("🌙 `/sleep`", "Hẹn giờ ngắt bot"),
        ("🛡️ `/delete-my-data`", "Xóa dữ liệu cá nhân"),
    ]

    for name, description in commands_list:

        embed.add_field(
            name=name,
            value=description,
            inline=False,
        )

    await interaction.response.send_message(
        embed=embed,
        ephemeral=True,
    )


# ============================================================
# ERROR
# ============================================================

@bot.tree.error
async def on_app_command_error(
    interaction,
    error,
):

    logger.error(
        "[SlashCommand] %s",
        error,
        exc_info=True,
    )

    message = (
        "⚠️ Đã xảy ra lỗi khi xử lý lệnh. "
        "Vui lòng thử lại."
    )

    try:

        if interaction.response.is_done():

            await interaction.followup.send(
                message,
                ephemeral=True,
            )

        else:

            await interaction.response.send_message(
                message,
                ephemeral=True,
            )

    except Exception:
        pass


# ============================================================
# SHUTDOWN
# ============================================================

async def shutdown():

    logger.info(
        "🛑 Shutdown Workstation Bot..."
    )

    for task in list(
        sleep_tasks.values()
    ):

        task.cancel()

    for task in list(
        idle_tasks.values()
    ):

        task.cancel()

    sleep_tasks.clear()
    idle_tasks.clear()

    for guild in bot.guilds:

        try:

            await disconnect_guild(
                guild,
            )

        except Exception:
            pass

    WORKSTATION_POOL.shutdown(
        wait=False,
        cancel_futures=True,
    )


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    TOKEN = os.getenv(
        "DISCORD_TOKEN"
    )

    if not TOKEN:

        logger.critical(
            "🚨 Không tìm thấy DISCORD_TOKEN."
        )

        raise SystemExit(1)

    try:

        bot.run(
            TOKEN
        )

    except KeyboardInterrupt:

        logger.info(
            "🛑 Bot stopped by user."
        )

    except Exception as exc:

        logger.critical(
            "🚨 Bot crashed: %s",
            exc,
            exc_info=True,
        )
