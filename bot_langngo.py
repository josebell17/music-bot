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
    handlers=[logging.StreamHandler()]
)

for handler in logging.root.handlers:
    if isinstance(handler, logging.StreamHandler):
        try:
            handler.stream.reconfigure(encoding="utf-8")
        except AttributeError:
            pass

logger = logging.getLogger("WorkstationProductionBot")


# ============================================================
# DISCORD CONFIG
# ============================================================

intents = discord.Intents.default()
intents.message_content = True
intents.voice_states = True
intents.guilds = True
intents.guild_messages = True

bot = commands.Bot(
    command_prefix="!",
    intents=intents
)


# ============================================================
# GLOBAL CONFIG
# ============================================================

MAX_QUEUE_SIZE = 10
MAX_FAVORITES = 10

STREAM_CACHE_TTL = 300       # 5 phút
EXTRACTION_TIMEOUT = 15
PLAYBACK_RETRY_COUNT = 2
IDLE_DISCONNECT_SECONDS = 60

WORKSTATION_POOL = ThreadPoolExecutor(
    max_workers=4,
    thread_name_prefix="ProductionWorker"
)


# ============================================================
# MEMORY
# ============================================================

class LimitedCache(OrderedDict):
    def __init__(self, maxsize=150):
        super().__init__()
        self.maxsize = maxsize

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


URL_CACHE = LimitedCache(maxsize=150)

queues = defaultdict(list)
volumes = defaultdict(lambda: 0.5)
user_collections = defaultdict(list)

guild_locks = defaultdict(asyncio.Lock)
sleep_tasks = {}

# Tránh nhiều callback cùng khởi động play_next()
playback_tasks = {}

# Track hiện tại của từng guild
current_tracks = {}

# Đếm retry
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
    "socket_timeout": 15,
    "retries": 2,
    "fragment_retries": 2,
    "skip_unavailable_fragments": True,
    "cachedir": False,
    "ignoreerrors": False,
}


def create_ytdl():
    """
    Tạo YoutubeDL riêng cho từng worker.
    Không dùng một instance YoutubeDL global cho nhiều thread.
    """
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
        "-bufsize 512k "
        "-ar 48000 "
        "-ac 2"
    ),
}


# ============================================================
# SECURITY / INPUT
# ============================================================

class SecuritySanitizer:

    @staticmethod
    def sanitize_input(text: str) -> str:
        if not text:
            return ""

        text = str(text).strip()

        # Không cho phép ký tự điều khiển.
        cleaned = "".join(
            ch for ch in text
            if ch.isalnum() or ch in " -_./:?&=+@%#(),[]'!"
        )

        return cleaned[:200].strip()


# ============================================================
# STREAM ENGINE
# ============================================================

class SelfHealingEngine:

    @staticmethod
    def _cache_valid(item: CachedStream) -> bool:
        return (
            time.monotonic() - item.created_at
        ) < STREAM_CACHE_TTL

    @staticmethod
    async def resolve_stream(
        query: str,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        force_refresh: bool = False
    ):
        loop = loop or asyncio.get_running_loop()

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
            and SelfHealingEngine._cache_valid(cached)
        ):
            return cached.url, cached.title

        # ----------------------------------------------------
        # SEARCH
        # ----------------------------------------------------

        if safe_query.startswith(("http://", "https://")):
            search_query = safe_query
        else:
            search_query = f"scsearch1:{safe_query}"

        def extract_worker():
            ytdl = create_ytdl()

            try:
                data = ytdl.extract_info(
                    search_query,
                    download=False
                )

                if not data:
                    return None

                if "entries" in data:
                    entries = data.get("entries") or []

                    for entry in entries:
                        if entry:
                            data = entry
                            break
                    else:
                        return None

                stream_url = data.get("url")
                title = data.get("title") or safe_query

                if not stream_url:
                    return None

                return {
                    "url": stream_url,
                    "title": title
                }

            except Exception as exc:
                logger.error(
                    "[yt-dlp] Extraction error: %s",
                    exc
                )
                return None

            finally:
                try:
                    ytdl.close()
                except Exception:
                    pass

        try:
            data = await asyncio.wait_for(
                loop.run_in_executor(
                    WORKSTATION_POOL,
                    extract_worker
                ),
                timeout=EXTRACTION_TIMEOUT
            )

        except asyncio.TimeoutError:
            logger.warning(
                "[yt-dlp] Timeout khi tìm: %s",
                safe_query
            )
            raise RuntimeError(
                "SoundCloud phản hồi quá chậm. Vui lòng thử lại."
            )

        if not data:
            raise RuntimeError(
                f"Không tìm thấy bài hát: {safe_query}"
            )

        stream_url = data["url"]
        title = data["title"]

        URL_CACHE[safe_query] = CachedStream(
            url=stream_url,
            title=title,
            created_at=time.monotonic()
        )

        return stream_url, title

    @staticmethod
    async def background_prefetch(query: str):
        try:
            await SelfHealingEngine.resolve_stream(query)

        except Exception as exc:
            logger.debug(
                "[Prefetch] Không thể prefetch '%s': %s",
                query,
                exc
            )

    @staticmethod
    def purge_cache(query: str):
        query = SecuritySanitizer.sanitize_input(query)

        if query in URL_CACHE:
            del URL_CACHE[query]


# ============================================================
# PLAYER
# ============================================================

class MusicPlayer:

    def __init__(self, guild_id: int):
        self.guild_id = guild_id
        self.track: Optional[Track] = None
        self.source = None

    async def create_source(
        self,
        track: Track,
        volume: float,
        force_refresh: bool = False
    ):

        stream_url, title = await SelfHealingEngine.resolve_stream(
            track.query,
            force_refresh=force_refresh
        )

        track.stream_url = stream_url
        track.title = title

        ffmpeg_source = discord.FFmpegPCMAudio(
            stream_url,
            **FFMPEG_OPTIONS
        )

        source = discord.PCMVolumeTransformer(
            ffmpeg_source,
            volume=max(0.0, min(volume, 1.0))
        )

        self.track = track
        self.source = source

        return source


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def get_voice_client(guild: discord.Guild):
    return guild.voice_client


def get_current_track(guild_id: int):
    return current_tracks.get(guild_id)


def cancel_sleep_timer(guild_id: int):
    task = sleep_tasks.pop(guild_id, None)

    if task:
        task.cancel()


async def disconnect_guild(guild: discord.Guild):
    guild_id = guild.id

    cancel_sleep_timer(guild_id)

    queues[guild_id].clear()
    current_tracks.pop(guild_id, None)
    track_retry_counter.pop(guild_id, None)

    vc = guild.voice_client

    if vc:
        try:
            await vc.disconnect(force=True)
        except Exception as exc:
            logger.warning(
                "[Voice] Disconnect error %s: %s",
                guild_id,
                exc
            )


# ============================================================
# PLAYBACK ENGINE
# ============================================================

async def play_track(
    guild: discord.Guild,
    track: Track,
    announce_channel=None,
    retry: int = 0
):
    guild_id = guild.id
    vc = guild.voice_client

    if not vc or not vc.is_connected():
        logger.warning(
            "[Playback] Guild %s chưa kết nối voice.",
            guild_id
        )
        return False

    player = MusicPlayer(guild_id)

    try:
        source = await player.create_source(
            track,
            volumes[guild_id],
            force_refresh=(retry > 0)
        )

    except Exception as exc:
        logger.error(
            "[Playback] Không thể tạo source '%s': %s",
            track.query,
            exc
        )

        if retry < PLAYBACK_RETRY_COUNT:
            SelfHealingEngine.purge_cache(track.query)

            await asyncio.sleep(1)

            return await play_track(
                guild,
                track,
                announce_channel,
                retry + 1
            )

        return False

    current_tracks[guild_id] = track

    # --------------------------------------------------------
    # CALLBACK
    # --------------------------------------------------------

    def after_playing(error):
        if error:
            logger.error(
                "[FFmpeg] Guild %s: %s",
                guild_id,
                error
            )

        async def callback():
            await handle_track_finished(
                guild,
                track,
                error,
                announce_channel
            )

        try:
            asyncio.run_coroutine_threadsafe(
                callback(),
                bot.loop
            )
        except Exception as exc:
            logger.error(
                "[Callback] Không thể gửi callback: %s",
                exc
            )

    try:
        if vc.is_playing() or vc.is_paused():
            vc.stop()

        vc.play(
            source,
            after=after_playing
        )

        logger.info(
            "[Playback] Guild %s -> %s",
            guild_id,
            track.title
        )

        if announce_channel:
            embed = discord.Embed(
                title="✧ ── ✦ 𝕸𝖚𝖘𝖎𝖈 𝖑𝖆𝖓𝖌𝖓𝖌𝖔 ✦ ── ✧",
                description=(
                    f"🔮 **Đang phát:**\n"
                    f"**{discord.utils.escape_markdown(track.title)}**"
                ),
                color=discord.Color.from_rgb(88, 24, 131)
            )

            try:
                await announce_channel.send(
                    embed=embed,
                    view=MusicControlView()
                )
            except Exception as exc:
                logger.debug(
                    "[Discord] Không thể gửi now-playing: %s",
                    exc
                )

        return True

    except Exception as exc:
        logger.error(
            "[Playback] vc.play() failed: %s",
            exc
        )

        if retry < PLAYBACK_RETRY_COUNT:
            SelfHealingEngine.purge_cache(track.query)

            await asyncio.sleep(1)

            return await play_track(
                guild,
                track,
                announce_channel,
                retry + 1
            )

        return False


async def handle_track_finished(
    guild: discord.Guild,
    track: Track,
    error,
    announce_channel=None
):
    guild_id = guild.id

    # Nếu track hiện tại không còn là track này,
    # callback cũ không được phép can thiệp.
    current = current_tracks.get(guild_id)

    if current is not track:
        return

    # --------------------------------------------------------
    # RETRY KHI STREAM HỎNG
    # --------------------------------------------------------

    if error:
        retry_count = track_retry_counter[guild_id]

        if retry_count < PLAYBACK_RETRY_COUNT:
            track_retry_counter[guild_id] += 1

            logger.warning(
                "[Recovery] Retry %s/%s: %s",
                retry_count + 1,
                PLAYBACK_RETRY_COUNT,
                track.title
            )

            SelfHealingEngine.purge_cache(track.query)

            await asyncio.sleep(1)

            if guild.voice_client:
                await play_track(
                    guild,
                    track,
                    announce_channel,
                    retry=retry_count + 1
                )

            return

    track_retry_counter[guild_id] = 0

    # --------------------------------------------------------
    # PLAY NEXT
    # --------------------------------------------------------

    async with guild_locks[guild_id]:

        current_tracks.pop(guild_id, None)

        if not queues[guild_id]:
            logger.info(
                "[Queue] Guild %s đã hết bài.",
                guild_id
            )

            # Không disconnect ngay.
            # Chờ một khoảng để người dùng có thể /play tiếp.
            asyncio.create_task(
                idle_disconnect(guild)
            )

            return

        next_track = queues[guild_id].pop(0)

    await asyncio.sleep(0.15)

    await play_track(
        guild,
        next_track,
        announce_channel
    )


async def idle_disconnect(guild: discord.Guild):
    guild_id = guild.id

    await asyncio.sleep(IDLE_DISCONNECT_SECONDS)

    async with guild_locks[guild_id]:

        vc = guild.voice_client

        if (
            vc
            and vc.is_connected()
            and not vc.is_playing()
            and not vc.is_paused()
            and not queues[guild_id]
        ):
            logger.info(
                "[Voice] Guild %s idle -> disconnect.",
                guild_id
            )

            await disconnect_guild(guild)


# ============================================================
# MUSIC CONTROL VIEW
# ============================================================

class MusicControlView(discord.ui.View):

    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        style=discord.ButtonStyle.secondary,
        emoji="⏸️",
        custom_id="music:pause"
    )
    async def pause(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button
    ):
        guild = interaction.guild

        if not guild:
            return

        vc = guild.voice_client

        if vc and vc.is_playing():
            vc.pause()

            await interaction.response.send_message(
                "💤 Đã tạm dừng phát nhạc.",
                ephemeral=True
            )
        else:
            await interaction.response.send_message(
                "⚠️ Không có nhạc đang phát.",
                ephemeral=True
            )

    @discord.ui.button(
        style=discord.ButtonStyle.secondary,
        emoji="▶️",
        custom_id="music:resume"
    )
    async def resume(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button
    ):
        guild = interaction.guild

        if not guild:
            return

        vc = guild.voice_client

        if vc and vc.is_paused():
            vc.resume()

            await interaction.response.send_message(
                "🔮 Đã tiếp tục phát nhạc.",
                ephemeral=True
            )
        else:
            await interaction.response.send_message(
                "⚠️ Nhạc không ở trạng thái tạm dừng.",
                ephemeral=True
            )

    @discord.ui.button(
        style=discord.ButtonStyle.secondary,
        emoji="⏭️",
        custom_id="music:skip"
    )
    async def skip(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button
    ):
        guild = interaction.guild

        if not guild:
            return

        vc = guild.voice_client

        if vc and (vc.is_playing() or vc.is_paused()):
            vc.stop()

            await interaction.response.send_message(
                "📿 Đã chuyển bài.",
                ephemeral=True
            )
        else:
            await interaction.response.send_message(
                "⚠️ Không có bài đang phát.",
                ephemeral=True
            )

    @discord.ui.button(
        style=discord.ButtonStyle.secondary,
        emoji="🖤",
        custom_id="music:save"
    )
    async def save(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button
    ):
        guild = interaction.guild

        if not guild:
            return

        track = current_tracks.get(guild.id)

        if not track:
            return await interaction.response.send_message(
                "⚠️ Không có bài hát đang phát.",
                ephemeral=True
            )

        favs = user_collections[interaction.user.id]

        if len(favs) >= MAX_FAVORITES:
            return await interaction.response.send_message(
                f"⚠️ Bộ sưu tập đã đủ {MAX_FAVORITES} bài.",
                ephemeral=True
            )

        if any(
            item["query"] == track.query
            for item in favs
        ):
            return await interaction.response.send_message(
                "💠 Bài hát đã có trong bộ sưu tập.",
                ephemeral=True
            )

        # Chỉ lưu query/title.
        # Không lưu stream URL vì URL SoundCloud có thể hết hạn.
        favs.append({
            "query": track.query,
            "title": track.title
        })

        asyncio.create_task(
            SelfHealingEngine.background_prefetch(
                track.query
            )
        )

        await interaction.response.send_message(
            f"✨ Đã lưu **{track.title}** vào kho "
            f"({len(favs)}/{MAX_FAVORITES}).",
            ephemeral=True
        )

    @discord.ui.button(
        style=discord.ButtonStyle.danger,
        emoji="⏹️",
        custom_id="music:stop"
    )
    async def stop(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button
    ):
        guild = interaction.guild

        if not guild:
            return

        async with guild_locks[guild.id]:
            await disconnect_guild(guild)

        await interaction.response.send_message(
            "🌌 Đã dừng hệ thống và ngắt kết nối.",
            ephemeral=True
        )


# ============================================================
# COLLECTION VIEW
# ============================================================

class RemoveCollectionModal(
    discord.ui.Modal,
    title="🗑️ Xóa Bài Hát"
):

    index_str = discord.ui.TextInput(
        label="Số thứ tự bài cần xóa",
        placeholder="Ví dụ: 1",
        required=True
    )

    async def on_submit(
        self,
        interaction: discord.Interaction
    ):
        favs = user_collections[interaction.user.id]

        try:
            index = int(self.index_str.value)

            if not (1 <= index <= len(favs)):
                raise ValueError

            removed = favs.pop(index - 1)

            await interaction.response.send_message(
                f"🗑️ Đã xóa **{removed['title']}**.",
                ephemeral=True
            )

        except ValueError:
            await interaction.response.send_message(
                "⚠️ Số thứ tự không hợp lệ.",
                ephemeral=True
            )


class ReorderCollectionModal(
    discord.ui.Modal,
    title="🔄 Sắp Xếp Bộ Sưu Tập"
):

    from_pos = discord.ui.TextInput(
        label="Vị trí cũ",
        placeholder="Ví dụ: 3",
        required=True
    )

    to_pos = discord.ui.TextInput(
        label="Vị trí mới",
        placeholder="Ví dụ: 1",
        required=True
    )

    async def on_submit(
        self,
        interaction: discord.Interaction
    ):
        favs = user_collections[interaction.user.id]

        try:
            old = int(self.from_pos.value)
            new = int(self.to_pos.value)

            if not (
                1 <= old <= len(favs)
                and 1 <= new <= len(favs)
            ):
                raise ValueError

            item = favs.pop(old - 1)
            favs.insert(new - 1, item)

            await interaction.response.send_message(
                f"🔄 Đã chuyển **{item['title']}** "
                f"sang vị trí `{new}`.",
                ephemeral=True
            )

        except ValueError:
            await interaction.response.send_message(
                "⚠️ Vị trí không hợp lệ.",
                ephemeral=True
            )


class CollectionView(discord.ui.View):

    def __init__(self, user_id: int):
        super().__init__(timeout=120)
        self.user_id = user_id

    @discord.ui.button(
        style=discord.ButtonStyle.success,
        emoji="📥",
        label="Lưu bài đang phát"
    )
    async def save_current(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button
    ):
        guild = interaction.guild

        if not guild:
            return

        track = current_tracks.get(guild.id)

        if not track:
            return await interaction.response.send_message(
                "⚠️ Không có bài đang phát.",
                ephemeral=True
            )

        favs = user_collections[self.user_id]

        if len(favs) >= MAX_FAVORITES:
            return await interaction.response.send_message(
                "⚠️ Kho đã đạt giới hạn 10 bài.",
                ephemeral=True
            )

        if any(
            item["query"] == track.query
            for item in favs
        ):
            return await interaction.response.send_message(
                "💠 Bài hát đã có sẵn.",
                ephemeral=True
            )

        favs.append({
            "query": track.query,
            "title": track.title
        })

        asyncio.create_task(
            SelfHealingEngine.background_prefetch(
                track.query
            )
        )

        await interaction.response.send_message(
            f"✨ Đã lưu **{track.title}**.",
            ephemeral=True
        )

    @discord.ui.button(
        style=discord.ButtonStyle.primary,
        emoji="📂",
        label="Xem danh sách"
    )
    async def view_list(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button
    ):
        favs = user_collections[self.user_id]

        if not favs:
            return await interaction.response.send_message(
                "📭 Kho lưu trữ trống.",
                ephemeral=True
            )

        lines = []

        for i, song in enumerate(favs, start=1):
            lines.append(
                f"`{i:02d}.` {song['title']}"
            )

        embed = discord.Embed(
            title=f"🔮 Kho Của {interaction.user.display_name}",
            description="\n".join(lines),
            color=discord.Color.from_rgb(88, 24, 131)
        )

        await interaction.response.send_message(
            embed=embed,
            ephemeral=True
        )

    @discord.ui.button(
        style=discord.ButtonStyle.danger,
        emoji="🗑️",
        label="Xóa bài"
    )
    async def remove_song(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button
    ):
        await interaction.response.send_modal(
            RemoveCollectionModal()
        )

    @discord.ui.button(
        style=discord.ButtonStyle.secondary,
        emoji="🔄",
        label="Sắp xếp"
    )
    async def reorder_song(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button
    ):
        await interaction.response.send_modal(
            ReorderCollectionModal()
        )


# ============================================================
# READY
# ============================================================

@bot.event
async def on_ready():

    # Chỉ sync command một lần.
    if not getattr(bot, "_commands_synced", False):
        try:
            synced = await bot.tree.sync()

            bot._commands_synced = True

            logger.info(
                "🚀 Đã đồng bộ %s Slash Commands.",
                len(synced)
            )

        except Exception as exc:
            logger.error(
                "[Discord] Slash sync error: %s",
                exc
            )

    # Persistent View
    if not getattr(bot, "_persistent_view_added", False):
        bot.add_view(MusicControlView())
        bot._persistent_view_added = True

    activity = discord.Activity(
        type=discord.ActivityType.listening,
        name="/help | SoundCloud Music"
    )

    try:
        await bot.change_presence(
            status=discord.Status.online,
            activity=activity
        )
    except Exception:
        pass

    logger.info(
        "✨ Bot online: %s (%s)",
        bot.user,
        bot.user.id if bot.user else "?"
    )


# ============================================================
# PLAY
# ============================================================

@bot.tree.command(
    name="play",
    description="🔮 Phát nhạc từ SoundCloud"
)
@discord.app_commands.describe(
    query="Tên bài hát hoặc link SoundCloud"
)
async def play(
    interaction: discord.Interaction,
    query: str
):
    await interaction.response.defer()

    guild = interaction.guild

    if not guild:
        return await interaction.followup.send(
            "⚠️ Lệnh này chỉ sử dụng được trong server.",
            ephemeral=True
        )

    if not interaction.user.voice:
        return await interaction.followup.send(
            "🥀 Bạn cần vào voice channel trước.",
            ephemeral=True
        )

    safe_query = SecuritySanitizer.sanitize_input(query)

    if not safe_query:
        return await interaction.followup.send(
            "⚠️ Từ khóa không hợp lệ.",
            ephemeral=True
        )

    guild_id = guild.id

    async with guild_locks[guild_id]:

        if len(queues[guild_id]) >= MAX_QUEUE_SIZE:
            return await interaction.followup.send(
                f"⚠️ Hàng đợi đã đầy ({MAX_QUEUE_SIZE} bài).",
                ephemeral=True
            )

        vc = guild.voice_client

        # ----------------------------------------------------
        # CONNECT
        # ----------------------------------------------------

        if not vc:
            try:
                vc = await interaction.user.voice.channel.connect(
                    self_deaf=True
                )

            except Exception as exc:
                logger.error(
                    "[Voice] Connect failed: %s",
                    exc
                )

                return await interaction.followup.send(
                    f"⚠️ Không thể kết nối voice: `{exc}`",
                    ephemeral=True
                )

        else:
            # Nếu bot đang ở channel khác
            if (
                vc.channel != interaction.user.voice.channel
                and not vc.is_playing()
            ):
                try:
                    await vc.move_to(
                        interaction.user.voice.channel
                    )
                except Exception:
                    pass

        # ----------------------------------------------------
        # RESOLVE
        # ----------------------------------------------------

        try:
            stream_url, title = await SelfHealingEngine.resolve_stream(
                safe_query
            )

        except Exception as exc:
            return await interaction.followup.send(
                f"⚠️ Không thể tìm bài hát:\n`{exc}`",
                ephemeral=True
            )

        track = Track(
            query=safe_query,
            title=title,
            stream_url=stream_url
        )

        # ----------------------------------------------------
        # PLAY NOW
        # ----------------------------------------------------

        if (
            not vc.is_playing()
            and not vc.is_paused()
            and guild_id not in current_tracks
        ):
            asyncio.create_task(
                play_track(
                    guild,
                    track,
                    interaction.channel
                )
            )

            cancel_sleep_timer(guild_id)

            return await interaction.followup.send(
                embed=discord.Embed(
                    title="✧ ── ✦ 𝕸𝖚𝖘𝖎𝖈 𝖑𝖆𝖓𝖌𝖓𝖌𝖔 ✦ ── ✧",
                    description=(
                        f"🔮 **Đang chuẩn bị phát:**\n"
                        f"**{title}**"
                    ),
                    color=discord.Color.from_rgb(
                        88, 24, 131
                    )
                ),
                view=MusicControlView()
            )

        # ----------------------------------------------------
        # ADD QUEUE
        # ----------------------------------------------------

        queues[guild_id].append(track)

        # Prefetch bài vừa thêm.
        asyncio.create_task(
            SelfHealingEngine.background_prefetch(
                safe_query
            )
        )

        position = len(queues[guild_id])

        return await interaction.followup.send(
            embed=discord.Embed(
                title="💠 Đã thêm vào hàng đợi",
                description=(
                    f"**{title}**\n"
                    f"Vị trí: `{position}/{MAX_QUEUE_SIZE}`"
                ),
                color=discord.Color.from_rgb(
                    45, 10, 75
                )
            )
        )


# ============================================================
# MY FAVORITE
# ============================================================

@bot.tree.command(
    name="myfavorite",
    description="🖤 Nạp bộ sưu tập cá nhân"
)
async def myfavorite(
    interaction: discord.Interaction
):
    if not interaction.user.voice:
        return await interaction.response.send_message(
            "🥀 Bạn cần vào voice channel trước.",
            ephemeral=True
        )

    favs = user_collections[interaction.user.id]

    if not favs:
        return await interaction.response.send_message(
            "📭 Kho lưu trữ trống.",
            ephemeral=True
        )

    await interaction.response.defer()

    guild = interaction.guild
    guild_id = guild.id

    async with guild_locks[guild_id]:

        if len(queues[guild_id]) >= MAX_QUEUE_SIZE:
            return await interaction.followup.send(
                "⚠️ Hàng đợi đã đầy.",
                ephemeral=True
            )

        vc = guild.voice_client

        if not vc:
            try:
                vc = await interaction.user.voice.channel.connect(
                    self_deaf=True
                )
            except Exception as exc:
                return await interaction.followup.send(
                    f"⚠️ Không thể kết nối voice: `{exc}`",
                    ephemeral=True
                )

        available_slots = (
            MAX_QUEUE_SIZE - len(queues[guild_id])
        )

        # Giữ đúng thứ tự collection.
        selected = favs[:available_slots]

        tracks = []

        # Không giữ guild lock trong thời gian extraction.
        # Nhưng ở đây đang trong lock -> thoát lock trước extraction.
        # Chỉ lấy snapshot.
    
    selected = list(selected)

    added = 0
    first_track = None

    for item in selected:

        try:
            track = Track(
                query=item["query"],
                title=item["title"]
            )

            # Resolve lần lượt để tránh tạo quá nhiều request.
            stream_url, title = await SelfHealingEngine.resolve_stream(
                track.query
            )

            track.stream_url = stream_url
            track.title = title

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

                else:
                    if len(queues[guild_id]) < MAX_QUEUE_SIZE:
                        queues[guild_id].append(track)
                    else:
                        break

            added += 1

        except Exception as exc:
            logger.warning(
                "[Favorite] Không thể load '%s': %s",
                item.get("title"),
                exc
            )

    if first_track:

        await play_track(
            guild,
            first_track,
            interaction.channel
        )

    await interaction.followup.send(
        embed=discord.Embed(
            title="🖤 Bộ Sưu Tập",
            description=(
                f"Đã nạp **{added} bài** "
                f"từ bộ sưu tập cá nhân."
            ),
            color=discord.Color.from_rgb(
                88, 24, 131
            )
        ),
        view=MusicControlView()
    )


# ============================================================
# QUEUE
# ============================================================

@bot.tree.command(
    name="queue",
    description="📜 Xem hàng đợi"
)
async def queue_cmd(
    interaction: discord.Interaction
):
    guild_id = interaction.guild.id

    current = current_tracks.get(guild_id)
    q = queues[guild_id]

    if not current and not q:
        return await interaction.response.send_message(
            "📭 Hàng đợi trống.",
            ephemeral=True
        )

    parts = []

    if current:
        parts.append(
            f"🔮 **Đang phát:**\n"
            f"`▶` **{current.title}**"
        )

    if q:
        lines = []

        for index, track in enumerate(q, start=1):
            lines.append(
                f"`{index:02d}.` {track.title}"
            )

        parts.append(
            "📜 **Tiếp theo:**\n"
            + "\n".join(lines)
        )

    total = len(q) + (1 if current else 0)

    embed = discord.Embed(
        title=f"⚡ Danh Sách Phát ({total} bài)",
        description="\n\n".join(parts),
        color=discord.Color.from_rgb(
            60, 15, 100
        )
    )

    await interaction.response.send_message(
        embed=embed,
        ephemeral=True
    )


# ============================================================
# REMOVE
# ============================================================

@bot.tree.command(
    name="remove",
    description="🗑️ Xóa bài khỏi hàng đợi"
)
@discord.app_commands.describe(
    index="Vị trí bắt đầu",
    count="Số lượng bài cần xóa"
)
async def remove(
    interaction: discord.Interaction,
    index: int,
    count: int = 1
):
    guild_id = interaction.guild.id

    async with guild_locks[guild_id]:

        q = queues[guild_id]

        if not q:
            return await interaction.response.send_message(
                "📭 Hàng đợi trống.",
                ephemeral=True
            )

        if not (1 <= index <= len(q)):
            return await interaction.response.send_message(
                "⚠️ Vị trí không hợp lệ.",
                ephemeral=True
            )

        count = max(
            1,
            min(count, len(q) - index + 1)
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
        ephemeral=True
    )


# ============================================================
# MOVE
# ============================================================

@bot.tree.command(
    name="move",
    description="🔄 Đổi vị trí bài hát"
)
@discord.app_commands.describe(
    from_pos="Vị trí cũ",
    to_pos="Vị trí mới"
)
async def move(
    interaction: discord.Interaction,
    from_pos: int,
    to_pos: int
):
    guild_id = interaction.guild.id

    async with guild_locks[guild_id]:

        q = queues[guild_id]

        if not q:
            return await interaction.response.send_message(
                "📭 Hàng đợi trống.",
                ephemeral=True
            )

        if not (
            1 <= from_pos <= len(q)
            and 1 <= to_pos <= len(q)
        ):
            return await interaction.response.send_message(
                "⚠️ Vị trí không hợp lệ.",
                ephemeral=True
            )

        track = q.pop(from_pos - 1)
        q.insert(to_pos - 1, track)

    await interaction.response.send_message(
        f"🔄 Đã chuyển **{track.title}** "
        f"từ `{from_pos}` → `{to_pos}`.",
        ephemeral=True
    )


# ============================================================
# DUPLICATE
# ============================================================

@bot.tree.command(
    name="duplicate",
    description="🧬 Nhân bản bài hát"
)
@discord.app_commands.describe(
    index="0 = bài đang phát, 1-10 = bài trong queue",
    amount="Số bản sao từ 1 đến 5"
)
async def duplicate(
    interaction: discord.Interaction,
    index: int,
    amount: int
):
    guild_id = interaction.guild.id

    if not (1 <= amount <= 5):
        return await interaction.response.send_message(
            "⚠️ Amount phải từ 1 đến 5.",
            ephemeral=True
        )

    async with guild_locks[guild_id]:

        q = queues[guild_id]

        if len(q) + amount > MAX_QUEUE_SIZE:
            return await interaction.response.send_message(
                "⚠️ Hàng đợi không đủ chỗ.",
                ephemeral=True
            )

        if index == 0:

            target = current_tracks.get(guild_id)

            if not target:
                return await interaction.response.send_message(
                    "⚠️ Không có bài đang phát.",
                    ephemeral=True
                )

        else:

            if not (1 <= index <= len(q)):
                return await interaction.response.send_message(
                    "⚠️ Vị trí không hợp lệ.",
                    ephemeral=True
                )

            target = q[index - 1]

        copies = [
            Track(
                query=target.query,
                title=target.title,
                stream_url=""
            )
            for _ in range(amount)
        ]

        insert_position = (
            0 if index == 0
            else index
        )

        for offset, copy in enumerate(copies):
            q.insert(
                insert_position + offset,
                copy
            )

    await interaction.response.send_message(
        f"🧬 Đã thêm **{amount} bản sao** "
        f"của **{target.title}**.",
        ephemeral=True
    )


# ============================================================
# VOLUME
# ============================================================

@bot.tree.command(
    name="volume",
    description="🔊 Chỉnh âm lượng"
)
@discord.app_commands.describe(
    level="Mức âm lượng 1-100"
)
async def volume(
    interaction: discord.Interaction,
    level: int
):
    if not (1 <= level <= 100):
        return await interaction.response.send_message(
            "⚠️ Âm lượng phải từ 1 đến 100.",
            ephemeral=True
        )

    guild_id = interaction.guild.id

    volumes[guild_id] = level / 100

    vc = interaction.guild.voice_client

    if vc and vc.source:
        try:
            if isinstance(
                vc.source,
                discord.PCMVolumeTransformer
            ):
                vc.source.volume = level / 100

        except Exception as exc:
            logger.debug(
                "[Volume] %s",
                exc
            )

    await interaction.response.send_message(
        f"🔊 Âm lượng: **{level}%**",
        ephemeral=True
    )


# ============================================================
# SLEEP
# ============================================================

@bot.tree.command(
    name="sleep",
    description="🌙 Hẹn giờ ngắt bot"
)
@discord.app_commands.describe(
    minutes="1-180 phút"
)
async def sleep(
    interaction: discord.Interaction,
    minutes: int
):
    if not (1 <= minutes <= 180):
        return await interaction.response.send_message(
            "⚠️ Thời gian từ 1 đến 180 phút.",
            ephemeral=True
        )

    guild = interaction.guild
    guild_id = guild.id

    cancel_sleep_timer(guild_id)

    async def timer():

        try:
            await asyncio.sleep(
                minutes * 60
            )

            async with guild_locks[guild_id]:
                await disconnect_guild(guild)

            if interaction.channel:
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
            sleep_tasks.pop(
                guild_id,
                None
            )

    sleep_tasks[guild_id] = asyncio.create_task(
        timer()
    )

    await interaction.response.send_message(
        f"⏰ Bot sẽ tự ngắt sau **{minutes} phút**.",
        ephemeral=True
    )


# ============================================================
# COLLECTION
# ============================================================

@bot.tree.command(
    name="collection",
    description="💼 Quản lý bộ sưu tập cá nhân"
)
async def collection(
    interaction: discord.Interaction
):
    embed = discord.Embed(
        title="🌌 Kho Lưu Trữ Cá Nhân",
        description=(
            "Sử dụng các nút bên dưới để quản lý "
            "bộ sưu tập của bạn."
        ),
        color=discord.Color.from_rgb(
            88, 24, 131
        )
    )

    await interaction.response.send_message(
        embed=embed,
        view=CollectionView(
            interaction.user.id
        ),
        ephemeral=True
    )


# ============================================================
# DELETE MY DATA
# ============================================================

@bot.tree.command(
    name="delete-my-data",
    description="🛡️ Xóa dữ liệu cá nhân"
)
async def delete_my_data(
    interaction: discord.Interaction
):
    user_id = interaction.user.id

    existed = user_id in user_collections

    user_collections.pop(
        user_id,
        None
    )

    if existed:
        await interaction.response.send_message(
            "🛡️ Đã xóa toàn bộ dữ liệu bộ sưu tập "
            "cá nhân đang được lưu trong bộ nhớ bot.",
            ephemeral=True
        )
    else:
        await interaction.response.send_message(
            "📭 Không có dữ liệu cá nhân được lưu.",
            ephemeral=True
        )


# ============================================================
# HELP
# ============================================================

@bot.tree.command(
    name="help",
    description="🚀 Hướng dẫn sử dụng bot"
)
async def help_cmd(
    interaction: discord.Interaction
):
    embed = discord.Embed(
        title="✧ ── ✦ WORKSTATION MUSIC BOT ✦ ── ✧",
        color=discord.Color.from_rgb(
            88, 24, 131
        )
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
            inline=False
        )

    await interaction.response.send_message(
        embed=embed,
        ephemeral=True
    )


# ============================================================
# ERROR HANDLER
# ============================================================

@bot.tree.error
async def on_app_command_error(
    interaction: discord.Interaction,
    error: discord.app_commands.AppCommandError
):

    logger.error(
        "[SlashCommand] %s",
        error,
        exc_info=True
    )

    message = (
        "⚠️ Đã xảy ra lỗi khi xử lý lệnh. "
        "Vui lòng thử lại."
    )

    try:
        if interaction.response.is_done():
            await interaction.followup.send(
                message,
                ephemeral=True
            )
        else:
            await interaction.response.send_message(
                message,
                ephemeral=True
            )

    except Exception:
        pass


# ============================================================
# SHUTDOWN
# ============================================================

async def shutdown():
    logger.info("🛑 Đang shutdown Workstation Bot...")

    for task in sleep_tasks.values():
        task.cancel()

    sleep_tasks.clear()

    for guild in bot.guilds:
        try:
            await disconnect_guild(guild)
        except Exception:
            pass

    WORKSTATION_POOL.shutdown(
        wait=False,
        cancel_futures=True
    )


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    TOKEN = os.getenv("DISCORD_TOKEN")

    if not TOKEN:
        logger.critical(
            "🚨 Không tìm thấy DISCORD_TOKEN."
        )

        raise SystemExit(1)

    try:
        bot.run(TOKEN)

    except KeyboardInterrupt:
        logger.info(
            "🛑 Bot stopped by user."
        )

    except Exception as exc:
        logger.critical(
            "🚨 Bot crashed: %s",
            exc,
            exc_info=True
        )
