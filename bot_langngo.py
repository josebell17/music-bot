import os
import asyncio
import logging
import discord
from discord.ext import commands
import yt_dlp
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict

# [NÂNG CẤP] Cấu hình ghi log chuẩn Production với mã hóa UTF-8 chống lỗi font Tiếng Việt trên console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [Workstation-Core] %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
for handler in logging.root.handlers:
    if isinstance(handler, logging.StreamHandler):
        try:
            handler.stream.reconfigure(encoding='utf-8')
        except AttributeError:
            pass

logger = logging.getLogger("WorkstationProductionBot")

# [NÂNG CẤP] Thiết lập đầy đủ intents cần thiết cho bot âm nhạc chuyên nghiệp
intents = discord.Intents.default()
intents.message_content = True
intents.voice_states = True
intents.guilds = True
intents.guild_messages = True

# Khởi tạo đối tượng Bot
bot = commands.Bot(command_prefix="!", intents=intents)

# [VÁ LỖI 1] Tăng worker pool xử lý song song
WORKSTATION_POOL = ThreadPoolExecutor(max_workers=4, thread_name_prefix="Production_Worker")

# Định nghĩa LimitedCache trước khi khởi tạo các biến bộ nhớ đệm
class LimitedCache(OrderedDict):
    def __init__(self, maxsize=150, *args, **kwargs):
        self.maxsize = maxsize
        super().__init__(*args, **kwargs)
    def __setitem__(self, key, value):
        if key in self:
            self.move_to_end(key)
        super().__setitem__(key, value)
        if len(self) > self.maxsize:
            oldest = next(iter(self))
            del self[oldest]

# Bộ nhớ đệm thông minh & Pre-fetch Cache
URL_CACHE = LimitedCache(maxsize=150)
FAILED_TRACKS_LEDGER = defaultdict(int)
queues = defaultdict(list)
volumes = defaultdict(lambda: 0.5)
sleep_tasks = {}
user_collections = defaultdict(list)
user_consent_ledger = set()
guild_locks = defaultdict(asyncio.Lock)

YTDL_OPTIONS = {
    'format': 'bestaudio/best',
    'extractaudio': True,
    'audioformat': 'mp3',
    'noplaylist': True,
    'nocheckcertificate': True,
    'quiet': True,
    'no_warnings': True,
    'socket_timeout': 15,
    'cachedir': False,
    'ignoreerrors': True,
}

FFMPEG_OPTIONS = {
    'before_options': '-reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5 -nostdin -analyzeduration 0 -probesize 32 -loglevel 0',
    'options': '-vn -b:a 192k',
}

ytdl = yt_dlp.YoutubeDL(YTDL_OPTIONS)

class SecuritySanitizer:
    @staticmethod
    def sanitize_input(text: str) -> str:
        if not text:
            return ""
        cleaned = "".join(ch for ch in text if ch.isalnum() or ch in " -_./:?&=+@")
        return cleaned.strip()[:200]

class SelfHealingEngine:
    @staticmethod
    async def resolve_stream(query: str, loop, cached_stream_url=None):
        safe_query = SecuritySanitizer.sanitize_input(query)
        stream_url = cached_stream_url
        title = safe_query

        # Ưu tiên lấy từ cache trước để chuyển bài mượt mà, không bị khựng
        if stream_url:
            return stream_url, title

        if safe_query in URL_CACHE:
            stream_url, title = URL_CACHE[safe_query]
        else:
            search_query = safe_query if safe_query.startswith("http") else f"scsearch:{safe_query}"
            
            def extract_worker():
                try:
                    data = ytdl.extract_info(search_query, download=False)
                    if 'entries' in data and data['entries']:
                        data = data['entries'][0]
                    return data
                except Exception as ex:
                    logger.error(f"[Extraction Error] Lỗi trích xuất: {ex}")
                    return None

            try:
                data = await asyncio.wait_for(
                    loop.run_in_executor(WORKSTATION_POOL, extract_worker), 
                    timeout=12.0
                )
            except asyncio.TimeoutError:
                data = None

            if data and 'url' in data:
                stream_url = data['url']
                title = data.get('title', safe_query)
                URL_CACHE[safe_query] = (stream_url, title)
            else:
                raise Exception("Không thể tìm thấy hoặc trích xuất được bài hát này.")

        return stream_url, title

    @staticmethod
    async def background_prefetch(query: str, loop):
        try:
            safe_query = SecuritySanitizer.sanitize_input(query)
            if safe_query and safe_query not in URL_CACHE:
                search_query = safe_query if safe_query.startswith("http") else f"scsearch:{safe_query}"
                def extract_worker():
                    try:
                        data = ytdl.extract_info(search_query, download=False)
                        if 'entries' in data and data['entries']:
                            data = data['entries'][0]
                        return data
                    except:
                        return None
                data = await loop.run_in_executor(WORKSTATION_POOL, extract_worker)
                if data and 'url' in data:
                    URL_CACHE[safe_query] = (data['url'], data.get('title', safe_query))
        except Exception:
            pass

    @staticmethod
    def purge_cache_key(search: str):
        if search in URL_CACHE:
            del URL_CACHE[search]

class YTDLSource(discord.FFmpegOpusAudio):
    def __init__(self, source, *, data, volume=0.5):
        super().__init__(source, **FFMPEG_OPTIONS)
        self.data = data
        self.title = data.get('title', 'Unknown Title')
        self.stream_url = data.get('url', '')
        self._volume = volume

    @property
    def volume(self):
        return self._volume

    @volume.setter
    def volume(self, val):
        self._volume = max(0.0, min(val, 1.0))

    @classmethod
    async def create_source(cls, search: str, *, loop=None, volume=0.5, cached_stream_url=None):
        loop = loop or asyncio.get_running_loop()
        try:
            stream_url, title = await SelfHealingEngine.resolve_stream(search, loop, cached_stream_url)
            return cls(stream_url, data={'title': title, 'url': stream_url}, volume=volume)
        except Exception as e:
            SelfHealingEngine.purge_cache_key(search)
            stream_url, title = await SelfHealingEngine.resolve_stream(search, loop, None)
            return cls(stream_url, data={'title': title, 'url': stream_url}, volume=volume)

async def play_next(ctx):
    guild_id = ctx.guild.id
    async with guild_locks[guild_id]:
        if len(queues[guild_id]) > 0:
            player = queues[guild_id].pop(0)
            player.volume = volumes[guild_id]
            
            if not hasattr(bot, 'current_players'):
                bot.current_players = {}
            bot.current_players[guild_id] = player
            
            if len(queues[guild_id]) > 0:
                next_song = queues[guild_id][0]
                bot.loop.create_task(SelfHealingEngine.background_prefetch(next_song.title, bot.loop))

            def after_playing(error):
                if error:
                    logger.error(f"[Playback Error] Sự cố phần cứng/mạng: {error}")
                asyncio.run_coroutine_threadsafe(play_next(ctx), bot.loop)

            if ctx.voice_client and ctx.voice_client.is_connected():
                try:
                    if ctx.voice_client.is_playing() or ctx.voice_client.is_paused():
                        ctx.voice_client.stop()
                        
                    ctx.voice_client.play(player, after=after_playing)
                    embed = discord.Embed(
                        title="✧ ── ✦ 𝕸𝖚𝖘𝖎𝖈 𝖑𝖆𝖓𝖌𝖓𝖌𝖔 ✦ ── ✧", 
                        description=f"🔮 **Đang Phát:** \n**{player.title}**", 
                        color=discord.Color.from_rgb(88, 24, 131)
                    )
                    asyncio.run_coroutine_threadsafe(ctx.send(embed=embed, view=MusicControlView(ctx)), bot.loop)
                except Exception as ex:
                    logger.error(f"[Play Exception] Không thể phát bài hát: {ex}")
                    asyncio.run_coroutine_threadsafe(play_next(ctx), bot.loop)
        else:
            if hasattr(bot, 'current_players'):
                bot.current_players.pop(guild_id, None)
            if ctx.voice_client and ctx.voice_client.is_connected():
                await asyncio.sleep(60)
                if ctx.voice_client and not ctx.voice_client.is_playing() and len(queues[guild_id]) == 0:
                    await ctx.voice_client.disconnect()
                    if guild_id in sleep_tasks:
                        sleep_tasks[guild_id].cancel()
                        sleep_tasks.pop(guild_id, None)

class MusicControlView(discord.ui.View):
    def __init__(self, ctx):
        super().__init__(timeout=None)
        self.ctx = ctx

    @discord.ui.button(style=discord.ButtonStyle.secondary, emoji="⏸️", custom_id="persistent_view:pause")
    async def pause(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True)

        async with guild_locks[interaction.guild.id]:
            vc = interaction.guild.voice_client
            if vc and vc.is_playing():
                vc.pause()
                await interaction.followup.send("💤 Đã tạm dừng phát nhạc.", ephemeral=True)
            else:
                await interaction.followup.send("⚠️ Không có nhạc đang phát.", ephemeral=True)

    @discord.ui.button(style=discord.ButtonStyle.secondary, emoji="▶️", custom_id="persistent_view:resume")
    async def resume(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True)

        async with guild_locks[interaction.guild.id]:
            vc = interaction.guild.voice_client
            if vc and vc.is_paused():
                vc.resume()
                await interaction.followup.send("🔮 Đã tiếp tục phát nhạc.", ephemeral=True)
            else:
                await interaction.followup.send("⚠️ Nhạc không ở trạng thái tạm dừng.", ephemeral=True)

    @discord.ui.button(style=discord.ButtonStyle.secondary, emoji="⏭️", custom_id="persistent_view:skip")
    async def skip(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True)

        async with guild_locks[interaction.guild.id]:
            vc = interaction.guild.voice_client
            if vc and (vc.is_playing() or vc.is_paused()):
                vc.stop()
                await interaction.followup.send("📿 Đã chuyển bài an toàn.", ephemeral=True)
            else:
                await interaction.followup.send("⚠️ Hàng đợi trống.", ephemeral=True)

    @discord.ui.button(style=discord.ButtonStyle.secondary, emoji="🖤", custom_id="persistent_view:fast_save")
    async def fast_save(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True)

        user_id = interaction.user.id
        guild_id = interaction.guild.id
        
        current = getattr(interaction.client, 'current_players', {}).get(guild_id, None)
        if not current:
            vc = interaction.guild.voice_client
            current = getattr(vc, 'current_source', None)

        if not current:
            return await interaction.followup.send("⚠️ Không có bài hát nào đang hoạt động.", ephemeral=True)
        
        favs = user_collections[user_id]
        if len(favs) >= 10:
            return await interaction.followup.send("⚠️ Kho lưu trữ cá nhân đã đạt giới hạn tối đa 10 bài!", ephemeral=True)
            
        if not any(song['title'] == current.title for song in favs):
            favs.append({'title': current.title, 'stream_url': getattr(current, 'stream_url', '')})
            interaction.client.loop.create_task(SelfHealingEngine.background_prefetch(current.title, interaction.client.loop))
            await interaction.followup.send(f"✨ Đã lưu vào kho và đệm sẵn dữ liệu: **{current.title}** (`{len(favs)}/10`)", ephemeral=True)
        else:
            await interaction.followup.send("💠 Bài hát đã có trong bộ sưu tập.", ephemeral=True)

    @discord.ui.button(style=discord.ButtonStyle.danger, emoji="⏹️", custom_id="persistent_view:stop")
    async def stop(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True)

        async with guild_locks[interaction.guild.id]:
            queues[interaction.guild.id].clear()
            if hasattr(bot, 'current_players'):
                bot.current_players.pop(interaction.guild.id, None)
            vc = interaction.guild.voice_client
            if vc:
                await vc.disconnect()
                await interaction.followup.send("🌌 Đã ngắt kết nối hệ thống.", ephemeral=True)

class RemoveCollectionModal(discord.ui.Modal, title="🗑️ Xóa Bài Hát Khỏi Bộ Sưu Tập"):
    index_str = discord.ui.TextInput(label="Số thứ tự bài cần xóa", placeholder="Nhập số (ví dụ: 1)", required=True)
    async def on_submit(self, interaction: discord.Interaction):
        user_id = interaction.user.id
        favs = user_collections[user_id]
        try:
            idx = int(self.index_str.value)
            if 1 <= idx <= len(favs):
                removed = favs.pop(idx - 1)
                await interaction.response.send_message(f"🗑️ Đã xóa **{removed['title']}**!", ephemeral=True)
            else:
                await interaction.response.send_message("⚠️ Số thứ tự không hợp lệ.", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("⚠️ Vui lòng chỉ nhập số.", ephemeral=True)

class ReorderCollectionModal(discord.ui.Modal, title="🔄 Sắp Xếp Lại Bộ Sưu Tập"):
    from_pos = discord.ui.TextInput(label="Vị trí cũ", placeholder="Ví dụ: 3", required=True)
    to_pos = discord.ui.TextInput(label="Vị trí mới", placeholder="Ví dụ: 1", required=True)
    async def on_submit(self, interaction: discord.Interaction):
        user_id = interaction.user.id
        favs = user_collections[user_id]
        try:
            f, t = int(self.from_pos.value), int(self.to_pos.value)
            if 1 <= f <= len(favs) and 1 <= t <= len(favs):
                song = favs.pop(f - 1)
                favs.insert(t - 1, song)
                await interaction.response.send_message(f"🔄 Đã dịch chuyển **{song['title']}** sang vị trí `{t}`!", ephemeral=True)
            else:
                await interaction.response.send_message("⚠️ Vị trí vượt quá giới hạn.", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("⚠️ Vui lòng chỉ nhập số.", ephemeral=True)

class CollectionView(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=60)
        self.user_id = user_id

    @discord.ui.button(style=discord.ButtonStyle.success, emoji="📥", label="Lưu bài đang phát")
    async def save_current(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild_id = interaction.guild.id
        current = getattr(bot, 'current_players', {}).get(guild_id, None)
        if not current:
            vc = interaction.guild.voice_client
            current = getattr(vc, 'current_source', None)

        if not current:
            return await interaction.response.send_message("⚠️ Không có bài hát đang phát!", ephemeral=True)
        
        favs = user_collections[self.user_id]
        if len(favs) >= 10:
            return await interaction.response.send_message("⚠️ Kho lưu trữ cá nhân đã đạt giới hạn tối đa 10 bài!", ephemeral=True)

        if not any(song['title'] == current.title for song in favs):
            favs.append({'title': current.title, 'stream_url': getattr(current, 'stream_url', '')})
            bot.loop.create_task(SelfHealingEngine.background_prefetch(current.title, bot.loop))
            await interaction.response.send_message(f"✨ Đã lưu vào kho: **{current.title}** (`{len(favs)}/10`)", ephemeral=True)
        else:
            await interaction.response.send_message("💠 Đã có sẵn trong kho.", ephemeral=True)

    @discord.ui.button(style=discord.ButtonStyle.primary, emoji="📂", label="Xem danh sách")
    async def view_list(self, interaction: discord.Interaction, button: discord.ui.Button):
        favs = user_collections[self.user_id]
        if not favs:
            return await interaction.response.send_message("📭 Kho lưu trữ trống.", ephemeral=True)
        fav_list = "\n".join([f"` ⟡ {i+1}. ` {song['title']}" for i, song in enumerate(favs)])
        embed = discord.Embed(title=f"🔮 Kho Của — {interaction.user.name} ({len(favs)}/10)", description=fav_list, color=discord.Color.from_rgb(88, 24, 131))
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @discord.ui.button(style=discord.ButtonStyle.danger, emoji="🗑️", label="Xóa bài")
    async def remove_song(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(RemoveCollectionModal())

    @discord.ui.button(style=discord.ButtonStyle.secondary, emoji="🔄", label="Sắp xếp")
    async def reorder_song(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(ReorderCollectionModal())

@bot.event
async def on_ready():
    try:
        synced = await bot.tree.sync()
        logger.info(f"✨ Hệ thống Production Bot đã trực tuyến: {bot.user} (ID: {bot.user.id})")
        logger.info(f"🚀 Đã đồng bộ thành công {len(synced)} lệnh Slash (/) lên Discord.")
        
        activity = discord.Activity(type=discord.ActivityType.listening, name="/help | 24/7 Music Production")
        await bot.change_presence(status=discord.Status.online, activity=activity)
    except Exception as e:
        logger.error(f"⚠️ Lỗi đồng bộ slash command: {e}")

@bot.tree.command(name="play", description="🔮 Phát nhạc mượt mà từ SoundCloud")
@discord.app_commands.describe(query="Tên bài hát hoặc link SoundCloud")
async def play(interaction: discord.Interaction, query: str):
    await interaction.response.defer(ephemeral=False)

    if not interaction.user.voice:
        return await interaction.followup.send("🥀 Vui lòng vào kênh thoại trước.", ephemeral=True)

    safe_query = SecuritySanitizer.sanitize_input(query)
    if not safe_query:
        return await interaction.followup.send("⚠️ Từ khóa không hợp lệ.", ephemeral=True)

    guild_id = interaction.guild.id
    if len(queues[guild_id]) >= 10:
        return await interaction.followup.send("⚠️ Hàng đợi đã đạt giới hạn tối đa 10 bài!", ephemeral=True)
    
    vc = interaction.guild.voice_client
    if not vc:
        try:
            vc = await interaction.user.voice.channel.connect()
        except Exception as e:
            return await interaction.followup.send(f"⚠️ Lỗi kết nối thoại: {e}")

    try:
        current_vol = volumes[guild_id]
        player = await YTDLSource.create_source(safe_query, loop=bot.loop, volume=current_vol)
        
        if not hasattr(bot, 'current_players'):
            bot.current_players = {}
        bot.current_players[guild_id] = player

        async with guild_locks[guild_id]:
            if vc.is_playing() or vc.is_paused():
                queues[guild_id].append(player)
                embed = discord.Embed(title="💠 Đã thêm vào hàng đợi", description=f"**{player.title}** (`#{len(queues[guild_id])}/10`)", color=discord.Color.from_rgb(45, 10, 75))
                await interaction.followup.send(embed=embed)
                bot.loop.create_task(SelfHealingEngine.background_prefetch(safe_query, bot.loop))
            else:
                def after_playing(error):
                    if error:
                        logger.error(f"[Player Error] Lỗi phát nhạc tại guild {guild_id}: {error}")
                    asyncio.run_coroutine_threadsafe(play_next(interaction.guild), bot.loop)

                vc.play(player, after=after_playing)
                embed = discord.Embed(title="✧ ── ✦ 𝕸𝖚𝖘𝖎𝖈 𝖑𝖆𝖓𝖌𝖓𝖌𝖔 ✦ ── ✧", description=f"🔮 **Đang Phát:** \n**{player.title}**", color=discord.Color.from_rgb(88, 24, 131))
                await interaction.followup.send(embed=embed, view=MusicControlView(interaction.guild))
    except Exception as e:
        await interaction.followup.send(f"⚠️ Lỗi xử lý dữ liệu: {e}")

@bot.tree.command(name="myfavorite", description="🖤 Nạp siêu tốc từ bộ nhớ đệm kho cá nhân với cơ chế dự phòng")
async def myfavorite(interaction: discord.Interaction):
    if not interaction.user.voice:
        return await interaction.response.send_message("🥀 Vui lòng vào kênh thoại trước.", ephemeral=True)
    
    user_id = interaction.user.id
    favs = user_collections[user_id]
    if not favs:
        return await interaction.response.send_message("📭 Kho lưu trữ trống.", ephemeral=True)

    guild_id = interaction.guild.id
    if len(queues[guild_id]) >= 10:
        return await interaction.response.send_message("⚠️ Hàng đợi đã đầy!", ephemeral=True)

    await interaction.response.defer()
    
    vc = interaction.guild.voice_client
    if not vc:
        try:
            vc = await interaction.user.voice.channel.connect()
        except Exception as e:
            return await interaction.followup.send(f"⚠️ Lỗi kết nối: {e}")

    current_vol = volumes[guild_id]
    added, first_player = 0, None

    for item in favs:
        if len(queues[guild_id]) >= 10:
            break
            
        try:
            player = await YTDLSource.create_source(
                item['title'], 
                loop=bot.loop, 
                volume=current_vol, 
                cached_stream_url=item.get('stream_url')
            )
            async with guild_locks[guild_id]:
                if not first_player and not vc.is_playing() and not vc.is_paused():
                    first_player = player
                    if not hasattr(bot, 'current_players'):
                        bot.current_players = {}
                    bot.current_players[guild_id] = player
                else:
                    queues[guild_id].append(player)
            added += 1
            await asyncio.sleep(0.05)
        except Exception:
            continue

    if first_player:
        def after_playing(error):
            if error:
                logger.error(f"[Player Error] Lỗi phát nhạc tại guild {guild_id}: {error}")
            asyncio.run_coroutine_threadsafe(play_next(interaction.guild), bot.loop)

        vc.play(first_player, after=after_playing)
        embed = discord.Embed(title="🖤 Đang Phát Kho Lưu Trữ", description=f"Đã nạp siêu tốc **{added} bài** từ bộ sưu tập.", color=discord.Color.from_rgb(88, 24, 131))
        await interaction.followup.send(embed=embed, view=MusicControlView(interaction.guild))
    elif added > 0:
        await interaction.followup.send(f"🖤 Đã nạp thêm **{added} bài** từ kho vào hàng đợi.")
    else:
        await interaction.followup.send("⚠️ Không thể khởi tạo danh sách từ kho.", ephemeral=True)

@bot.tree.command(name="queue", description="📜 Xem danh sách chờ (Tối đa 10 bài)")
async def queue_cmd(interaction: discord.Interaction):
    guild_id = interaction.guild.id
    vc = interaction.guild.voice_client
    current = getattr(bot, 'current_players', {}).get(guild_id, None)
    if not current and vc:
        current = getattr(vc, 'current_source', None)

    q = queues.get(guild_id, [])
    if not current and (not q or len(q) == 0):
        return await interaction.response.send_message("📭 Hàng đợi trống và không có bài hát nào đang phát.", ephemeral=True)

    desc_parts = []
    if current:
        desc_parts.append(f"🔮 **Đang Phát:**\n` ⟡ ▶ ` **{current.title}**\n")
    
    if q and len(q) > 0:
        desc_parts.append("📜 **Hàng Đợi Kế Tiếp:**")
        q_list = "\n".join([f"` ⟡ {i+1}. ` {song.title}" for i, song in enumerate(q[:10])])
        desc_parts.append(q_list)
    else:
        desc_parts.append("📜 **Hàng Đợi Kế Tiếp:**\n*Trống*")

    total_count = len(q) + (1 if current else 0)
    embed = discord.Embed(
        title=f"⚡ Danh Sách Phát Hệ Thống ({total_count} bài)", 
        description="\n".join(desc_parts), 
        color=discord.Color.from_rgb(60, 15, 100)
    )
    await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="remove", description="🗑️ Xóa bài hát khỏi hàng đợi")
@discord.app_commands.describe(index="Vị trí bài trong hàng đợi cần xóa", count="Số lượng bài muốn xóa liên tiếp")
async def remove(interaction: discord.Interaction, index: int, count: int = 1):
    guild_id = interaction.guild.id
    await interaction.response.defer(ephemeral=True)

    async with guild_locks[guild_id]:
        q = queues[guild_id]
        if not q or not (1 <= index <= len(q)):
            return await interaction.followup.send("⚠️ Vị trí bài hát trong hàng đợi không hợp lệ.", ephemeral=True)
        
        count = max(1, min(count, len(q) - index + 1))
        removed_items = [q.pop(index - 1) for _ in range(count)]
        
        titles = ", ".join([f"**{item.title}**" for item in removed_items])
        if len(titles) > 1500:
            titles = titles[:1500] + "..."
            
        await interaction.followup.send(f"🗑️ Đã xóa thành công **{count} bài** từ vị trí `{index}`:\n{titles}", ephemeral=True)

@bot.tree.command(name="duplicate", description="🧬 Nhân bản bài hát và chờ nạp hoàn tất vào hàng đợi mới phát")
@discord.app_commands.describe(index="Nhập 0 cho bài đang phát, hoặc số thứ tự", amount="Số bản sao (1-5)")
async def duplicate(interaction: discord.Interaction, index: int, amount: int):
    guild_id = interaction.guild.id
    q = queues[guild_id]
    current_vol = volumes[guild_id]
    
    if not (1 <= amount <= 5):
        return await interaction.response.send_message("⚠️ Số lượng nhân bản mỗi lần từ 1 đến 5.", ephemeral=True)
        
    if len(q) + amount > 10:
        return await interaction.response.send_message("⚠️ Vượt quá giới hạn hàng đợi! Tối đa 10 bài.", ephemeral=True)
    
    target_source = None
    target_title = "Bài hát"

    vc = interaction.guild.voice_client
    if index == 0:
        if vc and vc.is_playing():
            target_source = getattr(bot, 'current_players', {}).get(guild_id, None)
            if not target_source and hasattr(vc, 'source'):
                target_source = vc.source
            
            if target_source:
                target_title = getattr(target_source, 'title', "Bài đang phát")
            else:
                return await interaction.response.send_message("⚠️ Hiện tại không có bài hát nào đang hoạt động.", ephemeral=True)
        else:
            return await interaction.response.send_message("⚠️ Hiện tại không có bài hát nào đang phát.", ephemeral=True)
    else:
        if not q or not (1 <= index <= len(q)):
            return await interaction.response.send_message("⚠️ Số thứ tự trong hàng đợi không hợp lệ.", ephemeral=True)
        target_source = q[index - 1]
        target_title = getattr(target_source, 'title', "Bài hát trong hàng đợi")

    await interaction.response.send_message(f"🧬 Đang tiến hành nạp ngầm **{amount} bài** nhân bản vào hàng đợi...", ephemeral=True)

    async with guild_locks[guild_id]:
        try:
            cached_url = getattr(target_source, 'stream_url', None)
            new_players = []
            
            for _ in range(amount):
                duplicated_player = await YTDLSource.create_source(
                    target_title, 
                    loop=bot.loop, 
                    volume=current_vol, 
                    cached_stream_url=cached_url
                )
                new_players.append(duplicated_player)
                await asyncio.sleep(0.02)

            for i, duplicated_player in enumerate(new_players):
                if len(q) < 10:
                    if index == 0:
                        q.insert(i, duplicated_player)
                    else:
                        q.insert(index - 1 + i, duplicated_player)
                        
            await interaction.edit_original_response(content=f"🧬 Đã hoàn tất! Đã thêm thành công **{len(new_players)} bài** nhân bản của **{target_title}** vào hàng đợi.")
        except Exception as e:
            await interaction.edit_original_response(content=f"⚠️ Lỗi khi nạp nhân bản: {e}")

@bot.tree.command(name="move", description="🔄 Đổi vị trí bài hát trong hàng đợi")
@discord.app_commands.describe(from_pos="Vị trí cũ", to_pos="Vị trí mới")
async def move(interaction: discord.Interaction, from_pos: int, to_pos: int):
    guild_id = interaction.guild.id
    async with guild_locks[guild_id]:
        q = queues[guild_id]
        if not q or not (1 <= from_pos <= len(q)) or not (1 <= to_pos <= len(q)):
            return await interaction.response.send_message("⚠️ Vị trí không hợp lệ.", ephemeral=True)
        
        song = q.pop(from_pos - 1)
        q.insert(to_pos - 1, song)
        await interaction.response.send_message(f"🔄 Đã chuyển **{getattr(song, 'title', 'Bài hát')}** từ vị trí `{from_pos}` sang `{to_pos}`.", ephemeral=True)

@bot.tree.command(name="collection", description="💼 Quản lý kho lưu trữ cá nhân")
async def collection(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    embed = discord.Embed(title="🌌 Kho Lưu Trữ Cá Nhân", description="Sử dụng bảng điều khiển bảo mật bên dưới:", color=discord.Color.from_rgb(88, 24, 131))
    await interaction.followup.send(embed=embed, view=CollectionView(interaction.user.id), ephemeral=True)

@bot.tree.command(name="volume", description="🔊 Chỉnh âm lượng (1 - 100)")
@discord.app_commands.describe(level="Mức âm lượng")
async def volume(interaction: discord.Interaction, level: int):
    if not (1 <= level <= 100):
        return await interaction.response.send_message("⚠️ Chọn mức âm lượng từ 1 đến 100.", ephemeral=True)
    
    guild_id = interaction.guild.id
    volumes[guild_id] = level / 100.0
    
    vc = interaction.guild.voice_client
    if vc and vc.source:
        try:
            vc.source.volume = level / 100.0
        except AttributeError:
            pass

    await interaction.response.send_message(f"🔊 Đã chỉnh âm lượng hệ thống: **{level}%**", ephemeral=True)

@bot.tree.command(name="sleep", description="🌙 Hẹn giờ tự ngắt bot")
@discord.app_commands.describe(minutes="Số phút (1 - 180)")
async def sleep(interaction: discord.Interaction, minutes: int):
    if not (1 <= minutes <= 180):
        return await interaction.response.send_message("⚠️ Thời gian hẹn giờ từ 1 đến 180 phút.", ephemeral=True)

    guild_id = interaction.guild.id
    if guild_id in sleep_tasks:
        sleep_tasks[guild_id].cancel()
    
    async def timer():
        try:
            await asyncio.sleep(minutes * 60)
            async with guild_locks[guild_id]:
                vc = interaction.guild.voice_client
                if vc:
                    queues[guild_id].clear()
                    if hasattr(bot, 'current_players') and guild_id in bot.current_players:
                        bot.current_players.pop(guild_id, None)
                    await vc.disconnect()
                    try:
                        if interaction.channel:
                            await interaction.channel.send("🌙 Đã tự động ngắt kết nối do hết thời gian hẹn giờ.")
                    except Exception:
                        pass
        except asyncio.CancelledError:
            pass
        finally:
            sleep_tasks.pop(guild_id, None)

    sleep_tasks[guild_id] = bot.loop.create_task(timer())
    await interaction.response.send_message(f"⏰ Đã đặt hẹn giờ ngắt kết nối sau **{minutes} phút**.", ephemeral=True)

@bot.tree.command(name="delete-my-data", description="🛡️ Xóa toàn bộ dữ liệu cá nhân (Tuân thủ GDPR/CCPA)")
async def delete_my_data(interaction: discord.Interaction):
    user_id = interaction.user.id
    cleared = False
    
    try:
        if user_id in user_collections:
            del user_collections[user_id]
            cleared = True
        
        global user_consent_ledger
        if 'user_consent_ledger' in globals() and user_id in user_consent_ledger:
            user_consent_ledger.remove(user_id)
            cleared = True
    except Exception as e:
        logger.error(f"[GDPR Error] Lỗi khi xóa dữ liệu người dùng {user_id}: {e}")

    if cleared:
        await interaction.response.send_message("🛡️ Toàn bộ dữ liệu cá nhân của bạn đã được xóa hoàn toàn khỏi hệ thống lưu trữ.", ephemeral=True)
    else:
        await interaction.response.send_message("📭 Không tìm thấy dữ liệu cá nhân nào được lưu trữ.", ephemeral=True)

@bot.tree.command(name="help", description="🚀 Hướng dẫn hệ thống Workstation Bot (Production)")
async def help_cmd(interaction: discord.Interaction):
    embed = discord.Embed(
        title="✧ ── ✦ HỆ THỐNG WORKSTATION PRODUCTION BOT ✦ ── ✧",
        color=discord.Color.from_rgb(88, 24, 131)
    )
    embed.add_field(name="🔮 `/play [tên/link]`", value="Phát nhạc tối ưu băng thông & đệm tự động.", inline=False)
    embed.add_field(name="🖤 `/myfavorite`", value="Nạp siêu tốc tức thì từ bộ nhớ đệm cá nhân.", inline=False)
    embed.add_field(name="📜 `/queue`", value="Xem danh sách chờ hiện tại.", inline=False)
    embed.add_field(name="🗑️ `/remove`", value="Xóa bài hát khỏi hàng đợi an toàn.", inline=False)
    embed.add_field(name="🧬 `/duplicate`", value="Nhân bản bài hát thông minh.", inline=False)
    embed.add_field(name="🔄 `/move`", value="Đổi vị trí bài hát trong hàng đợi.", inline=False)
    embed.add_field(name="💼 `/collection`", value="Quản lý kho cá nhân (Tối đa 10 bài).", inline=False)
    embed.add_field(name="🔊 `/volume`", value="Điều chỉnh âm lượng hệ thống.", inline=False)
    embed.add_field(name="🌙 `/sleep`", value="Hẹn giờ ngắt bot tự động.", inline=False)
    embed.add_field(name="🛡️ `/delete-my-data`", value="Xóa dữ liệu cá nhân (GDPR/CCPA).", inline=False)
    await interaction.response.send_message(embed=embed, ephemeral=True)

# Khởi chạy an toàn với kiểm tra Token chuẩn Enterprise
if __name__ == "__main__":
    TOKEN = os.getenv("DISCORD_TOKEN")
    if not TOKEN:
        logger.critical("🚨 LỖI NGHIÊM TRỌNG: Không tìm thấy biến môi trường DISCORD_TOKEN!")
    else:
        bot.run(TOKEN)
