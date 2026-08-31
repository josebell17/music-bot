import os
import asyncio
import logging
import discord
from discord.ext import commands
import yt_dlp
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

# Cấu hình ghi log chuyên nghiệp tiêu chuẩn hệ thống
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] [Workstation-Core] %(message)s')
logger = logging.getLogger("WorkstationProductionBot")

intents = discord.Intents.default()
intents.message_content = True
intents.voice_states = True

bot = commands.Bot(command_prefix="!", intents=intents)

# Đa luồng tối ưu hóa cho tác vụ bóc tách mạng
WORKSTATION_POOL = ThreadPoolExecutor(max_workers=8, thread_name_prefix="Production_Worker")

# Bộ nhớ đệm thông minh & Pre-fetch Cache
URL_CACHE = {}          
FAILED_TRACKS_LEDGER = defaultdict(int) 
queues = defaultdict(list)
volumes = defaultdict(lambda: 0.5)
sleep_tasks = {}
user_collections = defaultdict(list)
user_consent_ledger = set() 
guild_locks = defaultdict(asyncio.Lock) 

YTDL_OPTIONS = {
    'default_search': 'scsearch',
    'format': 'bestaudio/best',
    'extractaudio': True,
    'audioformat': 'mp3',
    'noplaylist': True,
    'nocheckcertificate': True,
    'quiet': True,
    'no_warnings': True,
}

FFMPEG_OPTIONS = {
    'before_options': '-reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5 -nostdin -analyzeduration 0 -loglevel 0',
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
    """Mô đun tự học kết hợp Pre-fetch Cache tối ưu tốc độ"""
    @staticmethod
    async def resolve_stream(search: str, loop, cached_stream_url=None):
        safe_search = SecuritySanitizer.sanitize_input(search)
        stream_url = cached_stream_url
        title = safe_search
        force_fresh = False

        if safe_search in FAILED_TRACKS_LEDGER and FAILED_TRACKS_LEDGER[safe_search] > 2:
            force_fresh = True
            logger.warning(f"[Self-Healing] Từ khóa '{safe_search}' vượt ngưỡng lỗi. Làm mới luồng.")

        if not stream_url or force_fresh:
            if safe_search in URL_CACHE and not force_fresh:
                stream_url, title = URL_CACHE[safe_search]
            else:
                query = safe_search if safe_search.startswith("http") else f"scsearch:{safe_search}"
                
                def extract_worker():
                    try:
                        data = ytdl.extract_info(query, download=False)
                        if 'entries' in data and data['entries']:
                            data = data['entries'][0]
                        return data
                    except Exception as ex:
                        logger.error(f"[Extraction Error] Lỗi bóc tách: {ex}")
                        return None

                data = await loop.run_in_executor(WORKSTATION_POOL, extract_worker)
                
                if data and 'url' in data:
                    stream_url = data['url']
                    title = data.get('title', safe_search)
                    URL_CACHE[safe_search] = (stream_url, title)
                    if safe_search in FAILED_TRACKS_LEDGER:
                        FAILED_TRACKS_LEDGER[safe_search] = max(0, FAILED_TRACKS_LEDGER[safe_search] - 1)
                else:
                    FAILED_TRACKS_LEDGER[safe_search] += 1
                    raise Exception("Không thể trích xuất luồng phát âm thanh hợp lệ từ nguồn.")

        return stream_url, title

    @staticmethod
    async def background_prefetch(search: str, loop):
        """Tiến trình ngầm đệm dữ liệu trước giúp các thao tác sau siêu mượt"""
        try:
            safe_search = SecuritySanitizer.sanitize_input(search)
            if safe_search and safe_search not in URL_CACHE:
                query = safe_search if safe_search.startswith("http") else f"scsearch:{safe_search}"
                def extract_worker():
                    try:
                        data = ytdl.extract_info(query, download=False)
                        if 'entries' in data and data['entries']:
                            data = data['entries'][0]
                        return data
                    except:
                        return None
                data = await loop.run_in_executor(WORKSTATION_POOL, extract_worker)
                if data and 'url' in data:
                    URL_CACHE[safe_search] = (data['url'], data.get('title', safe_search))
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
        loop = loop or asyncio.get_event_loop()
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
            ctx.current_player = player
            
            # Kích hoạt pre-fetch ngầm cho bài tiếp theo trong hàng đợi nếu có
            if len(queues[guild_id]) > 0:
                next_song = queues[guild_id][0]
                bot.loop.create_task(SelfHealingEngine.background_prefetch(next_song.title, bot.loop))

            def after_playing(error):
                if error:
                    logger.error(f"[Playback Error] Sự cố phần cứng/mạng: {error}")
                asyncio.run_coroutine_threadsafe(play_next(ctx), bot.loop)

            if ctx.voice_client and ctx.voice_client.is_connected():
                try:
                    ctx.voice_client.play(player, after=after_playing)
                    embed = discord.Embed(
                        title="✧ ── ✦ 𝕸𝖚𝖘𝖎𝖈 𝖑𝖆𝖓𝖌𝖓𝖌𝖔 ✦ ── ✧", 
                        description=f"🔮 **Đang Phát:** \n**{player.title}**", 
                        color=discord.Color.from_rgb(88, 24, 131)
                    )
                    asyncio.run_coroutine_threadsafe(ctx.send(embed=embed, view=MusicControlView(ctx)), bot.loop)
                except Exception:
                    asyncio.run_coroutine_threadsafe(play_next(ctx), bot.loop)
        else:
            ctx.current_player = None
            if ctx.voice_client and ctx.voice_client.is_connected():
                await asyncio.sleep(60)
                if not ctx.voice_client.is_playing() and len(queues[guild_id]) == 0:
                    await ctx.voice_client.disconnect()


class MusicControlView(discord.ui.View):
    def __init__(self, ctx):
        super().__init__(timeout=None)
        self.ctx = ctx

    @discord.ui.button(style=discord.ButtonStyle.secondary, emoji="⏸️")
    async def pause(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True)

        async with guild_locks[self.ctx.guild.id]:
            if self.ctx.voice_client and self.ctx.voice_client.is_playing():
                self.ctx.voice_client.pause()
                await interaction.followup.send("💤 Đã tạm dừng phát nhạc.", ephemeral=True)
            else:
                await interaction.followup.send("⚠️ Không có nhạc đang phát.", ephemeral=True)

    @discord.ui.button(style=discord.ButtonStyle.secondary, emoji="▶️")
    async def resume(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True)

        async with guild_locks[self.ctx.guild.id]:
            if self.ctx.voice_client and self.ctx.voice_client.is_paused():
                self.ctx.voice_client.resume()
                await interaction.followup.send("🔮 Đã tiếp tục phát nhạc.", ephemeral=True)
            else:
                await interaction.followup.send("⚠️ Nhạc không ở trạng thái tạm dừng.", ephemeral=True)

    @discord.ui.button(style=discord.ButtonStyle.secondary, emoji="⏭️")
    async def skip(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True)

        async with guild_locks[self.ctx.guild.id]:
            if self.ctx.voice_client and (self.ctx.voice_client.is_playing() or self.ctx.voice_client.is_paused()):
                self.ctx.voice_client.stop()
                await interaction.followup.send("📿 Đã chuyển bài an toàn.", ephemeral=True)
            else:
                await interaction.followup.send("⚠️ Hàng đợi trống.", ephemeral=True)

    @discord.ui.button(style=discord.ButtonStyle.secondary, emoji="🖤")
    async def fast_save(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True)

        user_id = interaction.user.id
        current = getattr(self.ctx, 'current_player', None)
        if not current:
            return await interaction.followup.send("⚠️ Không có bài hát nào đang hoạt động.", ephemeral=True)
        
        favs = user_collections[user_id]
        if len(favs) >= 10:
            return await interaction.followup.send("⚠️ Kho lưu trữ cá nhân đã đạt giới hạn tối đa 10 bài!", ephemeral=True)
            
        if not any(song['title'] == current.title for song in favs):
            favs.append({'title': current.title, 'stream_url': current.stream_url})
            # Tự động pre-fetch ngay khi lưu vào kho để lần sau bấm nút nạp siêu tốc không có độ trễ
            bot.loop.create_task(SelfHealingEngine.background_prefetch(current.title, bot.loop))
            await interaction.followup.send(f"✨ Đã lưu vào kho và đệm sẵn dữ liệu: **{current.title}** (`{len(favs)}/10`)", ephemeral=True)
        else:
            await interaction.followup.send("💠 Bài hát đã có trong bộ sưu tập.", ephemeral=True)

    @discord.ui.button(style=discord.ButtonStyle.danger, emoji="⏹️")
    async def stop(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True)

        async with guild_locks[self.ctx.guild.id]:
            queues[self.ctx.guild.id].clear()
            if self.ctx.voice_client:
                await self.ctx.voice_client.disconnect()
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
        ctx = await commands.Context.from_interaction(interaction)
        current = getattr(ctx, 'current_player', None)
        if not current:
            return await interaction.response.send_message("⚠️ Không có bài hát đang phát!", ephemeral=True)
        
        favs = user_collections[self.user_id]
        if len(favs) >= 10:
            return await interaction.response.send_message("⚠️ Kho lưu trữ cá nhân đã đạt giới hạn tối đa 10 bài!", ephemeral=True)

        if not any(song['title'] == current.title for song in favs):
            favs.append({'title': current.title, 'stream_url': current.stream_url})
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
        await bot.tree.sync()
        logger.info(f"Hệ thống Production Bot đã trực tuyến: {bot.user}")
    except Exception as e:
        logger.error(f"Lỗi đồng bộ slash command: {e}")


@bot.tree.command(name="play", description="🔮 Phát nhạc tối ưu hóa mạng & bảo mật chuẩn doanh nghiệp")
@discord.app_commands.describe(search="Tên bài hát hoặc liên kết")
async def play(interaction: discord.Interaction, search: str):
    if not interaction.user.voice:
        return await interaction.response.send_message("🥀 Vui lòng vào kênh thoại trước.", ephemeral=True)

    safe_query = SecuritySanitizer.sanitize_input(search)
    if not safe_query:
        return await interaction.response.send_message("⚠️ Từ khóa không hợp lệ hoặc chứa ký tự bị chặn.", ephemeral=True)

    guild_id = interaction.guild.id
    if len(queues[guild_id]) >= 10:
        return await interaction.response.send_message("⚠️ Hàng đợi đã đạt giới hạn tối đa 10 bài!", ephemeral=True)

    await interaction.response.defer()
    ctx = await commands.Context.from_interaction(interaction)
    
    if not ctx.voice_client:
        try:
            await interaction.user.voice.channel.connect()
        except Exception as e:
            return await interaction.followup.send(f"⚠️ Lỗi kết nối thoại: {e}")

    try:
        current_vol = volumes[guild_id]
        player = await YTDLSource.create_source(safe_query, loop=bot.loop, volume=current_vol)
        ctx.current_player = player

        async with guild_locks[guild_id]:
            if ctx.voice_client.is_playing() or ctx.voice_client.is_paused():
                queues[guild_id].append(player)
                embed = discord.Embed(title="💠 Đã thêm vào hàng đợi", description=f"**{player.title}** (`#{len(queues[guild_id])}/10`)", color=discord.Color.from_rgb(45, 10, 75))
                await interaction.followup.send(embed=embed)
                # Đệm trước bài tiếp theo trong hàng đợi
                bot.loop.create_task(SelfHealingEngine.background_prefetch(player.title, bot.loop))
            else:
                ctx.voice_client.play(player, after=lambda e: asyncio.run_coroutine_threadsafe(play_next(ctx), bot.loop))
                embed = discord.Embed(title="✧ ── ✦ 𝕸𝖚𝖘𝖎𝖈 𝖑𝖆𝖓𝖌𝖓𝖌𝖔 ✦ ── ✧", description=f"🔮 **Đang Phát:** \n**{player.title}**", color=discord.Color.from_rgb(88, 24, 131))
                await interaction.followup.send(embed=embed, view=MusicControlView(ctx))
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
    ctx = await commands.Context.from_interaction(interaction)
    if not ctx.voice_client:
        try:
            await interaction.user.voice.channel.connect()
        except Exception as e:
            return await interaction.followup.send(f"⚠️ Lỗi kết nối: {e}")

    current_vol = volumes[guild_id]
    added, first_player = 0, None

    for item in favs:
        if len(queues[guild_id]) >= 10:
            break
            
        try:
            # Lấy trực tiếp từ URL Cache đã được pre-fetch sẵn, tốc độ lập tức
            player = await YTDLSource.create_source(
                item['title'], 
                loop=bot.loop, 
                volume=current_vol, 
                cached_stream_url=item.get('stream_url')
            )
            async with guild_locks[guild_id]:
                if not first_player and not ctx.voice_client.is_playing() and not ctx.voice_client.is_paused():
                    first_player = player
                    ctx.current_player = player
                else:
                    queues[guild_id].append(player)
            added += 1
            await asyncio.sleep(0.01)
        except Exception:
            continue

    if first_player:
        ctx.voice_client.play(first_player, after=lambda e: asyncio.run_coroutine_threadsafe(play_next(ctx), bot.loop))
        embed = discord.Embed(title="🖤 Đang Phát Kho Lưu Trữ", description=f"Đã nạp siêu tốc **{added} bài** từ bộ sưu tập.", color=discord.Color.from_rgb(88, 24, 131))
        await interaction.followup.send(embed=embed, view=MusicControlView(ctx))
    elif added > 0:
        await interaction.followup.send(f"🖤 Đã nạp thêm **{added} bài** từ kho vào hàng đợi.")
    else:
        await interaction.followup.send("⚠️ Không thể khởi tạo danh sách từ kho.", ephemeral=True)


@bot.tree.command(name="queue", description="📜 Xem danh sách chờ (Tối đa 10 bài)")
async def queue_cmd(interaction: discord.Interaction):
    guild_id = interaction.guild.id
    if guild_id not in queues or len(queues[guild_id]) == 0:
        return await interaction.response.send_message("📭 Hàng đợi trống.", ephemeral=True)
    
    q_list = "\n".join([f"` ⟡ {i+1}. ` {song.title}" for i, song in enumerate(queues[guild_id][:10])])
    embed = discord.Embed(title=f"⚡ Danh Sách Chờ ({len(queues[guild_id])}/10)", description=q_list, color=discord.Color.from_rgb(60, 15, 100))
    await interaction.response.send_message(embed=embed)


@bot.tree.command(name="remove", description="🗑️ Xóa bài hát khỏi hàng đợi")
@discord.app_commands.describe(index="Vị trí bắt đầu xóa", count="Số lượng bài muốn xóa")
async def remove(interaction: discord.Interaction, index: int, count: int = 1):
    guild_id = interaction.guild.id
    async with guild_locks[guild_id]:
        q = queues[guild_id]
        if not q or not (1 <= index <= len(q)):
            return await interaction.response.send_message("⚠️ Vị trí không hợp lệ.", ephemeral=True)
        
        count = max(1, min(count, len(q) - index + 1))
        removed_items = [q.pop(index - 1) for _ in range(count)]
        titles = ", ".join([item.title for item in removed_items])
        await interaction.response.send_message(f"🗑️ Đã xóa {count} bài từ vị trí `{index}`: **{titles}**")


@bot.tree.command(name="duplicate", description="🧬 Nhân bản bài hát (Tự động chạy tiếp mượt mà)")
@discord.app_commands.describe(index="Nhập 0 cho bài đang phát, hoặc số thứ tự", amount="Số bản sao")
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

    if index == 0:
        if interaction.guild.voice_client and interaction.guild.voice_client.source:
            target_source = interaction.guild.voice_client.source
            target_title = getattr(target_source, 'title', "Bài đang phát")
        else:
            return await interaction.response.send_message("⚠️ Hiện tại không có bài hát nào đang phát.", ephemeral=True)
    else:
        if not q or not (1 <= index <= len(q)):
            return await interaction.response.send_message("⚠️ Số thứ tự trong hàng đợi không hợp lệ.", ephemeral=True)
        target_source = q[index - 1]
        target_title = target_source.title

    await interaction.response.send_message(f"🧬 Đang tiến hành nhân bản **{target_title}** thêm **{amount} lần**...", ephemeral=True)

    was_playing = False
    if interaction.guild.voice_client and interaction.guild.voice_client.is_playing():
        interaction.guild.voice_client.pause()
        was_playing = True
        await asyncio.sleep(0.05)

    try:
        cached_url = getattr(target_source, 'stream_url', None)
        for _ in range(amount):
            duplicated_player = await YTDLSource.create_source(
                target_title, 
                loop=bot.loop, 
                volume=current_vol, 
                cached_stream_url=cached_url
            )
            if index == 0:
                q.insert(0, duplicated_player)
            else:
                q.insert(index, duplicated_player)
                
        await interaction.edit_original_response(content=f"🧬 Đã nhân bản thành công bài **{target_title}** thêm **{amount} lần**!")
    except Exception as e:
        await interaction.edit_original_response(content=f"⚠️ Lỗi khi nhân bản: {e}")
    finally:
        if interaction.guild.voice_client and was_playing:
            await asyncio.sleep(0.05)
            interaction.guild.voice_client.resume()


@bot.tree.command(name="move", description="🔄 Đổi vị trí bài hát trong hàng đợi")
@discord.app_commands.describe(from_pos="Vị trí cũ", to_pos="Vị trí mới")
async def move(interaction: discord.Interaction, from_pos: int, to_pos: int):
    guild_id = interaction.guild.id
    q = queues[guild_id]
    if not q or not (1 <= from_pos <= len(q)) or not (1 <= to_pos <= len(q)):
        return await interaction.response.send_message("⚠️ Vị trí không hợp lệ.", ephemeral=True)
    
    song = q.pop(from_pos - 1)
    q.insert(to_pos - 1, song)
    await interaction.response.send_message(f"🔄 Đã chuyển **{song.title}** từ vị trí `{from_pos}` sang `{to_pos}`.")


@bot.tree.command(name="collection", description="💼 Quản lý kho lưu trữ cá nhân")
async def collection(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    embed = discord.Embed(title="🌌 Kho Lưu Trữ Cá Nhân", description="Sử dụng bảng điều khiển bên dưới:", color=discord.Color.from_rgb(88, 24, 131))
    await interaction.followup.send(embed=embed, view=CollectionView(interaction.user.id), ephemeral=True)


@bot.tree.command(name="volume", description="🔊 Chỉnh âm lượng (1 - 100)")
@discord.app_commands.describe(level="Mức âm lượng")
async def volume(interaction: discord.Interaction, level: int):
    if not (1 <= level <= 100):
        return await interaction.response.send_message("⚠️ Chọn mức âm lượng từ 1 đến 100.", ephemeral=True)
    volumes[interaction.guild.id] = level / 100.0
    if interaction.guild.voice_client and interaction.guild.voice_client.source:
        interaction.guild.voice_client.source.volume = level / 100.0
    await interaction.response.send_message(f"🔊 Đã chỉnh âm lượng hệ thống: **{level}%**")


@bot.tree.command(name="sleep", description="🌙 Hẹn giờ tự ngắt bot")
@discord.app_commands.describe(minutes="Số phút")
async def sleep(interaction: discord.Interaction, minutes: int):
    guild_id = interaction.guild.id
    if guild_id in sleep_tasks:
        sleep_tasks[guild_id].cancel()
    
    async def timer():
        await asyncio.sleep(minutes * 60)
        async with guild_locks[guild_id]:
            if interaction.guild.voice_client:
                queues[guild_id].clear()
                await interaction.guild.voice_client.disconnect()
                try:
                    await interaction.channel.send("🌙 Đã tự động ngắt kết nối do hết thời gian hẹn giờ.")
                except Exception:
                    pass
        sleep_tasks.pop(guild_id, None)

    sleep_tasks[guild_id] = bot.loop.create_task(timer())
    await interaction.response.send_message(f"⏰ Đã đặt hẹn giờ ngắt kết nối sau **{minutes} phút**.")


@bot.tree.command(name="delete-my-data", description="🛡️ Xóa toàn bộ dữ liệu cá nhân (Tuân thủ GDPR/CCPA)")
async def delete_my_data(interaction: discord.Interaction):
    user_id = interaction.user.id
    cleared = False
    if user_id in user_collections:
        del user_collections[user_id]
        cleared = True
    if user_id in user_consent_ledger:
        user_consent_ledger.remove(user_id)
        cleared = True

    if cleared:
        await interaction.response.send_message("🛡️ Toàn bộ dữ liệu cá nhân của bạn đã được xóa hoàn toàn khỏi hệ thống lưu trữ.", ephemeral=True)
    else:
        await interaction.response.send_message("📭 Không tìm thấy dữ liệu cá nhân nào được lưu trữ.", ephemeral=True)


@bot.tree.command(name="help", description="🚀 Hướng dẫn hệ thống Workstation Bot (Production)")
async def help_cmd(interaction: discord.Interaction):
    embed = discord.Embed(
        title="✧ ── ✦ HỆ THỐNG WORKSTATION PRODUCTION BOT (PRE-FETCH CACHE) ✦ ── ✧",
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


TOKEN = os.getenv("DISCORD_TOKEN")
if TOKEN:
    bot.run(TOKEN)
