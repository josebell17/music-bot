import os
import asyncio
import logging
import discord
from discord.ext import commands
import yt_dlp
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

# Cấu hình logging chuyên nghiệp
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] [%(name)s] %(message)s')
logger = logging.getLogger("WorkstationBot")

intents = discord.Intents.default()
intents.message_content = True
intents.voice_states = True

bot = commands.Bot(command_prefix="!", intents=intents)

# Đa luồng tối ưu hóa cho tác vụ bóc tách mạng
WORKSTATION_POOL = ThreadPoolExecutor(max_workers=6, thread_name_prefix="Workstation_Worker")

# Bộ nhớ đệm và hệ thống quản lý trạng thái
URL_CACHE = {}          
FAILED_TRACKS_LEDGER = defaultdict(int) # Sổ cái tự học: Theo dõi các từ khóa/link lỗi để tinh chỉnh chiến lược fetch
queues = defaultdict(list)
volumes = defaultdict(lambda: 0.5)
sleep_tasks = {}
user_collections = defaultdict(list)
guild_locks = defaultdict(asyncio.Lock) # Khóa đồng bộ độc quyền theo từng Guild

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
    'before_options': '-reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5 -nostdin',
    'options': '-vn',
}

ytdl = yt_dlp.YoutubeDL(YTDL_OPTIONS)

class SelfHealingEngine:
    """Mô đun đồng bộ chuyên nghiệp xử lý tự động sửa lỗi và tinh chỉnh luồng"""
    
    @staticmethod
    async def resolve_stream(search: str, loop, cached_stream_url=None):
        stream_url = cached_stream_url
        title = search
        force_fresh = False

        # Nếu bài hát từng bị ghi nhận lỗi trong sổ cái, ép buộc bỏ qua cache để tạo mới
        if search in FAILED_TRACKS_LEDGER and FAILED_TRACKS_LEDGER[search] > 2:
            force_fresh = True
            logger.warning(f"[Self-Healing] Từ khóa '{search}' có lịch sử lỗi cao. Buộc làm mới đường dẫn tuyệt đối.")

        if not stream_url or force_fresh:
            if search in URL_CACHE and not force_fresh:
                stream_url, title = URL_CACHE[search]
            else:
                query = search if search.startswith("http") else f"scsearch:{search}"
                
                def extract_worker():
                    try:
                        data = ytdl.extract_info(query, download=False)
                        if 'entries' in data and data['entries']:
                            data = data['entries'][0]
                        return data
                    except Exception as ex:
                        logger.error(f"[Extraction Error] Không thể bóc tách {query}: {ex}")
                        return None

                data = await loop.run_in_executor(WORKSTATION_POOL, extract_worker)
                
                if data and 'url' in data:
                    stream_url = data['url']
                    title = data.get('title', search)
                    URL_CACHE[search] = (stream_url, title)
                    if search in FAILED_TRACKS_LEDGER:
                        FAILED_TRACKS_LEDGER[search] = max(0, FAILED_TRACKS_LEDGER[search] - 1)
                else:
                    FAILED_TRACKS_LEDGER[search] += 1
                    raise Exception(f"Mô đun tự học không thể trích xuất luồng hợp lệ cho: {search}")

        return stream_url, title

    @staticmethod
    def purge_cache_key(search: str):
        if search in URL_CACHE:
            del URL_CACHE[search]
            logger.info(f"[Self-Healing] Đã tự động làm sạch Cache cho từ khóa lỗi: {search}")


class YTDLSource(discord.PCMVolumeTransformer):
    def __init__(self, source, *, data, volume=0.5):
        super().__init__(source, volume)
        self.data = data
        self.title = data.get('title', 'Unknown Title')
        self.url = data.get('url', '')
        self.stream_url = data.get('url', '')

    @classmethod
    async def create_source(cls, search: str, *, loop=None, volume=0.5, cached_stream_url=None):
        loop = loop or asyncio.get_event_loop()
        
        try:
            stream_url, title = await SelfHealingEngine.resolve_stream(search, loop, cached_stream_url)
            audio_source = discord.FFmpegPCMAudio(stream_url, **FFMPEG_OPTIONS)
            return cls(audio_source, data={'title': title, 'url': stream_url}, volume=volume)
        except Exception as e:
            # Tự động sửa lỗi tầng FFmpeg / Token hết hạn
            SelfHealingEngine.purge_cache_key(search)
            # Thử lại một lần cuối với luồng mới hoàn toàn
            try:
                stream_url, title = await SelfHealingEngine.resolve_stream(search, loop, None)
                audio_source = discord.FFmpegPCMAudio(stream_url, **FFMPEG_OPTIONS)
                return cls(audio_source, data={'title': title, 'url': stream_url}, volume=volume)
            except Exception as inner_e:
                raise Exception(f"Lỗi khởi tạo nguồn phát: {inner_e}")


async def play_next(ctx):
    guild_id = ctx.guild.id
    async with guild_locks[guild_id]:
        if len(queues[guild_id]) > 0:
            player = queues[guild_id].pop(0)
            player.volume = volumes[guild_id]
            ctx.current_player = player
            
            def after_playing(error):
                if error:
                    logger.error(f"[Playback Error] Lỗi luồng phần cứng/mạng: {error}")
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

        guild_id = self.ctx.guild.id
        async with guild_locks[guild_id]:
            if self.ctx.voice_client and self.ctx.voice_client.is_playing():
                self.ctx.voice_client.pause()
                await interaction.followup.send("💤 Đã tạm dừng phát nhạc.", ephemeral=True)
            else:
                await interaction.followup.send("⚠️ Không có nhạc đang phát hoặc trạng thái không hợp lệ.", ephemeral=True)

    @discord.ui.button(style=discord.ButtonStyle.secondary, emoji="▶️")
    async def resume(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True)

        guild_id = self.ctx.guild.id
        async with guild_locks[guild_id]:
            if self.ctx.voice_client and self.ctx.voice_client.is_paused():
                self.ctx.voice_client.resume()
                await interaction.followup.send("🔮 Đã tiếp tục phát nhạc thành công.", ephemeral=True)
            else:
                await interaction.followup.send("⚠️ Nhạc không ở trạng thái tạm dừng.", ephemeral=True)

    @discord.ui.button(style=discord.ButtonStyle.secondary, emoji="⏭️")
    async def skip(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True)

        guild_id = self.ctx.guild.id
        async with guild_locks[guild_id]:
            if self.ctx.voice_client and (self.ctx.voice_client.is_playing() or self.ctx.voice_client.is_paused()):
                self.ctx.voice_client.stop()
                await interaction.followup.send("📿 Đã chuyển bài an toàn.", ephemeral=True)
            else:
                await interaction.followup.send("⚠️ Hàng đợi trống hoặc không có nhạc hoạt động.", ephemeral=True)

    @discord.ui.button(style=discord.ButtonStyle.secondary, emoji="🖤")
    async def fast_save(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True)

        user_id = interaction.user.id
        current = getattr(self.ctx, 'current_player', None)
        if not current:
            return await interaction.followup.send("⚠️ Không có bài hát nào đang hoạt động để lưu.", ephemeral=True)
        
        favs = user_collections[user_id]
        if len(favs) >= 10:
            return await interaction.followup.send("⚠️ Kho lưu trữ cá nhân đã đạt giới hạn tối đa 10 bài!", ephemeral=True)
            
        if not any(song['title'] == current.title for song in favs):
            favs.append({'title': current.title, 'stream_url': current.stream_url})
            await interaction.followup.send(f"✨ Đã lưu vào kho cá nhân: **{current.title}** (`{len(favs)}/10`)", ephemeral=True)
        else:
            await interaction.followup.send("💠 Bài hát đã có sẵn trong bộ sưu tập của bạn.", ephemeral=True)

    @discord.ui.button(style=discord.ButtonStyle.danger, emoji="⏹️")
    async def stop(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True)

        guild_id = self.ctx.guild.id
        async with guild_locks[guild_id]:
            queues[guild_id].clear()
            if self.ctx.voice_client:
                await self.ctx.voice_client.disconnect()
                await interaction.followup.send("🌌 Đã ngắt kết nối và xóa sạch hàng đợi an toàn.", ephemeral=True)


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
        logger.info(f"Bot đã sẵn sàng đăng nhập dưới tên: {bot.user}")
    except Exception as e:
        logger.error(f"Lỗi đồng bộ slash command: {e}")


@bot.tree.command(name="play", description="🔮 Phát nhạc trực tiếp với hệ thống Khóa luồng & Tự học thông minh")
@discord.app_commands.describe(search="Tên bài hát hoặc link trực tiếp")
async def play(interaction: discord.Interaction, search: str):
    if not interaction.user.voice:
        return await interaction.response.send_message("🥀 Vui lòng vào kênh thoại trước.", ephemeral=True)

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
        player = await YTDLSource.create_source(search, loop=bot.loop, volume=current_vol)
        ctx.current_player = player

        async with guild_locks[guild_id]:
            if ctx.voice_client.is_playing() or ctx.voice_client.is_paused():
                queues[guild_id].append(player)
                embed = discord.Embed(title="💠 Đã thêm vào hàng đợi", description=f"**{player.title}** (`#{len(queues[guild_id])}/10`)", color=discord.Color.from_rgb(45, 10, 75))
                await interaction.followup.send(embed=embed)
            else:
                ctx.voice_client.play(player, after=lambda e: asyncio.run_coroutine_threadsafe(play_next(ctx), bot.loop))
                embed = discord.Embed(title="✧ ── ✦ 𝕸𝖚𝖘𝖎𝖈 𝖑𝖆𝖓𝖌𝖓𝖌𝖔 ✦ ── ✧", description=f"🔮 **Đang Phát:** \n**{player.title}**", color=discord.Color.from_rgb(88, 24, 131))
                await interaction.followup.send(embed=embed, view=MusicControlView(ctx))
    except Exception as e:
        await interaction.followup.send(f"⚠️ Lỗi tải dữ liệu: {e}")


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
        return await interaction.response.send_message("⚠️ Hàng đợi đã đầy! Không thể nạp thêm từ kho.", ephemeral=True)

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
            await asyncio.sleep(0.02)
        except Exception:
            continue

    if first_player:
        ctx.voice_client.play(first_player, after=lambda e: asyncio.run_coroutine_threadsafe(play_next(ctx), bot.loop))
        embed = discord.Embed(title="🖤 Đang Phát Kho Lưu Trữ", description=f"Đã nạp siêu tốc **{added} bài** từ bộ sưu tập vào hệ thống.", color=discord.Color.from_rgb(88, 24, 131))
        await interaction.followup.send(embed=embed, view=MusicControlView(ctx))
    elif added > 0:
        await interaction.followup.send(f"🖤 Đã nạp thêm **{added} bài** từ kho vào hàng đợi.")
    else:
        await interaction.followup.send("⚠️ Không thể khởi tạo danh sách từ kho cá nhân.", ephemeral=True)


@bot.tree.command(name="queue", description="📜 Xem danh sách chờ (Tối đa 10 bài)")
async def queue_cmd(interaction: discord.Interaction):
    guild_id = interaction.guild.id
    if guild_id not in queues or len(queues[guild_id]) == 0:
        return await interaction.response.send_message("📭 Hàng đợi trống.", ephemeral=True)
    
    q_list = "\n".join([f"` ⟡ {i+1}. ` {song.title}" for i, song in enumerate(queues[guild_id][:10])])
    embed = discord.Embed(title=f"⚡ Danh Sách Chờ ({len(queues[guild_id])}/10)", description=q_list, color=discord.Color.from_rgb(60, 15, 100))
    await interaction.response.send_message(embed=embed)


@bot.tree.command(name="remove", description="🗑️ Xóa bài hát khỏi hàng đợi")
@discord.app_commands.describe(index="Vị trí bắt đầu xóa", count="Số lượng bài muốn xóa (mặc định 1)")
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


@bot.tree.command(name="volume", description="🔊 Chỉnh âm lượng hệ thống (1 - 100)")
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


@bot.tree.command(name="help", description="🚀 Hướng dẫn hệ thống Workstation Bot (Self-Healing)")
async def help_cmd(interaction: discord.Interaction):
    embed = discord.Embed(
        title="✧ ── ✦ HỆ THỐNG WORKSTATION MUSIC BOT (LOCKED & SYNCED) ✦ ── ✧",
        color=discord.Color.from_rgb(88, 24, 131)
    )
    embed.add_field(name="🔮 `/play [tên/link]`", value="Phát nhạc với cơ chế khóa luồng độc quyền.", inline=False)
    embed.add_field(name="🖤 `/myfavorite`", value="Tải siêu tốc từ kho cá nhân có cơ chế dự phòng tự học.", inline=False)
    embed.add_field(name="📜 `/queue`", value="Xem danh sách chờ hiện tại.", inline=False)
    embed.add_field(name="🗑️ `/remove [vị trí] [số lượng]`", value="Xóa bài hát khỏi hàng đợi an toàn.", inline=False)
    embed.add_field(name="💼 `/collection`", value="Quản lý kho cá nhân (Tối đa 10 bài).", inline=False)
    embed.add_field(name="🔊 `/volume [1-100]`", value="Điều chỉnh âm lượng.", inline=False)
    embed.add_field(name="🌙 `/sleep [phút]`", value="Hẹn giờ ngắt bot tự động.", inline=False)
    await interaction.response.send_message(embed=embed, ephemeral=True)


TOKEN = os.getenv("DISCORD_TOKEN")
if TOKEN:
    bot.run(TOKEN)
