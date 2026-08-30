import os
import asyncio
import logging
import discord
from discord.ext import commands
import yt_dlp
from aiohttp import web
from collections import defaultdict

# Cấu hình logging chuyên nghiệp
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

intents = discord.Intents.default()
intents.message_content = True
intents.voice_states = True

bot = commands.Bot(command_prefix="!", intents=intents)

# Quản lý trạng thái hệ thống
queues = defaultdict(list)
volumes = defaultdict(lambda: 0.5)
sleep_tasks = {}
user_collections = defaultdict(list)

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
    'options': '-vn -b:a 192k',
}

ytdl = yt_dlp.YoutubeDL(YTDL_OPTIONS)

class YTDLSource(discord.PCMVolumeTransformer):
    def __init__(self, source, *, data, volume=0.5):
        super().__init__(source, volume)
        self.data = data
        self.title = data.get('title', 'Unknown Title')
        self.url = data.get('url', '')
        self.webpage_url = data.get('webpage_url', '')

    @classmethod
    async def create_source(cls, search: str, *, loop=None, volume=0.5):
        loop = loop or asyncio.get_event_loop()
        query = search if search.startswith("http") else f"scsearch:{search}"
        data = await loop.run_in_executor(None, lambda: ytdl.extract_info(query, download=False))
        
        if 'entries' in data and data['entries']:
            data = data['entries'][0]
        else:
            raise Exception("Không tìm thấy kết quả phù hợp trên SoundCloud!")

        audio_source = discord.FFmpegPCMAudio(data['url'], **FFMPEG_OPTIONS)
        return cls(audio_source, data=data, volume=volume)

# Autocomplete chuẩn chỉnh giống Google Chrome / SoundCloud: Trả về danh sách bài hát thực tế để bấm chọn
async def search_soundcloud(current: str):
    if not current or len(current.strip()) < 2:
        return [discord.app_commands.Choice(name="🔥 Gợi ý: Gõ tên bài hát hoặc nghệ sĩ...", value="Lofi Chill")]
    try:
        loop = asyncio.get_event_loop()
        # Truy vấn trực tiếp scsearch để lấy danh sách bài hát thật khớp với từ khóa
        info = await asyncio.wait_for(
            loop.run_in_executor(
                None, 
                lambda: ytdl.extract_info(f"scsearch10:{current}", download=False)
            ),
            timeout=3.0
        )
        entries = info.get('entries', []) if info else []
        results = []
        for entry in entries:
            title = entry.get('title')
            webpage_url = entry.get('webpage_url') or entry.get('url')
            if title and webpage_url:
                if len(title) > 100:
                    title = title[:97] + "..."
                # Hiển thị tên bài hát rõ ràng trong menu thả xuống
                results.append(discord.app_commands.Choice(name=title, value=webpage_url))
        
        if not results:
            results.append(discord.app_commands.Choice(name=f"🔍 Tìm kiếm: {current}", value=current))
        return results[:25]
    except Exception:
        return [discord.app_commands.Choice(name=f"🎵 Phát nhanh: {current}", value=current)]

async def play_next(ctx):
    guild_id = ctx.guild.id
    if len(queues[guild_id]) > 0:
        player = queues[guild_id].pop(0)
        player.volume = volumes[guild_id]
        ctx.current_player = player
        
        def after_playing(error):
            if error:
                logging.error(f"Lỗi phát nhạc: {error}")
            asyncio.run_coroutine_threadsafe(play_next(ctx), bot.loop)

        if ctx.voice_client and ctx.voice_client.is_connected():
            ctx.voice_client.play(player, after=after_playing)
            try:
                embed = discord.Embed(title="🎧 Đang phát bài tiếp theo", description=f"**{player.title}**", color=discord.Color.dark_embed())
                asyncio.run_coroutine_threadsafe(ctx.send(embed=embed), bot.loop)
            except Exception:
                pass
    else:
        ctx.current_player = None
        if ctx.voice_client and ctx.voice_client.is_connected():
            await asyncio.sleep(180)
            if not ctx.voice_client.is_playing() and len(queues[guild_id]) == 0:
                await ctx.voice_client.disconnect()

class MusicControlView(discord.ui.View):
    def __init__(self, ctx):
        super().__init__(timeout=None)
        self.ctx = ctx

    @discord.ui.button(label="Tạm dừng", style=discord.ButtonStyle.secondary, emoji="⏸️")
    async def pause(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.ctx.voice_client and self.ctx.voice_client.is_playing():
            self.ctx.voice_client.pause()
            await interaction.response.send_message("⏸️ Đã tạm dừng nhạc.", ephemeral=True)
        else:
            await interaction.response.send_message("❌ Không có nhạc đang phát!", ephemeral=True)

    @discord.ui.button(label="Tiếp tục", style=discord.ButtonStyle.success, emoji="▶️")
    async def resume(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.ctx.voice_client and self.ctx.voice_client.is_paused():
            self.ctx.voice_client.resume()
            await interaction.response.send_message("▶️ Tiếp tục phát nhạc.", ephemeral=True)
        else:
            await interaction.response.send_message("❌ Nhạc không ở trạng thái dừng!", ephemeral=True)

    @discord.ui.button(label="Bỏ qua", style=discord.ButtonStyle.primary, emoji="⏭️")
    async def skip(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.ctx.voice_client and self.ctx.voice_client.is_playing():
            self.ctx.voice_client.stop()
            await interaction.response.send_message("⏭️ Đã chuyển sang bài tiếp theo.", ephemeral=True)
        else:
            await interaction.response.send_message("❌ Không có bài để bỏ qua!", ephemeral=True)

    @discord.ui.button(label="Lưu Yêu Thích", style=discord.ButtonStyle.blurple, emoji="💖")
    async def fast_save(self, interaction: discord.Interaction, button: discord.ui.Button):
        user_id = interaction.user.id
        current = getattr(self.ctx, 'current_player', None)
        if not current:
            return await interaction.response.send_message("❌ Không có bài hát nào đang phát để lưu!", ephemeral=True)
        if current.title not in user_collections[user_id]:
            user_collections[user_id].append(current.title)
            await interaction.response.send_message(f"🔥 Đã thêm **{current.title}** vào bộ sưu tập cá nhân!", ephemeral=True)
        else:
            await interaction.response.send_message("⚠️ Bài này đã có sẵn trong bộ sưu tập của bạn rồi!", ephemeral=True)

    @discord.ui.button(label="Rời phòng", style=discord.ButtonStyle.danger, emoji="⏹️")
    async def stop(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild_id = self.ctx.guild.id
        queues[guild_id].clear()
        if self.ctx.voice_client:
            await self.ctx.voice_client.disconnect()
            await interaction.response.send_message("⏹️ Đã dừng nhạc và rời phòng thoại.", ephemeral=True)

class CollectionView(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=60)
        self.user_id = user_id

    @discord.ui.button(label="Lưu bài đang phát", style=discord.ButtonStyle.success, emoji="📥")
    async def save_current(self, interaction: discord.Interaction, button: discord.ui.Button):
        ctx = await commands.Context.from_interaction(interaction)
        current = getattr(ctx, 'current_player', None)
        if not current:
            return await interaction.response.send_message("❌ Không có bài hát nào đang phát!", ephemeral=True)
        if current.title not in user_collections[self.user_id]:
            user_collections[self.user_id].append(current.title)
            await interaction.response.send_message(f"✨ Đã lưu thành công: **{current.title}** vào bộ sưu tập!", ephemeral=True)
        else:
            await interaction.response.send_message("⚠️ Bài hát này đã có trong bộ sưu tập của bạn!", ephemeral=True)

    @discord.ui.button(label="Xem danh sách", style=discord.ButtonStyle.primary, emoji="📂")
    async def view_list(self, interaction: discord.Interaction, button: discord.ui.Button):
        favs = user_collections[self.user_id]
        if not favs:
            return await interaction.response.send_message("📭 Bộ sưu tập của bạn đang trống trơn!", ephemeral=True)
        fav_list = "\n".join([f"`{i+1}.` {song}" for i, song in enumerate(favs[:15])])
        embed = discord.Embed(title=f"🎯 Bộ Sưu Tập Của {interaction.user.name}", description=fav_list, color=discord.Color.from_rgb(0, 255, 128))
        await interaction.response.send_message(embed=embed, ephemeral=True)

async def handle(request):
    return web.Response(text="Music langngo Bot is active 24/7!")

async def start_web_server():
    app = web.Application()
    app.router.add_get('/', handle)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv("PORT", 8080))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()

@bot.event
async def on_ready():
    bot.loop.create_task(start_web_server())
    try:
        await bot.tree.sync()
        logging.info(f"Đã đồng bộ thành công hệ thống lệnh cho {bot.user}")
    except Exception as e:
        logging.error(f"Lỗi đồng bộ: {e}")

@bot.tree.command(name="play", description="🎮 Tìm kiếm real-time và phát nhạc cực chiến cho dân chơi")
@discord.app_commands.describe(search="Nhập tên bài hát hoặc dán link SoundCloud")
async def play(interaction: discord.Interaction, search: str):
    if not interaction.user.voice:
        return await interaction.response.send_message("❌ Vào kênh thoại đi người anh em!", ephemeral=True)

    await interaction.response.defer()
    ctx = await commands.Context.from_interaction(interaction)
    
    if not ctx.voice_client:
        try:
            await interaction.user.voice.channel.connect()
        except Exception as e:
            return await interaction.followup.send(f"❌ Không thể kết nối kênh thoại: {e}")

    try:
        current_vol = volumes[interaction.guild.id]
        player = await YTDLSource.create_source(search, loop=bot.loop, volume=current_vol)
        guild_id = interaction.guild.id
        ctx.current_player = player

        if ctx.voice_client.is_playing() or ctx.voice_client.is_paused():
            queues[guild_id].append(player)
            embed = discord.Embed(title="🔥 Đã xếp vào hàng chờ", description=f"**{player.title}**", color=discord.Color.dark_orange())
            embed.add_field(name="Vị trí", value=str(len(queues[guild_id])), inline=True)
            await interaction.followup.send(embed=embed)
        else:
            ctx.voice_client.play(player, after=lambda e: asyncio.run_coroutine_threadsafe(play_next(ctx), bot.loop))
            embed = discord.Embed(title="🎧 Music langngo - Đang Phát", description=f"**{player.title}**", color=discord.Color.teal())
            embed.set_footer(text=f"Yêu cầu bởi {interaction.user.name}", icon_url=interaction.user.display_avatar.url)
            await interaction.followup.send(embed=embed, view=MusicControlView(ctx))
            
    except Exception as e:
        await interaction.followup.send(f"❌ Không load được bài này: {e}")

@play.autocomplete("search")
async def play_autocomplete(interaction: discord.Interaction, current: str):
    return await search_soundcloud(current)

@bot.tree.command(name="queue", description="📜 Kiểm tra danh sách bài đang chờ phát")
async def queue(interaction: discord.Interaction):
    guild_id = interaction.guild.id
    if guild_id not in queues or len(queues[guild_id]) == 0:
        return await interaction.response.send_message("📭 Hàng đợi trống trơn, lên nòng bài mới thôi!", ephemeral=True)
    
    q_list = "\n".join([f"`{i+1}.` {song.title}" for i, song in enumerate(queues[guild_id][:15])])
    embed = discord.Embed(title="⚡ Danh Sách Chờ (Queue)", description=q_list, color=discord.Color.gold())
    embed.set_footer(text=f"Tổng số bài: {len(queues[guild_id])}")
    await interaction.response.send_message(embed=embed)

@bot.tree.command(name="remove", description="🗑️ Xóa một bài cụ thể trong hàng đợi")
@discord.app_commands.describe(index="Số thứ tự bài hát trong /queue")
async def remove(interaction: discord.Interaction, index: int):
    guild_id = interaction.guild.id
    if guild_id not in queues or len(queues[guild_id]) == 0:
        return await interaction.response.send_message("❌ Hàng đợi đang trống!", ephemeral=True)
    
    if 1 <= index <= len(queues[guild_id]):
        removed = queues[guild_id].pop(index - 1)
        await interaction.response.send_message(f"🗑️ Đã bay màu bài: **{removed.title}**")
    else:
        await interaction.response.send_message("❌ Số thứ tự không hợp lệ!", ephemeral=True)

@bot.tree.command(name="move", description="🔄 Đổi vị trí bài hát trong hàng đợi")
@discord.app_commands.describe(from_pos="Vị trí cũ", to_pos="Vị trí mới")
async def move(interaction: discord.Interaction, from_pos: int, to_pos: int):
    guild_id = interaction.guild.id
    q = queues[guild_id]
    if not q or not (1 <= from_pos <= len(q)) or not (1 <= to_pos <= len(q)):
        return await interaction.response.send_message("❌ Vị trí sai rồi bro!", ephemeral=True)
    
    song = q.pop(from_pos - 1)
    q.insert(to_pos - 1, song)
    await interaction.response.send_message(f"🔄 Đã bốc bài **{song.title}** từ vị trí `{from_pos}` sang `{to_pos}`.")

@bot.tree.command(name="collection", description="🎯 Mở bảng quản lý bộ sưu tập nhạc yêu thích cá nhân")
async def collection(interaction: discord.Interaction):
    embed = discord.Embed(
        title="💼 Bộ Sưu Tập Cá Nhân - Music langngo",
        description="Bấm vào nút bên dưới để lưu nhanh bài đang phát hoặc mở danh sách yêu thích của bạn!",
        color=discord.Color.purple()
    )
    await interaction.response.send_message(embed=embed, view=CollectionView(interaction.user.id), ephemeral=True)

@bot.tree.command(name="volume", description="🔊 Tăng giảm âm lượng bot (1 - 100)")
@discord.app_commands.describe(level="Mức âm lượng")
async def volume(interaction: discord.Interaction, level: int):
    if not (1 <= level <= 100):
        return await interaction.response.send_message("❌ Chọn mức từ 1 đến 100 nhé!", ephemeral=True)
    
    volumes[interaction.guild.id] = level / 100.0
    if interaction.guild.voice_client and interaction.guild.voice_client.source:
        interaction.guild.voice_client.source.volume = level / 100.0
    await interaction.response.send_message(f"🔊 Đã kéo âm lượng lên mức: **{level}%** ⚡")

@bot.tree.command(name="sleep", description="⏰ Hẹn giờ tự động sập nguồn bot sau số phút")
@discord.app_commands.describe(minutes="Số phút")
async def sleep(interaction: discord.Interaction, minutes: int):
    guild_id = interaction.guild.id
    if guild_id in sleep_tasks:
        sleep_tasks[guild_id].cancel()
    
    async def timer():
        await asyncio.sleep(minutes * 60)
        if interaction.guild.voice_client:
            queues[guild_id].clear()
            await interaction.guild.voice_client.disconnect()
            try:
                await interaction.channel.send("🌙 Hết giờ! Music langngo đã tự động cút khỏi phòng để bro nghỉ ngơi.")
            except Exception:
                pass
        sleep_tasks.pop(guild_id, None)

    sleep_tasks[guild_id] = bot.loop.create_task(timer())
    await interaction.response.send_message(f"⏰ Đã hẹn giờ tắt nhạc sau **{minutes} phút**.")

@bot.tree.command(name="help", description="🚀 Hiển thị bảng điều khiển & hướng dẫn hệ thống Music langngo")
async def help_cmd(interaction: discord.Interaction):
    embed = discord.Embed(
        title="🔥 Music langngo - Hệ Thống Âm Nhạc Cực Chiến ⚡",
        description="Bot âm nhạc chất lượng cao chạy nền tảng 24/7 dành riêng cho dân chơi.",
        color=discord.Color.dark_red()
    )
    embed.add_field(name="🎮 `/play [tên bài]`", value="Tìm kiếm real-time mượt mà và phát nhạc cực đỉnh.", inline=False)
    embed.add_field(name="📜 `/queue`", value="Check ngay danh sách bài hát đang chờ.", inline=False)
    embed.add_field(name="🗑️ `/remove [STT]`", value="Xóa bài cụ thể khỏi hàng đợi.", inline=False)
    embed.add_field(name="🔄 `/move [cũ] [mới]`", value="Đổi chỗ vị trí các bài trong hàng.", inline=False)
    embed.add_field(name="🎯 `/collection`", value="Mở bảng quản lý và lưu bài yêu thích bằng một cú click.", inline=False)
    embed.add_field(name="🔊 `/volume [1-100]`", value="Chỉnh to nhỏ âm thanh tùy ý.", inline=False)
    embed.add_field(name="⏰ `/sleep [phút]`", value="Hẹn giờ tự động tắt nhạc đi ngủ.", inline=False)
    embed.set_footer(text="⚡ Coded for Ultimate Gaming Experience | langngo", icon_url=interaction.user.display_avatar.url)
    await interaction.response.send_message(embed=embed)

TOKEN = os.getenv("DISCORD_TOKEN")
if TOKEN:
    bot.run(TOKEN)
