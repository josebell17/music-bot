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
user_collections = defaultdict(list) # Bộ sưu tập bài hát yêu thích cá nhân (Collection)

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

async def search_soundcloud(current: str):
    if not current or len(current.strip()) == 0:
        return []
    try:
        loop = asyncio.get_event_loop()
        info = await asyncio.wait_for(
            loop.run_in_executor(None, lambda: ytdl.extract_info(f"scsearch5:{current}", download=False)),
            timeout=2.0
        )
        entries = info.get('entries', [])
        results = []
        for entry in entries:
            title = entry.get('title', 'Unknown')
            webpage_url = entry.get('webpage_url') or entry.get('url')
            if len(title) > 100:
                title = title[:97] + "..."
            results.append(discord.app_commands.Choice(name=title, value=webpage_url))
        return results
    except Exception:
        return []

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

class LofiControlView(discord.ui.View):
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

    @discord.ui.button(label="Dừng & Rời phòng", style=discord.ButtonStyle.danger, emoji="⏹️")
    async def stop(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild_id = self.ctx.guild.id
        queues[guild_id].clear()
        if self.ctx.voice_client:
            await self.ctx.voice_client.disconnect()
            await interaction.response.send_message("⏹️ Đã dừng nhạc và rời phòng thoại.", ephemeral=True)

# Web Server duy trì 24/7 trên Render
async def handle(request):
    return web.Response(text="Lofi Radio Bot is active 24/7!")

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

# Lệnh /play
@bot.tree.command(name="play", description="Tìm kiếm real-time và phát nhạc SoundCloud phong cách Lofi Radio")
@discord.app_commands.describe(search="Nhập tên bài hát hoặc dán link SoundCloud")
async def play(interaction: discord.Interaction, search: str):
    if not interaction.user.voice:
        return await interaction.response.send_message("❌ Bạn cần vào kênh thoại trước!", ephemeral=True)

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

        if ctx.voice_client.is_playing() or ctx.voice_client.is_paused():
            queues[guild_id].append(player)
            embed = discord.Embed(title="➕ Đã thêm vào hàng đợi", description=f"**{player.title}**", color=discord.Color.orange())
            embed.add_field(name="Vị trí chờ", value=str(len(queues[guild_id])), inline=True)
            await interaction.followup.send(embed=embed)
        else:
            ctx.current_player = player
            ctx.voice_client.play(player, after=lambda e: asyncio.run_coroutine_threadsafe(play_next(ctx), bot.loop))
            embed = discord.Embed(title="🌙 Đang Phát - Lofi Radio", description=f"**{player.title}**", color=discord.Color.blurple())
            embed.set_footer(text=f"Yêu cầu bởi {interaction.user.name}", icon_url=interaction.user.display_avatar.url)
            await interaction.followup.send(embed=embed, view=LofiControlView(ctx))
            
    except Exception as e:
        await interaction.followup.send(f"❌ Lỗi xử lý bài hát: {e}")

@play.autocomplete("search")
async def play_autocomplete(interaction: discord.Interaction, current: str):
    return await search_soundcloud(current)

# Lệnh quản lý hàng đợi: Xem, Xóa, Đổi vị trí
@bot.tree.command(name="queue", description="Hiển thị danh sách phát hiện tại")
async def queue(interaction: discord.Interaction):
    guild_id = interaction.guild.id
    if guild_id not in queues or len(queues[guild_id]) == 0:
        return await interaction.response.send_message("📭 Hàng đợi hiện đang trống!", ephemeral=True)
    
    q_list = "\n".join([f"`{i+1}.` {song.title}" for i, song in enumerate(queues[guild_id][:15])])
    embed = discord.Embed(title="📜 Danh Sách Phát (Queue)", description=q_list, color=discord.Color.gold())
    embed.set_footer(text=f"Tổng số bài trong hàng: {len(queues[guild_id])}")
    await interaction.response.send_message(embed=embed)

@bot.tree.command(name="remove", description="Xóa một bài hát khỏi hàng đợi theo số thứ tự")
@discord.app_commands.describe(index="Số thứ tự bài hát trong /queue")
async def remove(interaction: discord.Interaction, index: int):
    guild_id = interaction.guild.id
    if guild_id not in queues or len(queues[guild_id]) == 0:
        return await interaction.response.send_message("❌ Hàng đợi trống!", ephemeral=True)
    
    if 1 <= index <= len(queues[guild_id]):
        removed = queues[guild_id].pop(index - 1)
        await interaction.response.send_message(f"🗑️ Đã xóa khỏi hàng đợi: **{removed.title}**")
    else:
        await interaction.response.send_message("❌ Số thứ tự không hợp lệ!", ephemeral=True)

@bot.tree.command(name="move", description="Thay đổi vị trí của một bài hát trong hàng đợi")
@discord.app_commands.describe(from_pos="Vị trí cũ", to_pos="Vị trí mới")
async def move(interaction: discord.Interaction, from_pos: int, to_pos: int):
    guild_id = interaction.guild.id
    q = queues[guild_id]
    if not q or not (1 <= from_pos <= len(q)) or not (1 <= to_pos <= len(q)):
        return await interaction.response.send_message("❌ Vị trí không hợp lệ!", ephemeral=True)
    
    song = q.pop(from_pos - 1)
    q.insert(to_pos - 1, song)
    await interaction.response.send_message(f"🔄 Đã chuyển bài **{song.title}** từ vị trí `{from_pos}` sang `{to_pos}`.")

# Lệnh Bộ sưu tập cá nhân (Collection / Playlist)
@bot.tree.command(name="collection", description="Quản lý bộ sưu tập bài hát yêu thích cá nhân của bạn")
@discord.app_commands.describe(action="Chọn hành động: save (lưu bài hiện tại) hoặc view (xem bộ sưu tập)", title="Tên bài hát nếu lưu thủ công")
@discord.app_commands.choices(action=[
    discord.app_commands.Choice(name="Xem bộ sưu tập (View)", value="view"),
    discord.app_commands.Choice(name="Lưu bài đang phát (Save)", value="save")
])
async def collection(interaction: discord.Interaction, action: str, title: str = None):
    user_id = interaction.user.id
    if action == "save":
        ctx = await commands.Context.from_interaction(interaction)
        current = getattr(ctx, 'current_player', None)
        song_title = current.title if current else title
        if not song_title:
            return await interaction.response.send_message("❌ Không có bài hát nào đang phát để lưu!", ephemeral=True)
        user_collections[user_id].append(song_title)
        await interaction.response.send_message(f"❤️ Đã thêm **{song_title}** vào bộ sưu tập cá nhân của bạn!")
    elif action == "view":
        favs = user_collections[user_id]
        if not favs:
            return await interaction.response.send_message("📭 Bộ sưu tập của bạn đang trống!", ephemeral=True)
        fav_list = "\n".join([f"`{i+1}.` {song}" for i, song in enumerate(favs[:15])])
        embed = discord.Embed(title=f"💖 Bộ Sưu Tập Của {interaction.user.name}", description=fav_list, color=discord.Color.magenta())
        await interaction.response.send_message(embed=embed)

# Lệnh điều chỉnh âm lượng
@bot.tree.command(name="volume", description="Điều chỉnh âm lượng bot (1 - 100)")
@discord.app_commands.describe(level="Mức âm lượng")
async def volume(interaction: discord.Interaction, level: int):
    if not (1 <= level <= 100):
        return await interaction.response.send_message("❌ Vui lòng chọn mức từ 1 đến 100!", ephemeral=True)
    
    volumes[interaction.guild.id] = level / 100.0
    if interaction.guild.voice_client and interaction.guild.voice_client.source:
        interaction.guild.voice_client.source.volume = level / 100.0
    await interaction.response.send_message(f"🔊 Đã đổi âm lượng thành: **{level}%**")

# Lệnh Hẹn giờ đi ngủ (Sleep Timer)
@bot.tree.command(name="sleep", description="Hẹn giờ tự động tắt nhạc và rời phòng sau số phút")
@discord.app_commands.describe(minutes="Số phút muốn hẹn giờ")
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
                await interaction.channel.send("🌙 Đã hết giờ hẹn, Lofi Radio đã tự động tắt để bạn nghỉ ngơi!")
            except Exception:
                pass
        sleep_tasks.pop(guild_id, None)

    sleep_tasks[guild_id] = bot.loop.create_task(timer())
    await interaction.response.send_message(f"⏰ Đã hẹn giờ tắt nhạc sau **{minutes} phút**.")

# Lệnh Help hệ thống
@bot.tree.command(name="help", description="Xem hướng dẫn toàn bộ lệnh hệ thống Lofi Radio")
async def help_cmd(interaction: discord.Interaction):
    embed = discord.Embed(
        title="🌙 Lofi Radio - Hướng Dẫn Hệ Thống",
        description="Bot âm nhạc chất lượng cao chạy nền tảng 24/7.",
        color=discord.Color.dark_purple()
    )
    embed.add_field(name="/play [tên bài]", value="Tìm kiếm real-time và phát nhạc SoundCloud.", inline=False)
    embed.add_field(name="/queue", value="Xem danh sách bài hát đang chờ.", inline=False)
    embed.add_field(name="/remove [STT]", value="Xóa bài hát khỏi hàng đợi.", inline=False)
    embed.add_field(name="/move [cũ] [mới]", value="Đổi vị trí bài hát trong hàng đợi.", inline=False)
    embed.add_field(name="/collection", value="Quản lý và xem bộ sưu tập yêu thích cá nhân.", inline=False)
    embed.add_field(name="/volume [1-100]", value="Chỉnh âm lượng bot.", inline=False)
    embed.add_field(name="/sleep [phút]", value="Hẹn giờ tắt nhạc tự động đi ngủ.", inline=False)
    embed.set_footer(text="Designed for Ultimate Experience")
    await interaction.response.send_message(embed=embed)

TOKEN = os.getenv("DISCORD_TOKEN")
if TOKEN:
    bot.run(TOKEN)
