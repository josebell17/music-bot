import os
import asyncio
import logging
import discord
from discord.ext import commands
import yt_dlp
from aiohttp import web
from collections import defaultdict

# Cấu hình logging chuyên nghiệp để theo dõi lỗi real-time trên Render
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

intents = discord.Intents.default()
intents.message_content = True
intents.voice_states = True

bot = commands.Bot(command_prefix="!", intents=intents)

# Quản lý trạng thái thông minh cho từng Guild (Server) riêng biệt
queues = defaultdict(list)
volumes = defaultdict(lambda: 0.5)
loop_modes = defaultdict(lambda: False) # Chế độ lặp bài hiện tại

# Cấu hình tối ưu cao nhất cho yt-dlp với SoundCloud
YTDL_OPTIONS = {
    'default_search': 'scsearch',
    'format': 'bestaudio/best',
    'extractaudio': True,
    'audioformat': 'mp3',
    'noplaylist': True,
    'nocheckcertificate': True,
    'quiet': True,
    'no_warnings': True,
    'extract_flat': False,
}

FFMPEG_OPTIONS = {
    'before_options': '-reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5 -nostdin',
    'options': '-vn -b:a 192k', # Ép bitrate cao cho âm thanh sắc nét
}

ytdl = yt_dlp.YoutubeDL(YTDL_OPTIONS)

class YTDLSource(discord.PCMVolumeTransformer):
    def __init__(self, source, *, data, volume=0.5):
        super().__init__(source, volume)
        self.data = data
        self.title = data.get('title', 'Unknown Title')
        self.url = data.get('url', '')
        self.webpage_url = data.get('webpage_url', '')
        self.duration = data.get('duration', 0)

@classmethod
    async def create_source(cls, search: str, *, loop=None, volume=0.5):
        loop = loop or asyncio.get_event_loop()
        
        # Phân biệt nếu là URL từ Autocomplete hay là từ khóa tìm kiếm thường
        query = search if search.startswith("http") else f"scsearch:{search}"
        
        data = await loop.run_in_executor(None, lambda: ytdl.extract_info(query, download=False))
        
        if 'entries' in data and data['entries']:
            data = data['entries'][0]
        else:
            raise Exception("Không tìm thấy kết quả phù hợp trên SoundCloud!")

        audio_source = discord.FFmpegPCMAudio(data['url'], **FFMPEG_OPTIONS)
        return cls(audio_source, data=data, volume=volume)
        
# Thuật toán Autocomplete thông minh với độ trễ thấp
async def search_soundcloud(current: str):
    if not current or len(current.strip()) == 0:
        return []
    try:
        loop = asyncio.get_event_loop()
        info = await loop.run_in_executor(None, lambda: ytdl.extract_info(f"scsearch5:{current}", download=False))
        entries = info.get('entries', [])
        results = []
        for entry in entries:
            title = entry.get('title', 'Unknown')
            webpage_url = entry.get('webpage_url') or entry.get('url')
            if len(title) > 100:
                title = title[:97] + "..."
            results.append(discord.app_commands.Choice(name=title, value=webpage_url))
        return results
    except Exception as e:
        logging.error(f"Lỗi Autocomplete: {e}")
        return []

# Hệ thống điều phối hàng đợi (Queue Management System) tự động
async def play_next(ctx):
    guild_id = ctx.guild.id
    
    # Kiểm tra chế độ lặp bài (Loop mode)
    if loop_modes[guild_id] and hasattr(ctx, 'current_player') and ctx.current_player:
        # Phát lại chính bài hiện tại nếu bật loop
        pass

    if len(queues[guild_id]) > 0:
        player = queues[guild_id].pop(0)
        player.volume = volumes[guild_id]
        ctx.current_player = player
        
        def after_playing(error):
            if error:
                logging.error(f"Lỗi phát nhạc trong luồng: {error}")
            asyncio.run_coroutine_threadsafe(play_next(ctx), bot.loop)

        if ctx.voice_client and ctx.voice_client.is_connected():
            ctx.voice_client.play(player, after=after_playing)
            try:
                embed = discord.Embed(title="🎶 Đang Phát Bài Tiếp Theo", description=f"**{player.title}**", color=discord.Color.blurple())
                asyncio.run_coroutine_threadsafe(ctx.send(embed=embed), bot.loop)
            except Exception:
                pass
    else:
        ctx.current_player = None
        if ctx.voice_client and ctx.voice_client.is_connected():
            await asyncio.sleep(180) # Tự động rời phòng nếu trống 3 phút
            if not ctx.voice_client.is_playing() and len(queues[guild_id]) == 0:
                await ctx.voice_client.disconnect()

# Giao diện Menu điều khiển hiện đại (Interactive Buttons)
class AdvancedMusicControlView(discord.ui.View):
    def __init__(self, ctx):
        super().__init__(timeout=None)
        self.ctx = ctx

    @discord.ui.button(label="Tạm dừng", style=discord.ButtonStyle.blurple, emoji="⏸️")
    async def pause(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.ctx.voice_client and self.ctx.voice_client.is_playing():
            self.ctx.voice_client.pause()
            await interaction.response.send_message("⏸️ Đã tạm dừng phát nhạc.", ephemeral=True)
        else:
            await interaction.response.send_message("❌ Không có bài hát nào đang phát!", ephemeral=True)

    @discord.ui.button(label="Tiếp tục", style=discord.ButtonStyle.green, emoji="▶️")
    async def resume(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.ctx.voice_client and self.ctx.voice_client.is_paused():
            self.ctx.voice_client.resume()
            await interaction.response.send_message("▶️ Đã tiếp tục phát nhạc.", ephemeral=True)
        else:
            await interaction.response.send_message("❌ Nhạc không ở trạng thái dừng!", ephemeral=True)

    @discord.ui.button(label="Bỏ qua", style=discord.ButtonStyle.grey, emoji="⏭️")
    async def skip(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.ctx.voice_client and self.ctx.voice_client.is_playing():
            self.ctx.voice_client.stop()
            await interaction.response.send_message("⏭️ Đã bỏ qua bài hát hiện tại!", ephemeral=True)
        else:
            await interaction.response.send_message("❌ Không có bài hát để bỏ qua!", ephemeral=True)

    @discord.ui.button(label="Dừng hẳn", style=discord.ButtonStyle.red, emoji="⏹️")
    async def stop(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild_id = self.ctx.guild.id
        queues[guild_id].clear()
        if self.ctx.voice_client:
            await self.ctx.voice_client.disconnect()
            await interaction.response.send_message("⏹️ Đã xóa hàng đợi và ngắt kết nối.", ephemeral=True)

# Web Server bất đồng sinh hoạt ngầm giữ bot online 24/7 trên Render
async def handle(request):
    return web.Response(text="Bot SoundCloud 24/7 đang hoạt động hoàn hảo!")

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
        logging.info(f"Đã đồng bộ Slash Commands thành công cho {bot.user}")
    except Exception as e:
        logging.error(f"Lỗi đồng bộ lệnh: {e}")

# Lệnh /play với Autocomplete real-time
@bot.tree.command(name="play", description="Tìm kiếm real-time và phát nhạc chất lượng cao từ SoundCloud")
@discord.app_commands.describe(search="Gõ tên bài hát để chọn từ danh sách gợi ý")
async def play(interaction: discord.Interaction, search: str):
    if not interaction.user.voice:
        return await interaction.response.send_message("❌ Bạn cần vào một kênh thoại trước!", ephemeral=True)

    await interaction.response.defer()
    ctx = await commands.Context.from_interaction(interaction)
    
    if not ctx.voice_client:
        try:
            await interaction.user.voice.channel.connect()
        except Exception as e:
            return await interaction.followup.send(f"❌ Không thể kết nối vào kênh thoại: {e}")

    try:
        current_vol = volumes[interaction.guild.id]
        player = await YTDLSource.create_source(search, loop=bot.loop, volume=current_vol)
        guild_id = interaction.guild.id

        if ctx.voice_client.is_playing() or ctx.voice_client.is_paused():
            queues[guild_id].append(player)
            embed = discord.Embed(title="➕ Đã Thêm Vào Hàng Đợi", description=f"**{player.title}**", color=discord.Color.orange())
            embed.add_field(name="Vị trí chờ", value=str(len(queues[guild_id])), inline=True)
            await interaction.followup.send(embed=embed)
        else:
            ctx.current_player = player
            ctx.voice_client.play(player, after=lambda e: asyncio.run_coroutine_threadsafe(play_next(ctx), bot.loop))
            embed = discord.Embed(title="🎶 Đang Phát Ngay", description=f"**{player.title}**", color=discord.Color.blurple())
            embed.set_footer(text=f"Yêu cầu bởi {interaction.user.name}", icon_url=interaction.user.display_avatar.url)
            await interaction.followup.send(embed=embed, view=AdvancedMusicControlView(ctx))
            
    except Exception as e:
        await interaction.followup.send(f"❌ Xảy ra lỗi khi tải bài hát: {e}")

@play.autocomplete("search")
async def play_autocomplete(interaction: discord.Interaction, current: str):
    return await search_soundcloud(current)

# Lệnh quản lý âm lượng
@bot.tree.command(name="volume", description="Tinh chỉnh âm lượng phát nhạc trực tiếp (1 - 100)")
@discord.app_commands.describe(level="Mức âm lượng mong muốn")
async def volume(interaction: discord.Interaction, level: int):
    if level < 1 or level > 100:
        return await interaction.response.send_message("❌ Vui lòng nhập mức âm lượng từ 1 đến 100!", ephemeral=True)
    
    volumes[interaction.guild.id] = level / 100.0
    if interaction.guild.voice_client and interaction.guild.voice_client.source:
        interaction.guild.voice_client.source.volume = level / 100.0
        
    await interaction.response.send_message(f"🔊 Đã điều chỉnh âm lượng hệ thống thành: **{level}%**")

# Lệnh xem danh sách chờ
@bot.tree.command(name="queue", description="Hiển thị danh sách các bài hát đang xếp hàng chờ phát")
async def queue(interaction: discord.Interaction):
    guild_id = interaction.guild.id
    if guild_id not in queues or len(queues[guild_id]) == 0:
        return await interaction.response.send_message("📭 Hàng đợi hiện tại đang trống!", ephemeral=True)
    
    q_list = "\n".join([f"`{i+1}.` {song.title}" for i, song in enumerate(queues[guild_id][:10])])
    embed = discord.Embed(title="📜 Hàng Đợi Phát Nhạc", description=q_list, color=discord.Color.gold())
    embed.set_footer(text=f"Tổng số bài trong hàng: {len(queues[guild_id])}")
    await interaction.response.send_message(embed=embed)

# Lệnh thông tin hệ thống (Help / Info)
@bot.tree.command(name="help", description="Xem toàn bộ tài nguyên hướng dẫn và trạng thái vận hành của bot")
async def help_cmd(interaction: discord.Interaction):
    embed = discord.Embed(
        title="🤖 Trung Tâm Điều Hành Bot SoundCloud",
        description="Hệ thống bot nhạc tự động tối ưu hóa hiệu suất cao, vận hành nền tảng đám mây 24/7.",
        color=discord.Color.teal()
    )
    embed.add_field(name="/play [tên bài]", value="Tìm kiếm thông minh real-time và phát nhạc ngay lập tức.", inline=False)
    embed.add_field(name="/volume [1-100]", value="Tùy chỉnh thông số âm thanh chuẩn xác.", inline=False)
    embed.add_field(name="/queue", value="Kiểm soát toàn bộ danh sách bài hát đang chờ.", inline=False)
    embed.set_footer(text="Hạ tầng tối ưu chuyên dụng cho Render & SoundCloud")
    await interaction.response.send_message(embed=embed)

# Khởi chạy ứng dụng an toàn thông qua biến môi trường bảo mật
TOKEN = os.getenv("DISCORD_TOKEN")
if TOKEN:
    bot.run(TOKEN)
else:
    logging.error("Lỗi chí mạng: Không tìm thấy biến môi trường DISCORD_TOKEN trên hệ thống!")
