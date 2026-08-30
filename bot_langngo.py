import os
import asyncio
import discord
from discord.ext import commands
import yt_dlp
from aiohttp import web

intents = discord.Intents.default()
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents)

# Cấu hình yt-dlp dùng SoundCloud
YTDL_OPTIONS = {
    'default_search': 'scsearch',
    'format': 'bestaudio/best',
    'extractaudio': True,
    'audioformat': 'mp3',
    'outtmpl': '%(extractor)s-%(id)s-%(title)s.%(ext)s',
    'restrictfilenames': True,
    'noplaylist': True,
    'nocheckcertificate': True,
    'ignoreerrors': False,
    'logtostderr': False,
    'quiet': True,
    'no_warnings': True,
    'source_address': '0.0.0.0',
}

FFMPEG_OPTIONS = {
    'before_options': '-reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5',
    'options': '-vn',
}

ytdl = yt_dlp.YoutubeDL(YTDL_OPTIONS)

class YTDLSource(discord.PCMVolumeTransformer):
    def __init__(self, source, *, data, volume=0.5):
        super().__init__(source, volume)
        self.data = data
        self.title = data.get('title')
        self.url = data.get('url')

    @classmethod
    async def from_url(cls, url, *, loop=None, stream=True):
        loop = loop or asyncio.get_event_loop()
        data = await loop.run_in_executor(None, lambda: ytdl.extract_info(url, download=not stream))

        if 'entries' in data:
            data = data['entries'][0]

        filename = data['url'] if stream else ytdl.prepare_filename(data)
        return cls(discord.FFmpegPCMAudio(filename, **FFMPEG_OPTIONS), data=data)

# ---------------- GIAO DIỆN MENU NÚT BẤM (UI VIEWS) ----------------
class MusicControlView(discord.ui.View):
    def __init__(self, ctx):
        super().__init__(timeout=None)
        self.ctx = ctx

    @discord.ui.button(label="Tạm dừng", style=discord.ButtonStyle.blurple, emoji="⏸️")
    async def pause_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.ctx.voice_client and self.ctx.voice_client.is_playing():
            self.ctx.voice_client.pause()
            await interaction.response.send_message("⏸️ Đã tạm dừng phát nhạc.", ephemeral=True)
        else:
            await interaction.response.send_message("❌ Không có nhạc đang phát!", ephemeral=True)

    @discord.ui.button(label="Tiếp tục", style=discord.ButtonStyle.green, emoji="▶️")
    async def resume_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.ctx.voice_client and self.ctx.voice_client.is_paused():
            self.ctx.voice_client.resume()
            await interaction.response.send_message("▶️ Đã tiếp tục phát nhạc.", ephemeral=True)
        else:
            await interaction.response.send_message("❌ Nhạc không ở trạng thái dừng!", ephemeral=True)

    @discord.ui.button(label="Dừng & Thoát", style=discord.ButtonStyle.red, emoji="⏹️")
    async def stop_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.ctx.voice_client:
            await self.ctx.voice_client.disconnect()
            await interaction.response.send_message("⏹️ Đã dừng phát nhạc và rời kênh.", ephemeral=True)
        else:
            await interaction.response.send_message("❌ Bot không ở trong kênh thoại!", ephemeral=True)

# ---------------- HTTP SERVER CHO CƠ CHẾ PING 5 PHÚT ----------------
async def handle(request):
    return web.Response(text="Bot đang online!")

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
    await bot.tree.sync()
    print(f"✅ Bot {bot.user} đã online và sẵn sàng!")

# ---------------- LỆNH /PLAY HOẶC !PLAY ----------------
@bot.hybrid_command(name="play", description="Phát nhạc từ SoundCloud kèm giao diện điều khiển")
async def play(ctx: commands.Context, *, search: str):
    if not ctx.author.voice:
        return await ctx.send("❌ Bạn phải vào một Voice Channel trước!")

    await ctx.defer()

    if not ctx.voice_client:
        await ctx.author.voice.channel.connect()

    try:
        player = await YTDLSource.from_url(search, loop=bot.loop, stream=True)
        ctx.voice_client.play(player, after=lambda e: print(f'Lỗi khi phát: {e}') if e else None)
        
        # Tạo khung Embed hiển thị bài hát đẹp mắt kèm Menu nút bấm
        embed = discord.Embed(
            title="🎶 Đang Phát Nhạc",
            description=f"**{player.title}**",
            color=discord.Color.blurple()
        )
        embed.set_footer(text=fYêu cầu bởi {ctx.author.name}", icon_url=ctx.author.display_avatar.url)
        
        view = MusicControlView(ctx)
        await ctx.send(embed=embed, view=view)
    except Exception as e:
        await ctx.send(f"❌ Có lỗi xảy ra: {e}")

# Chạy Bot
TOKEN = os.getenv("DISCORD_TOKEN")
if TOKEN:
    bot.run(TOKEN)
