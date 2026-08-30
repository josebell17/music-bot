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
    # Khởi chạy Web Server ngầm để nhận request ping duy trì 24/7
    bot.loop.create_task(start_web_server())
    
    # Đồng bộ Slash Commands lên Discord
    await bot.tree.sync()
    print(f"✅ Bot {bot.user} đã online và sẵn sàng!")

# ---------------- LỆNH /PLAY HOẶC !PLAY ----------------
@bot.hybrid_command(name="play", description="Phát nhạc từ SoundCloud")
async def play(ctx: commands.Context, *, search: str):
    if not ctx.author.voice:
        return await ctx.send("❌ Bạn phải vào một Voice Channel trước!")

    await ctx.defer()

    if not ctx.voice_client:
        await ctx.author.voice.channel.connect()

    try:
        player = await YTDLSource.from_url(search, loop=bot.loop, stream=True)
        ctx.voice_client.play(player, after=lambda e: print(f'Lỗi khi phát: {e}') if e else None)
        await ctx.send(f"🎶 Đang phát: **{player.title}**")
    except Exception as e:
        await ctx.send(f"❌ Có lỗi xảy ra: {e}")

# ---------------- LỆNH /PAUSE HOẶC !PAUSE ----------------
@bot.hybrid_command(name="pause", description="Tạm dừng nhạc")
async def pause(ctx: commands.Context):
    if ctx.voice_client and ctx.voice_client.is_playing():
        ctx.voice_client.pause()
        await ctx.send("⏸️ Đã tạm dừng phát nhạc.")
    else:
        await ctx.send("❌ Hiện không có bài hát nào đang phát!")

# ---------------- LỆNH /RESUME HOẶC !RESUME ----------------
@bot.hybrid_command(name="resume", description="Tiếp tục phát nhạc")
async def resume(ctx: commands.Context):
    if ctx.voice_client and ctx.voice_client.is_paused():
        ctx.voice_client.resume()
        await ctx.send("▶️ Đã tiếp tục phát nhạc.")
    else:
        await ctx.send("❌ Nhạc đang phát bình thường!")

# ---------------- LỆNH /STOP HOẶC !STOP ----------------
@bot.hybrid_command(name="stop", description="Dừng phát nhạc và rời kênh")
async def stop(ctx: commands.Context):
    if ctx.voice_client:
        await ctx.voice_client.disconnect()
        await ctx.send("⏹️ Đã dừng phát nhạc và rời khỏi kênh thoại.")
    else:
        await ctx.send("❌ Bot không ở trong kênh thoại!")

# Chạy Bot
TOKEN = os.getenv("DISCORD_TOKEN")
if TOKEN:
    bot.run(TOKEN)
