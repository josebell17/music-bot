import discord
from discord import app_commands
from discord.ext import commands
import yt_dlp
import asyncio

class MusicBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        super().__init__(command_prefix="!", intents=intents)

    async def setup_hook(self):
        await self.tree.sync()
        print("Đã đồng bộ hóa các lệnh Slash!")

bot = MusicBot()

# Cấu hình yt-dlp & FFmpeg
YTDL_OPTIONS = {
    # 1. Bắt buộc yt-dlp lấy định dạng âm thanh bất kỳ khả thi nhất
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
    
    # 2. Đổi nguồn tìm kiếm mặc định sang YouTube Music (ytmsearch)
    'default_search': 'ytmsearch',
    'source_address': '0.0.0.0',
    'cookiefile': '/etc/secrets/cookies.txt',
    
    # 3. Sử dụng iOS client của YouTube Music (ít bị kiểm tra IP nhất)
    'extractor_args': {
        'youtube': {
            'player_client': ['ios', 'mweb']
        }
    }
}

FFMPEG_OPTIONS = {
    'before_options': '-reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5',
    'options': '-vn', # -vn giúp bỏ qua luồng hình ảnh, chỉ phát luồng tiếng
}

ytdl = yt_dlp.YoutubeDL(YTDL_OPTIONS)

# Lưu trữ danh sách phát cho từng máy chủ (Guild ID -> List of Dict)
queues = {}

def get_queue(guild_id):
    if guild_id not in queues:
        queues[guild_id] = []
    return queues[guild_id]

def play_next(interaction):
    guild_id = interaction.guild_id
    queue = get_queue(guild_id)
    voice_client = interaction.guild.voice_client

    if queue and voice_client and voice_client.is_connected():
        next_song = queue.pop(0)
        source = discord.FFmpegPCMAudio(next_song['url'], **FFMPEG_OPTIONS)
        voice_client.play(source, after=lambda e: play_next(interaction))
        
        # Gửi thông báo phát bài tiếp theo vào kênh chat
        coro = interaction.channel.send(f"🎵 Đang phát bài tiếp theo: **{next_song['title']}**")
        asyncio.run_coroutine_threadsafe(coro, bot.loop)
    else:
        if voice_client and voice_client.is_connected():
            asyncio.run_coroutine_threadsafe(voice_client.disconnect(), bot.loop)

@bot.event
async def on_ready():
    print(f'Bot {bot.user.name} đã sẵn sàng hoạt động!')

# 1. Lệnh phát nhạc / thêm vào hàng chờ
@bot.tree.command(name="play", description="Phát nhạc từ YouTube hoặc thêm vào danh sách chờ")
async def play(interaction: discord.Interaction, search: str):
    await interaction.response.defer()

    if not interaction.user.voice:
        await interaction.followup.send("Bạn phải vào một phòng thoại trước!")
        return

    channel = interaction.user.voice.channel
    voice_client = interaction.guild.voice_client

    if voice_client is None:
        voice_client = await channel.connect()

    try:
        loop = asyncio.get_event_loop()
        data = await loop.run_in_executor(None, lambda: ytdl.extract_info(search, download=False))
        
        queue = get_queue(interaction.guild_id)

        # Xử lý nếu là Playlist hoặc bài hát đơn
        songs_added = []
        if 'entries' in data and data['entries']:
            # Nếu truyền vào từ khóa tìm kiếm hoặc link playlist
            if 'extract_flat' in data or not search.startswith("http"):
                first_entry = data['entries'][0]
                songs_added.append({'url': first_entry['url'], 'title': first_entry.get('title', 'Bài hát')})
            else:
                for entry in data['entries']:
                    if entry:
                        songs_added.append({'url': entry['url'], 'title': entry.get('title', 'Bài hát')})
        else:
            songs_added.append({'url': data['url'], 'title': data.get('title', 'Bài hát')})

        for song in songs_added:
            queue.append(song)

        if not voice_client.is_playing() and not voice_client.is_paused():
            current_song = queue.pop(0)
            source = discord.FFmpegPCMAudio(current_song['url'], **FFMPEG_OPTIONS)
            voice_client.play(source, after=lambda e: play_next(interaction))
            await interaction.followup.send(f'▶️ Đang phát: **{current_song["title"]}**')
        else:
            if len(songs_added) == 1:
                await interaction.followup.send(f'📝 Đã thêm vào danh sách chờ: **{songs_added[0]["title"]}**')
            else:
                await interaction.followup.send(f'📚 Đã thêm **{len(songs_added)}** bài hát từ playlist vào danh sách chờ!')

    except Exception as e:
        await interaction.followup.send(f"Lỗi tải nhạc: {e}")

# 2. Lệnh Tạm dừng
@bot.tree.command(name="pause", description="Tạm dừng nhạc đang phát")
async def pause(interaction: discord.Interaction):
    voice_client = interaction.guild.voice_client
    if voice_client and voice_client.is_playing():
        voice_client.pause()
        await interaction.response.send_message("⏸️ Đã tạm dừng bài hát!")
    else:
        await interaction.response.send_message("Không có bài hát nào đang phát để tạm dừng.", ephemeral=True)

# 3. Lệnh Tiếp tục phát
@bot.tree.command(name="resume", description="Tiếp tục phát nhạc")
async def resume(interaction: discord.Interaction):
    voice_client = interaction.guild.voice_client
    if voice_client and voice_client.is_paused():
        voice_client.resume()
        await interaction.response.send_message("▶️ Đã tiếp tục phát nhạc!")
    else:
        await interaction.response.send_message("Nhạc không ở trạng thái tạm dừng.", ephemeral=True)

# 4. Lệnh Bỏ qua bài hiện tại
@bot.tree.command(name="skip", description="Bỏ qua bài hát hiện tại")
async def skip(interaction: discord.Interaction):
    voice_client = interaction.guild.voice_client
    if voice_client and (voice_client.is_playing() or voice_client.is_paused()):
        voice_client.stop()
        await interaction.response.send_message("⏭️ Đã bỏ qua bài hát hiện tại!")
    else:
        await interaction.response.send_message("Không có bài hát nào đang phát.", ephemeral=True)

# 5. Lệnh Xem danh sách chờ
@bot.tree.command(name="queue", description="Xem danh sách bài hát đang chờ")
async def show_queue(interaction: discord.Interaction):
    queue = get_queue(interaction.guild_id)
    if not queue:
        await interaction.response.send_message("Danh sách chờ hiện tại đang trống!")
        return

    msg = "**📋 Danh sách bài hát đang chờ:**\n"
    for idx, song in enumerate(queue[:10], start=1):
        msg += f"{idx}. {song['title']}\n"

    if len(queue) > 10:
        msg += f"*...và còn {len(queue) - 10} bài hát khác.*"

    await interaction.response.send_message(msg)

# 6. Lệnh Dừng phát & Dọn danh sách
@bot.tree.command(name="stop", description="Dừng phát nhạc và xóa toàn bộ danh sách chờ")
async def stop(interaction: discord.Interaction):
    guild_id = interaction.guild_id
    queues[guild_id] = []
    voice_client = interaction.guild.voice_client

    if voice_client:
        voice_client.stop()
        await voice_client.disconnect()
        await interaction.response.send_message("⏹️ Đã dừng phát nhạc, xóa danh sách chờ và rời khỏi phòng thoại.")
    else:
        await interaction.response.send_message("Bot không ở trong phòng thoại.", ephemeral=True)

import os

TOKEN = os.getenv("DISCORD_TOKEN")
bot.run(TOKEN)
