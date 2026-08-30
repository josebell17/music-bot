import os
import discord
from discord import app_commands
from discord.ext import commands
import wavelink

intents = discord.Intents.default()
intents.message_content = True

class MusicBot(commands.Bot):
    def __init__(self):
        super().__init__(command_prefix="!", intents=intents)

  async def setup_hook(self):
        # Kết nối Lavalink Node công cộng mới (Dùng SSL/HTTPS cực kỳ ổn định)
        host = os.getenv("LAVALINK_HOST", "lava-v3.ajiezy.de")
        port = int(os.getenv("LAVALINK_PORT", 443))
        password = os.getenv("LAVALINK_PASS", "https://dsc.gg/ajiezy")

        node = wavelink.Node(uri=f"https://{host}:{port}", password=password)
        await wavelink.Pool.connect(nodes=[node], client=self)
        
        # Đồng bộ Slash Commands với Discord
        await self.tree.sync()
        print("Đã đồng bộ Slash Commands thành công!")

bot = MusicBot()

@bot.event
async def on_ready():
    print(f"Bot đã online: {bot.user}")

@bot.event
async def on_wavelink_track_end(payload: wavelink.TrackEndEventPayload):
    player: wavelink.Player = payload.player
    if player and not player.queue.is_empty:
        next_track = await player.queue.get_wait()
        await player.play(next_track)

# ---------------- LỆNH /PLAY ----------------
@bot.tree.command(name="play", description="Phát nhạc từ YouTube/SoundCloud")
@app_commands.describe(search="Tên bài hát hoặc đường link")
async def play(interaction: discord.Interaction, search: str):
    if not interaction.user.voice:
        return await interaction.response.send_message("❌ Bạn phải vào một Voice Channel trước!", ephemeral=True)

    # Báo cho Discord biết bot đang xử lý (tránh lỗi 3s không phản hồi)
    await interaction.response.defer()

    # Kết nối vào voice channel
    if not interaction.guild.voice_client:
        vc: wavelink.Player = await interaction.user.voice.channel.connect(cls=wavelink.Player)
    else:
        vc: wavelink.Player = interaction.guild.voice_client

    # Tìm kiếm bài hát
    tracks: wavelink.Search = await wavelink.Playable.search(search)
    if not tracks:
        return await interaction.followup.send("❌ Không tìm thấy bài hát nào!")

    track = tracks[0]

    if vc.playing:
        await vc.queue.put_wait(track)
        await interaction.followup.send(f"➕ Đã thêm vào hàng đợi: **{track.title}**")
    else:
        await vc.play(track)
        await interaction.followup.send(f"🎶 Đang phát: **{track.title}**")

# ---------------- LỆNH /PAUSE ----------------
@bot.tree.command(name="pause", description="Tạm dừng bài hát")
async def pause(interaction: discord.Interaction):
    vc: wavelink.Player = interaction.guild.voice_client
    if not vc or not vc.playing:
        return await interaction.response.send_message("❌ Bot hiện không phát bài hát nào!", ephemeral=True)

    await vc.pause(True)
    await interaction.response.send_message("⏸️ Đã tạm dừng phát nhạc.")

# ---------------- LỆNH /RESUME ----------------
@bot.tree.command(name="resume", description="Tiếp tục phát nhạc")
async def resume(interaction: discord.Interaction):
    vc: wavelink.Player = interaction.guild.voice_client
    if not vc or not vc.paused:
        return await interaction.response.send_message("❌ Nhạc đang phát bình thường!", ephemeral=True)

    await vc.pause(False)
    await interaction.response.send_message("▶️ Đã tiếp tục phát nhạc.")

# ---------------- LỆNH /SKIP ----------------
@bot.tree.command(name="skip", description="Bỏ qua bài hát hiện tại")
async def skip(interaction: discord.Interaction):
    vc: wavelink.Player = interaction.guild.voice_client
    if not vc or not vc.playing:
        return await interaction.response.send_message("❌ Không có bài hát nào để bỏ qua!", ephemeral=True)

    await vc.skip(force=True)
    await interaction.response.send_message("⏭️ Đã chuyển sang bài tiếp theo.")

# ---------------- LỆNH /STOP ----------------
@bot.tree.command(name="stop", description="Dừng phát nhạc và rời kênh")
async def stop(interaction: discord.Interaction):
    vc: wavelink.Player = interaction.guild.voice_client
    if not vc:
        return await interaction.response.send_message("❌ Bot không ở trong kênh thoại!", ephemeral=True)

    vc.queue.clear()
    await vc.disconnect()
    await interaction.response.send_message("⏹️ Đã dừng phát nhạc và rời khỏi kênh.")

# ---------------- LỆNH /QUEUE ----------------
@bot.tree.command(name="queue", description="Xem hàng đợi bài hát")
async def queue(interaction: discord.Interaction):
    vc: wavelink.Player = interaction.guild.voice_client
    if not vc or vc.queue.is_empty:
        return await interaction.response.send_message("📄 Hàng đợi hiện đang trống!", ephemeral=True)

    msg = "**📄 Hàng đợi bài hát:**\n"
    for i, track in enumerate(vc.queue, start=1):
        msg += f"{i}. **{track.title}**\n"
        if i >= 10:
            msg += f"... và còn {len(vc.queue) - 10} bài nữa."
            break

    await interaction.response.send_message(msg)

# Chạy Bot
TOKEN = os.getenv("DISCORD_TOKEN")
if TOKEN:
    bot.run(TOKEN)
