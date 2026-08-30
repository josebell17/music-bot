import os
import discord
from discord.ext import commands
import wavelink

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="/", intents=intents)

# Thông tin kết nối Lavalink Node
LAVALINK_HOST = os.getenv("LAVALINK_HOST", "free-lava.heavencloud.in")
LAVALINK_PORT = int(os.getenv("LAVALINK_PORT", 4000))
LAVALINK_PASS = os.getenv("LAVALINK_PASS", "heavencloud.in")

@bot.event
async def on_ready():
    print(f"Bot đã online với tên: {bot.user}")
    
    # Kết nối Bot tới Lavalink Node
    node = wavelink.Node(
        uri=f"http://{LAVALINK_HOST}:{LAVALINK_PORT}",
        password=LAVALINK_PASS
    )
    await wavelink.Pool.connect(nodes=[node], client=bot)

@bot.event
async def on_wavelink_node_ready(payload: wavelink.NodeReadyEventPayload):
    print(f"Đã kết nối thành công tới Lavalink Node: {payload.node.identifier}")

# Sự kiện tự động phát bài tiếp theo khi bài hiện tại kết thúc
@bot.event
async def on_wavelink_track_end(payload: wavelink.TrackEndEventPayload):
    player: wavelink.Player = payload.player
    if player and not player.queue.is_empty:
        next_track = await player.queue.get_wait()
        await player.play(next_track)

# ---------------- LỆNH PHÁT NHẠC (PLAY) ----------------
@bot.command(name="play", aliases=["p"])
async def play(ctx: commands.Context, *, search: str):
    if not ctx.author.voice:
        return await ctx.send("❌ Bạn phải vào một Voice Channel trước!")

    # Tham gia kênh thoại
    if not ctx.voice_client:
        vc: wavelink.Player = await ctx.author.voice.channel.connect(cls=wavelink.Player)
    else:
        vc: wavelink.Player = ctx.voice_client

    # Tìm kiếm bài hát
    tracks: wavelink.Search = await wavelink.Playable.search(search)
    if not tracks:
        return await ctx.send("❌ Không tìm thấy bài hát nào!")

    track = tracks[0]

    # Nếu đang phát nhạc thì thêm vào hàng đợi (Queue), nếu không thì phát ngay
    if vc.playing:
        await vc.queue.put_wait(track)
        await ctx.send(f"➕ Đã thêm vào hàng đợi: **{track.title}**")
    else:
        await vc.play(track)
        await ctx.send(f"🎶 Đang phát: **{track.title}**")

# ---------------- LỆNH TẠM DỪNG (PAUSE) ----------------
@bot.command(name="pause")
async def pause(ctx: commands.Context):
    vc: wavelink.Player = ctx.voice_client
    if not vc or not vc.playing:
        return await ctx.send("❌ Bot hiện không phát bài hát nào!")
    
    await vc.pause(True)
    await ctx.send("⏸️ Đã tạm dừng phát nhạc.")

# ---------------- LỆNH TIẾP TỤC (RESUME) ----------------
@bot.command(name="resume")
async def resume(ctx: commands.Context):
    vc: wavelink.Player = ctx.voice_client
    if not vc:
        return await ctx.send("❌ Bot chưa vào kênh thoại!")
    if not vc.paused:
        return await ctx.send("❌ Nhạc đang phát bình thường, không bị tạm dừng!")

    await vc.pause(False)
    await ctx.send("▶️ Đã tiếp tục phát nhạc.")

# ---------------- LỆNH BỎ QUA BÀI (SKIP) ----------------
@bot.command(name="skip", aliases=["s"])
async def skip(ctx: commands.Context):
    vc: wavelink.Player = ctx.voice_client
    if not vc or not vc.playing:
        return await ctx.send("❌ Không có bài hát nào để bỏ qua!")

    await vc.skip(force=True)
    await ctx.send("⏭️ Đã chuyển sang bài tiếp theo.")

# ---------------- LỆNH DỪNG & THÁT (STOP) ----------------
@bot.command(name="stop", aliases=["leave", "disconnect"])
async def stop(ctx: commands.Context):
    vc: wavelink.Player = ctx.voice_client
    if not vc:
        return await ctx.send("❌ Bot không ở trong kênh thoại nào!")

    vc.queue.clear()
    await vc.disconnect()
    await ctx.send("⏹️ Đã dừng phát nhạc, xóa hàng đợi và rời khỏi kênh thoại.")

# ---------------- LỆNH XEM HÀNG ĐỢI (QUEUE) ----------------
@bot.command(name="queue", aliases=["q"])
async def queue(ctx: commands.Context):
    vc: wavelink.Player = ctx.voice_client
    if not vc or vc.queue.is_empty:
        return await ctx.send("📄 Hàng đợi hiện đang trống!")

    msg = "**📄 Hàng đợi bài hát:**\n"
    for i, track in enumerate(vc.queue, start=1):
        msg += f"{i}. **{track.title}**\n"
        if i >= 10:  # Chỉ hiển thị tối đa 10 bài đầu tiên
            msg += f"... và còn {len(vc.queue) - 10} bài nữa."
            break

    await ctx.send(msg)

# ---------------- LỆNH BÀI ĐANG PHÁT (NOW PLAYING) ----------------
@bot.command(name="np")
async def now_playing(ctx: commands.Context):
    vc: wavelink.Player = ctx.voice_client
    if not vc or not vc.current:
        return await ctx.send("❌ Hiện không có bài hát nào đang phát!")

    await ctx.send(f"🎧 Đang phát: **{vc.current.title}**")

# ---------------- LỆNH CHỈNH ÂM LƯỢNG (VOLUME) ----------------
@bot.command(name="volume", aliases=["vol"])
async def volume(ctx: commands.Context, vol: int):
    vc: wavelink.Player = ctx.voice_client
    if not vc:
        return await ctx.send("❌ Bot chưa vào kênh thoại!")
    
    if not 0 <= vol <= 100:
        return await ctx.send("❌ Âm lượng phải nằm trong khoảng từ 0 đến 100!")

    await vc.set_volume(vol)
    await ctx.send(f"🔊 Đã chỉnh âm lượng thành: **{vol}%**")

# Chạy Bot
TOKEN = os.getenv("DISCORD_TOKEN")
if TOKEN:
    bot.run(TOKEN)
