import os
import asyncio
import logging
import discord
from discord.ext import commands
import yt_dlp
from aiohttp import web
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

intents = discord.Intents.default()
intents.message_content = True
intents.voice_states = True

bot = commands.Bot(command_prefix="!", intents=intents)

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
                embed = discord.Embed(
                    title="✧ ── ✦ 𝕸𝖚𝖘𝖎𝖈 𝖑𝖆𝖓𝖌𝖓𝖌𝖔 ✦ ── ✧", 
                    description=f"🔮 **Đang Phát:** \n` ⟡ ` **{player.title}**", 
                    color=discord.Color.from_rgb(88, 24, 131)
                )
                embed.set_footer(text="⚡ Cyberpunk Sound System • 24/7 Active")
                asyncio.run_coroutine_threadsafe(ctx.send(embed=embed, view=MusicControlView(ctx)), bot.loop)
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

    @discord.ui.button(style=discord.ButtonStyle.secondary, emoji="⏸️")
    async def pause(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.ctx.voice_client and self.ctx.voice_client.is_playing():
            self.ctx.voice_client.pause()
            await interaction.response.send_message("💤 `[System]` Đã tạm dừng nhịp đập âm thanh.", ephemeral=True)
        else:
            await interaction.response.send_message("⚠️ `[Error]` Không có tiến trình phát nào đang hoạt động.", ephemeral=True)

    @discord.ui.button(style=discord.ButtonStyle.secondary, emoji="▶️")
    async def resume(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.ctx.voice_client and self.ctx.voice_client.is_paused():
            self.ctx.voice_client.resume()
            await interaction.response.send_message("🔮 `[System]` Tiếp tục tái tạo âm thanh.", ephemeral=True)
        else:
            await interaction.response.send_message("⚠️ `[Error]` Tiến trình không ở trạng thái dừng.", ephemeral=True)

    @discord.ui.button(style=discord.ButtonStyle.secondary, emoji="⏭️")
    async def skip(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.ctx.voice_client and self.ctx.voice_client.is_playing():
            self.ctx.voice_client.stop()
            await interaction.response.send_message("📿 `[System]` Đã chuyển hướng sang tần số tiếp theo.", ephemeral=True)
        else:
            await interaction.response.send_message("⚠️ `[Error]` Hàng đợi hiện tại trống rỗng.", ephemeral=True)

    @discord.ui.button(style=discord.ButtonStyle.secondary, emoji="🖤")
    async def fast_save(self, interaction: discord.Interaction, button: discord.ui.Button):
        user_id = interaction.user.id
        current = getattr(self.ctx, 'current_player', None)
        if not current:
            return await interaction.response.send_message("⚠️ `[Error]` Không có tần số âm thanh nào đang hoạt động để lưu.", ephemeral=True)
        if current.title not in user_collections[user_id]:
            user_collections[user_id].append(current.title)
            await interaction.response.send_message(f"✨ `[Vault]` Đã lưu trữ: **{current.title}** vào không gian cá nhân.", ephemeral=True)
        else:
            await interaction.response.send_message("💠 `[Vault]` Tần số này đã tồn tại trong bộ sưu tập của bạn.", ephemeral=True)

    @discord.ui.button(style=discord.ButtonStyle.danger, emoji="⏹️")
    async def stop(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild_id = self.ctx.guild.id
        queues[guild_id].clear()
        if self.ctx.voice_client:
            await self.ctx.voice_client.disconnect()
            await interaction.response.send_message("🌌 `[System]` Đã ngắt kết nối không gian thoại.", ephemeral=True)

# Modal Xóa Bài Hát Khỏi Bộ Sưu Tập
class RemoveCollectionModal(discord.ui.Modal, title="🗑️ Xóa Bài Hát Khỏi Bộ Sưu Tập"):
    index_str = discord.ui.TextInput(label="Số thứ tự bài cần xóa (xem trong /collection)", placeholder="Nhập số thứ tự (ví dụ: 1)", required=True)

    async def on_submit(self, interaction: discord.Interaction):
        user_id = interaction.user.id
        favs = user_collections[user_id]
        try:
            idx = int(self.index_str.value)
            if 1 <= idx <= len(favs):
                removed = favs.pop(idx - 1)
                await interaction.response.send_message(f"🗑️ `[Vault]` Đã xóa thành công bài **{removed}** khỏi bộ sưu tập!", ephemeral=True)
            else:
                await interaction.response.send_message("⚠️ `[Error]` Số thứ tự không hợp lệ trong bộ sưu tập.", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("⚠️ `[Error]` Vui lòng chỉ nhập số nguyên.", ephemeral=True)

# Modal Sắp Xếp Lại Bộ Sưu Tập
class ReorderCollectionModal(discord.ui.Modal, title="🔄 Sắp Xếp Lại Bộ Sưu Tập"):
    from_pos = discord.ui.TextInput(label="Vị trí cũ của bài hát", placeholder="Ví dụ: 5", required=True)
    to_pos = discord.ui.TextInput(label="Vị trí mới muốn chuyển đến", placeholder="Ví dụ: 1", required=True)

    async def on_submit(self, interaction: discord.Interaction):
        user_id = interaction.user.id
        favs = user_collections[user_id]
        try:
            f = int(self.from_pos.value)
            t = int(self.to_pos.value)
            if 1 <= f <= len(favs) and 1 <= t <= len(favs):
                song = favs.pop(f - 1)
                favs.insert(t - 1, song)
                await interaction.response.send_message(f"🔄 `[Vault]` Đã dịch chuyển **{song}** từ vị trí `{f}` sang `{t}` thành công!", ephemeral=True)
            else:
                await interaction.response.send_message("⚠️ `[Error]` Vị trí nhập vào vượt quá giới hạn bộ sưu tập.", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("⚠️ `[Error]` Vui lòng chỉ nhập số nguyên.", ephemeral=True)

class CollectionView(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=60)
        self.user_id = user_id

    @discord.ui.button(style=discord.ButtonStyle.success, emoji="📥", label="Lưu bài đang phát")
    async def save_current(self, interaction: discord.Interaction, button: discord.ui.Button):
        ctx = await commands.Context.from_interaction(interaction)
        current = getattr(ctx, 'current_player', None)
        if not current:
            return await interaction.response.send_message("⚠️ `[Error]` Không có bài hát nào đang phát!", ephemeral=True)
        if current.title not in user_collections[self.user_id]:
            user_collections[self.user_id].append(current.title)
            await interaction.response.send_message(f"✨ `[Vault]` Đã khóa mã thành công: **{current.title}**", ephemeral=True)
        else:
            await interaction.response.send_message("💠 `[Vault]` Bài hát đã nằm trong kho lưu trữ.", ephemeral=True)

    @discord.ui.button(style=discord.ButtonStyle.primary, emoji="📂", label="Xem danh sách")
    async def view_list(self, interaction: discord.Interaction, button: discord.ui.Button):
        favs = user_collections[self.user_id]
        if not favs:
            return await interaction.response.send_message("📭 `[Vault]` Không gian lưu trữ của bạn đang trống.", ephemeral=True)
        fav_list = "\n".join([f"` ⟡ {i+1}. ` {song}" for i, song in enumerate(favs[:15])])
        embed = discord.Embed(
            title=f"🔮 Kho Lưu Trữ Của — {interaction.user.name}", 
            description=fav_list, 
            color=discord.Color.from_rgb(88, 24, 131)
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @discord.ui.button(style=discord.ButtonStyle.danger, emoji="🗑️", label="Xóa bài")
    async def remove_song(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(RemoveCollectionModal())

    @discord.ui.button(style=discord.ButtonStyle.secondary, emoji="🔄", label="Sắp xếp")
    async def reorder_song(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(ReorderCollectionModal())

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

@bot.tree.command(name="play", description="🔮 Phát nhạc trực tiếp bằng tên bài hát hoặc link SoundCloud")
@discord.app_commands.describe(search="Nhập tên bài hát hoặc dán link SoundCloud")
async def play(interaction: discord.Interaction, search: str):
    if not interaction.user.voice:
        return await interaction.response.send_message("🥀 `[Access Denied]` Vui lòng vào kênh thoại trước.", ephemeral=True)

    await interaction.response.defer()
    ctx = await commands.Context.from_interaction(interaction)
    
    if not ctx.voice_client:
        try:
            await interaction.user.voice.channel.connect()
        except Exception as e:
            return await interaction.followup.send(f"⚠️ `[Error]` Không thể kết nối kênh thoại: {e}")

    try:
        current_vol = volumes[interaction.guild.id]
        player = await YTDLSource.create_source(search, loop=bot.loop, volume=current_vol)
        guild_id = interaction.guild.id
        ctx.current_player = player

        if ctx.voice_client.is_playing() or ctx.voice_client.is_paused():
            queues[guild_id].append(player)
            embed = discord.Embed(
                title="💠 `[Queue System]` Đã thêm vào hàng đợi", 
                description=f"` ⟡ ` **{player.title}**", 
                color=discord.Color.from_rgb(45, 10, 75)
            )
            embed.add_field(name="Vị trí", value=f"` #{len(queues[guild_id])} `", inline=True)
            await interaction.followup.send(embed=embed)
        else:
            ctx.voice_client.play(player, after=lambda e: asyncio.run_coroutine_threadsafe(play_next(ctx), bot.loop))
            embed = discord.Embed(
                title="✧ ── ✦ 𝕸𝖚𝖘𝖎𝖈 𝖑𝖆𝖓𝖌𝖓𝖌𝖔 ✦ ── ✧", 
                description=f"🔮 **Đang Phát:** \n` ⟡ ` **{player.title}**", 
                color=discord.Color.from_rgb(88, 24, 131)
            )
            embed.set_footer(text=f"Yêu cầu bởi {interaction.user.name}", icon_url=interaction.user.display_avatar.url)
            await interaction.followup.send(embed=embed, view=MusicControlView(ctx))
            
    except Exception as e:
        await interaction.followup.send(f"⚠️ `[Error]` Không thể tải dữ liệu: {e}")

@bot.tree.command(name="myfavorite", description="🖤 Phát toàn bộ các tần số âm thanh trong không gian lưu trữ cá nhân")
async def myfavorite(interaction: discord.Interaction):
    if not interaction.user.voice:
        return await interaction.response.send_message("🥀 `[Access Denied]` Vui lòng vào kênh thoại trước.", ephemeral=True)
    
    user_id = interaction.user.id
    favs = user_collections[user_id]
    if not favs:
        return await interaction.response.send_message("📭 `[Vault]` Không gian lưu trữ trống. Hãy dùng nút `🖤` để lưu lại những tần số bạn yêu thích.", ephemeral=True)

    await interaction.response.defer()
    ctx = await commands.Context.from_interaction(interaction)
    
    if not ctx.voice_client:
        try:
            await interaction.user.voice.channel.connect()
        except Exception as e:
            return await interaction.followup.send(f"⚠️ `[Error]` Không thể kết nối kênh thoại: {e}")

    guild_id = interaction.guild.id
    current_vol = volumes[guild_id]
    
    added_count = 0
    first_player = None

    for song_name in favs:
        try:
            player = await YTDLSource.create_source(song_name, loop=bot.loop, volume=current_vol)
            if not first_player and not ctx.voice_client.is_playing() and not ctx.voice_client.is_paused():
                first_player = player
                ctx.current_player = player
            else:
                queues[guild_id].append(player)
            added_count += 1
        except Exception:
            continue

    if first_player:
        ctx.voice_client.play(first_player, after=lambda e: asyncio.run_coroutine_threadsafe(play_next(ctx), bot.loop))
        embed = discord.Embed(
            title="🖤 `[Vault Playback]` Đang Phát Kho Lưu Trữ", 
            description=f"Đã nạp thành công **{added_count} tần số âm thanh** vào hệ thống phát.", 
            color=discord.Color.from_rgb(88, 24, 131)
        )
        embed.set_footer(text=f"Yêu cầu bởi {interaction.user.name}", icon_url=interaction.user.display_avatar.url)
        await interaction.followup.send(embed=embed, view=MusicControlView(ctx))
    else:
        await interaction.followup.send("⚠️ `[Error]` Không thể khởi tạo danh sách lưu trữ lúc này.", ephemeral=True)

@bot.tree.command(name="queue", description="📜 Kiểm tra danh sách các tần số đang chờ phát")
async def queue(interaction: discord.Interaction):
    guild_id = interaction.guild.id
    if guild_id not in queues or len(queues[guild_id]) == 0:
        return await interaction.response.send_message("📭 `[Queue]` Hàng đợi hiện tại trống rỗng.", ephemeral=True)
    
    q_list = "\n".join([f"` ⟡ {i+1}. ` {song.title}" for i, song in enumerate(queues[guild_id][:15])])
    embed = discord.Embed(
        title="⚡ `[Queue System]` Danh Sách Chờ", 
        description=q_list, 
        color=discord.Color.from_rgb(60, 15, 100)
    )
    embed.set_footer(text=f"Tổng số bài trong hàng chờ: {len(queues[guild_id])}")
    await interaction.response.send_message(embed=embed)

@bot.tree.command(name="remove", description="🗑️ Loại bỏ một tần số cụ thể khỏi hàng đợi")
@discord.app_commands.describe(index="Số thứ tự bài hát trong /queue")
async def remove(interaction: discord.Interaction, index: int):
    guild_id = interaction.guild.id
    if guild_id not in queues or len(queues[guild_id]) == 0:
        return await interaction.response.send_message("⚠️ `[Error]` Hàng đợi trống.", ephemeral=True)
    
    if 1 <= index <= len(queues[guild_id]):
        removed = queues[guild_id].pop(index - 1)
        await interaction.response.send_message(f"🗡️ `[Queue]` Đã xóa bỏ tần số: **{removed.title}**")
    else:
        await interaction.response.send_message("⚠️ `[Error]` Số thứ tự không hợp lệ.", ephemeral=True)

@bot.tree.command(name="move", description="🔄 Sắp xếp lại vị trí tần số trong hàng đợi")
@discord.app_commands.describe(from_pos="Vị trí cũ", to_pos="Vị trí mới")
async def move(interaction: discord.Interaction, from_pos: int, to_pos: int):
    guild_id = interaction.guild.id
    q = queues[guild_id]
    if not q or not (1 <= from_pos <= len(q)) or not (1 <= to_pos <= len(q)):
        return await interaction.response.send_message("⚠️ `[Error]` Vị trí không hợp lệ.", ephemeral=True)
    
    song = q.pop(from_pos - 1)
    q.insert(to_pos - 1, song)
    await interaction.response.send_message(f"🔄 `[Queue]` Đã dịch chuyển **{song.title}** từ vị trí `{from_pos}` sang `{to_pos}`.")

@bot.tree.command(name="collection", description="💼 Truy cập giao diện quản lý kho lưu trữ cá nhân")
async def collection(interaction: discord.Interaction):
    embed = discord.Embed(
        title="🌌 Kho Lưu Trữ Cá Nhân — Music langngo",
        description="` ⟡ ` Sử dụng bảng điều khiển bên dưới để lưu trữ, xem danh sách, xóa hoặc sắp xếp lại các bài hát yêu thích.",
        color=discord.Color.from_rgb(88, 24, 131)
    )
    await interaction.response.send_message(embed=embed, view=CollectionView(interaction.user.id), ephemeral=True)

@bot.tree.command(name="volume", description="🔊 Tự động điều chỉnh âm lượng hệ thống (1 - 100)")
@discord.app_commands.describe(level="Mức âm lượng")
async def volume(interaction: discord.Interaction, level: int):
    if not (1 <= level <= 100):
        return await interaction.response.send_message("⚠️ `[Error]` Vui lòng chọn mức từ 1 đến 100.", ephemeral=True)
    
    volumes[interaction.guild.id] = level / 100.0
    if interaction.guild.voice_client and interaction.guild.voice_client.source:
        interaction.guild.voice_client.source.volume = level / 100.0
    await interaction.response.send_message(f"🔊 `[Audio]` Đã điều chỉnh biên độ âm thanh lên mức: **{level}%** ⚡")

@bot.tree.command(name="sleep", description="🌙 Hẹn giờ tự động ngắt kết nối hệ thống sau khoảng thời gian")
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
                await interaction.channel.send("🌙 `[System Timeout]` Hết thời gian hoạt động. Music langngo đã tự động ngắt kết nối.")
            except Exception:
                pass
        sleep_tasks.pop(guild_id, None)

    sleep_tasks[guild_id] = bot.loop.create_task(timer())
    await interaction.response.send_message(f"⏰ `[Timer]` Đã lập lịch tự động đóng băng hệ thống sau **{minutes} phút**.")

@bot.tree.command(name="help", description="🚀 Hiển thị bảng điều khiển & hướng dẫn chi tiết hệ thống Music langngo")
async def help_cmd(interaction: discord.Interaction):
    embed = discord.Embed(
        title="✧ ── ✦ HỆ THỐNG ÂM NHẠC MUSIC LANGNGO — DARK AESTHETIC ✦ ── ✧",
        description="` ⟡ ` Bảng điều khiển lệnh chi tiết dành cho hệ thống âm thanh độc quyền.",
        color=discord.Color.from_rgb(88, 24, 131)
    )
    
    embed.add_field(
        name="🔮 `/play [tên bài hoặc link]`", 
        value="• **Chức năng:** Truy xuất trực tiếp tần số âm thanh từ SoundCloud và phát vào kênh thoại.\n• **Cú pháp:** `/play lofi chill`", 
        inline=False
    )

    embed.add_field(
        name="🖤 `/myfavorite`", 
        value="• **Chức năng:** Tự động kích hoạt và nạp toàn bộ danh sách trong kho lưu trữ cá nhân của bạn.", 
        inline=False
    )
    
    embed.add_field(
        name="📜 `/queue`", 
        value="• **Chức năng:** Trích xuất bảng danh sách các bài hát đang xếp hàng chờ.", 
        inline=False
    )
    
    embed.add_field(
        name="🗑️ `/remove [số thứ tự]`", 
        value="• **Chức năng:** Xóa bỏ một bài hát khỏi danh sách chờ dựa trên số thứ tự.", 
        inline=False
    )
    
    embed.add_field(
        name="🔄 `/move [vị trí cũ] [vị trí mới]`", 
        value="• **Chức năng:** Đảo vị trí ưu tiên của bài hát trong hàng chờ.", 
        inline=False
    )
    
    embed.add_field(
        name="💼 `/collection`", 
        value="• **Chức năng:** Mở bảng giao diện cá nhân hóa để lưu trữ, xem, **xóa** hoặc **sắp xếp lại** các bài hát yêu thích qua nút bấm & bảng nhập liệu nhanh.", 
        inline=False
    )
    
    embed.add_field(
        name="🔊 `/volume [1 - 100]`", 
        value="• **Chức năng:** Tinh chỉnh công suất phát âm thanh trong kênh thoại.", 
        inline=False
    )
    
    embed.add_field(
        name="🌙 `/sleep [số phút]`", 
        value="• **Chức năng:** Đặt hẹn giờ tự động thu hồi bot khỏi phòng thoại.", 
        inline=False
    )
    
    embed.set_footer(text="⚡ Dark Aesthetic Code Structure | Music langngo", icon_url=interaction.user.display_avatar.url)
    await interaction.response.send_message(embed=embed, ephemeral=True)

TOKEN = os.getenv("DISCORD_TOKEN")
if TOKEN:
    bot.run(TOKEN)
