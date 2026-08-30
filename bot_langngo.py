import os
import asyncio
import logging
import discord
from discord.ext import commands
import yt_dlp
from collections import defaultdict

logging.basicConfig(level=logging.WARNING, format='%(asctime)s [%(levelname)s] %(message)s')

intents = discord.Intents.default()
intents.message_content = True
intents.voice_states = True

bot = commands.Bot(command_prefix="!", intents=intents)

queues = defaultdict(list)
volumes = defaultdict(lambda: 0.5)
sleep_tasks = {}
user_collections = defaultdict(list)

# Cấu hình yt_dlp tối ưu tốc độ mạng và tiết kiệm tài nguyên
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

# Tối ưu hóa bộ đệm FFmpeg cho 0.1 vCPU và băng thông mạng tối đa
FFMPEG_OPTIONS = {
    'before_options': '-reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5 -thread_queue_size 2048 -probesize 16K -analyzeduration 0 -nostdin',
    'options': '-vn -b:a 96k -threads 1',
}

ytdl = yt_dlp.YoutubeDL(YTDL_OPTIONS)

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
        
        # Tái sử dụng luồng URL đã cache, hoàn toàn không gọi yt_dlp search -> cực kỳ tiết kiệm vCPU
        if cached_stream_url:
            try:
                audio_source = discord.FFmpegPCMAudio(cached_stream_url, **FFMPEG_OPTIONS)
                return cls(audio_source, data={'title': search, 'url': cached_stream_url}, volume=volume)
            except Exception:
                pass

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
            asyncio.run_coroutine_threadsafe(play_next(ctx), bot.loop)

        if ctx.voice_client and ctx.voice_client.is_connected():
            ctx.voice_client.play(player, after=after_playing)
            try:
                embed = discord.Embed(
                    title="✧ ── ✦ 𝕸𝖚𝖘𝖎𝖈 𝖑𝖆𝖓𝖌𝖓𝖌𝖔 ✦ ── ✧", 
                    description=f"🔮 **Đang Phát:** \n**{player.title}**", 
                    color=discord.Color.from_rgb(88, 24, 131)
                )
                asyncio.run_coroutine_threadsafe(ctx.send(embed=embed, view=MusicControlView(ctx)), bot.loop)
            except Exception:
                pass
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
        if self.ctx.voice_client and self.ctx.voice_client.is_playing():
            self.ctx.voice_client.pause()
            await interaction.response.send_message("💤 Đã tạm dừng.", ephemeral=True)
        else:
            await interaction.response.send_message("⚠️ Không có nhạc đang phát.", ephemeral=True)

    @discord.ui.button(style=discord.ButtonStyle.secondary, emoji="▶️")
    async def resume(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.ctx.voice_client and self.ctx.voice_client.is_paused():
            self.ctx.voice_client.resume()
            await interaction.response.send_message("🔮 Tiếp tục phát.", ephemeral=True)
        else:
            await interaction.response.send_message("⚠️ Nhạc không ở trạng thái dừng.", ephemeral=True)

    @discord.ui.button(style=discord.ButtonStyle.secondary, emoji="⏭️")
    async def skip(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.ctx.voice_client and self.ctx.voice_client.is_playing():
            self.ctx.voice_client.stop()
            await interaction.response.send_message("📿 Đã chuyển bài.", ephemeral=True)
        else:
            await interaction.response.send_message("⚠️ Hàng đợi trống.", ephemeral=True)

    @discord.ui.button(style=discord.ButtonStyle.secondary, emoji="🖤")
    async def fast_save(self, interaction: discord.Interaction, button: discord.ui.Button):
        user_id = interaction.user.id
        current = getattr(self.ctx, 'current_player', None)
        if not current:
            return await interaction.response.send_message("⚠️ Không có bài hát nào đang hoạt động.", ephemeral=True)
        
        favs = user_collections[user_id]
        if not any(song['title'] == current.title for song in favs):
            favs.append({'title': current.title, 'stream_url': current.stream_url})
            await interaction.response.send_message(f"✨ Đã lưu kèm tối ưu link: **{current.title}**", ephemeral=True)
        else:
            await interaction.response.send_message("💠 Bài hát đã có trong bộ sưu tập.", ephemeral=True)

    @discord.ui.button(style=discord.ButtonStyle.danger, emoji="⏹️")
    async def stop(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild_id = self.ctx.guild.id
        queues[guild_id].clear()
        if self.ctx.voice_client:
            await self.ctx.voice_client.disconnect()
            await interaction.response.send_message("🌌 Đã ngắt kết nối.", ephemeral=True)

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
    from_pos = discord.ui.TextInput(label="Vị trí cũ", placeholder="Ví dụ: 5", required=True)
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
        if not any(song['title'] == current.title for song in favs):
            favs.append({'title': current.title, 'stream_url': current.stream_url})
            await interaction.response.send_message(f"✨ Đã lưu tối ưu: **{current.title}**", ephemeral=True)
        else:
            await interaction.response.send_message("💠 Đã có sẵn trong kho.", ephemeral=True)

    @discord.ui.button(style=discord.ButtonStyle.primary, emoji="📂", label="Xem danh sách")
    async def view_list(self, interaction: discord.Interaction, button: discord.ui.Button):
        favs = user_collections[self.user_id]
        if not favs:
            return await interaction.response.send_message("📭 Kho lưu trữ trống.", ephemeral=True)
        fav_list = "\n".join([f"` ⟡ {i+1}. ` {song['title']}" for i, song in enumerate(favs[:10])])
        embed = discord.Embed(title=f"🔮 Kho Của — {interaction.user.name}", description=fav_list, color=discord.Color.from_rgb(88, 24, 131))
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
    except Exception:
        pass

@bot.tree.command(name="play", description="🔮 Phát nhạc trực tiếp từ tên hoặc link SoundCloud")
@discord.app_commands.describe(search="Tên bài hát hoặc link")
async def play(interaction: discord.Interaction, search: str):
    if not interaction.user.voice:
        return await interaction.response.send_message("🥀 Vui lòng vào kênh thoại trước.", ephemeral=True)

    guild_id = interaction.guild.id
    if (interaction.guild.voice_client and interaction.guild.voice_client.is_playing()) or (interaction.guild.voice_client and interaction.guild.voice_client.is_paused()):
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

@bot.tree.command(name="myfavorite", description="🖤 Phát toàn bộ kho lưu trữ cá nhân (tối đa 10 bài, tải siêu tốc)")
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
            if not first_player and not ctx.voice_client.is_playing() and not ctx.voice_client.is_paused():
                first_player = player
                ctx.current_player = player
            else:
                queues[guild_id].append(player)
            added += 1
            # Thêm khoảng nghỉ cực nhỏ giúp vCPU 0.1 không bị nghẽn lệnh liên tục
            await asyncio.sleep(0.05)
        except Exception:
            continue

    if first_player:
        ctx.voice_client.play(first_player, after=lambda e: asyncio.run_coroutine_threadsafe(play_next(ctx), bot.loop))
        embed = discord.Embed(title="🖤 Đang Phát Kho Lưu Trữ", description=f"Đã nạp siêu tốc **{added} bài** từ bộ sưu tập vào hàng đợi.", color=discord.Color.from_rgb(88, 24, 131))
        await interaction.followup.send(embed=embed, view=MusicControlView(ctx))
    elif added > 0:
        await interaction.followup.send(f"🖤 Đã nạp thêm **{added} bài** từ kho vào hàng đợi.")
    else:
        await interaction.followup.send("⚠️ Không thể khởi tạo danh sách từ kho.", ephemeral=True)

@bot.tree.command(name="queue", description="📜 Xem danh sách chờ (Tối đa 10 bài)")
async def queue(interaction: discord.Interaction):
    guild_id = interaction.guild.id
    if guild_id not in queues or len(queues[guild_id]) == 0:
        return await interaction.response.send_message("📭 Hàng đợi trống.", ephemeral=True)
    
    q_list = "\n".join([f"` ⟡ {i+1}. ` {song.title}" for i, song in enumerate(queues[guild_id][:10])])
    embed = discord.Embed(title=f"⚡ Danh Sách Chờ ({len(queues[guild_id])}/10)", description=q_list, color=discord.Color.from_rgb(60, 15, 100))
    await interaction.response.send_message(embed=embed)

@bot.tree.command(name="remove", description="🗑️ Xóa một hoặc nhiều bài hát liên tiếp khỏi hàng đợi")
@discord.app_commands.describe(index="Vị trí bắt đầu xóa", count="Số lượng bài muốn xóa (mặc định là 1)")
async def remove(interaction: discord.Interaction, index: int, count: int = 1):
    guild_id = interaction.guild.id
    q = queues[guild_id]
    if not q or not (1 <= index <= len(q)):
        return await interaction.response.send_message("⚠️ Vị trí không hợp lệ.", ephemeral=True)
    
    count = max(1, min(count, len(q) - index + 1))
    removed_items = [q.pop(index - 1) for _ in range(count)]
    titles = ", ".join([item.title for item in removed_items])
    await interaction.response.send_message(f"🗑️ Đã xóa {count} bài từ vị trí `{index}`: **{titles}**")

@bot.tree.command(name="duplicate", description="🧬 Nhân bản bài đang phát (nhập 0) hoặc trong hàng chờ (Tối đa 5 lần)")
@discord.app_commands.describe(index="Nhập 0 cho bài đang phát, hoặc số thứ tự trong /queue", amount="Số bản sao muốn thêm (Tối đa 5)")
async def duplicate(interaction: discord.Interaction, index: int, amount: int):
    guild_id = interaction.guild.id
    q = queues[guild_id]
    current_vol = volumes[guild_id]
    
    if not (1 <= amount <= 5):
        return await interaction.response.send_message("⚠️ Số lượng nhân bản mỗi lần chỉ từ 1 đến 5.", ephemeral=True)
        
    if len(q) + amount > 10:
        return await interaction.response.send_message(f"⚠️ Vượt quá giới hạn hàng đợi! Tối đa 10 bài (hiện đang có {len(q)} bài).", ephemeral=True)
    
    target_source = None
    target_title = "Bài hát"

    if index == 0:
        if interaction.guild.voice_client and interaction.guild.voice_client.source:
            target_source = interaction.guild.voice_client.source
            target_title = getattr(target_source, 'title', "Bài đang phát")
        else:
            return await interaction.response.send_message("⚠️ Hiện tại không có bài hát nào đang phát.", ephemeral=True)
    else:
        if not q or not (1 <= index <= len(q)):
            return await interaction.response.send_message("⚠️ Số thứ tự trong hàng đợi không hợp lệ.", ephemeral=True)
        target_source = q[index - 1]
        target_title = target_source.title

    if not target_source:
        return await interaction.response.send_message("⚠️ Không tìm thấy nguồn dữ liệu để nhân bản.", ephemeral=True)

    await interaction.response.defer()
    try:
        cached_url = getattr(target_source, 'stream_url', None)
        for _ in range(amount):
            duplicated_player = await YTDLSource.create_source(
                target_title, 
                loop=bot.loop, 
                volume=current_vol, 
                cached_stream_url=cached_url
            )
            if index == 0:
                q.insert(0, duplicated_player)
            else:
                q.insert(index, duplicated_player)
            # Nghỉ cực ngắn giữa mỗi lần tạo bản sao để vCPU 0.1 không bị giật cục luồng phát chính
            await asyncio.sleep(0.02)
                
        await interaction.followup.send(f"🧬 Đã nhân bản thành công bài **{target_title}** thêm **{amount} lần**!")
    except Exception as e:
        await interaction.followup.send(f"⚠️ Lỗi khi nhân bản: {e}")

@bot.tree.command(name="move", description="🔄 Đổi vị trí bài hát trong hàng đợi")
@discord.app_commands.describe(from_pos="Vị trí cũ", to_pos="Vị trí mới")
async def move(interaction: discord.Interaction, from_pos: int, to_pos: int):
    guild_id = interaction.guild.id
    q = queues[guild_id]
    if not q or not (1 <= from_pos <= len(q)) or not (1 <= to_pos <= len(q)):
        return await interaction.response.send_message("⚠️ Vị trí không hợp lệ.", ephemeral=True)
    
    song = q.pop(from_pos - 1)
    q.insert(to_pos - 1, song)
    await interaction.response.send_message(f"🔄 Đã chuyển **{song.title}** từ vị trí `{from_pos}` sang `{to_pos}`.")

@bot.tree.command(name="collection", description="💼 Quản lý kho lưu trữ cá nhân")
async def collection(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    embed = discord.Embed(title="🌌 Kho Lưu Trữ Cá Nhân", description="Sử dụng bảng điều khiển bên dưới:", color=discord.Color.from_rgb(88, 24, 131))
    await interaction.followup.send(embed=embed, view=CollectionView(interaction.user.id), ephemeral=True)

@bot.tree.command(name="volume", description="🔊 Chỉnh âm lượng (1 - 100)")
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

@bot.tree.command(name="help", description="🚀 Hướng dẫn toàn bộ hệ thống lệnh")
async def help_cmd(interaction: discord.Interaction):
    embed = discord.Embed(
        title="✧ ── ✦ HƯỚNG DẪN HỆ THỐNG MUSIC LANGNGO ✦ ── ✧",
        color=discord.Color.from_rgb(88, 24, 131)
    )
    embed.add_field(name="🔮 `/play [tên/link]`", value="Phát nhạc từ SoundCloud (Hàng đợi tối đa 10 bài).", inline=False)
    embed.add_field(name="🖤 `/myfavorite`", value="Phát siêu tốc kho lưu trữ cá nhân (tối đa 10 bài).", inline=False)
    embed.add_field(name="📜 `/queue`", value="Xem danh sách chờ hiện tại.", inline=False)
    embed.add_field(name="🗑️ `/remove [vị trí] [số lượng]`", value="Xóa bài hát khỏi hàng đợi.", inline=False)
    embed.add_field(name="🧬 `/duplicate [0 / vị trí] [số lượng]`", value="Nhập `0` nhân bản bài đang nghe, hoặc số thứ tự bài chờ (Tối đa 5 lần).", inline=False)
    embed.add_field(name="🔄 `/move [cũ] [mới]`", value="Đổi vị trí bài hát trong hàng đợi.", inline=False)
    embed.add_field(name="💼 `/collection`", value="Bảng quản lý bộ sưu tập cá nhân.", inline=False)
    embed.add_field(name="🔊 `/volume [1-100]`", value="Điều chỉnh âm lượng.", inline=False)
    embed.add_field(name="🌙 `/sleep [phút]`", value="Hẹn giờ ngắt bot tự động.", inline=False)
    await interaction.response.send_message(embed=embed, ephemeral=True)

TOKEN = os.getenv("DISCORD_TOKEN")
if TOKEN:
    bot.run(TOKEN)
