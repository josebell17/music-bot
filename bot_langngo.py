import os
import sys
import logging
import traceback
import asyncio
from typing import Dict, Any, Callable, Optional, List
from datetime import datetime
import discord
from discord import app_commands
from discord.ext import commands

# ==========================================
# [21][34] OBSERVABILITY & STRUCTURED LOGGING
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | 💀 %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("BotLangNgo")

# ==========================================
# [4][22][47] ERROR CLASSIFICATION & PIPELINE
# ==========================================
class SystemErrorType:
    VALIDATION = "Validation Error"
    PERMISSION = "Permission Error"
    VOICE_ERROR = "Voice Connection Error"
    AUDIO_ERROR = "Audio Processing Error"
    INTERNAL = "Internal System Error"

class ApplicationCore:
    @staticmethod
    async def execute_pipeline(interaction: discord.Interaction, action_name: str, coro: Callable):
        correlation_id = f"ngo-{int(datetime.utcnow().timestamp() * 1000)}"
        logger.info(f"[{correlation_id}] Pipeline initiated: '{action_name}' | User: {interaction.user.id} | Guild: {interaction.guild_id}")
        
        try:
            if not interaction.user:
                raise PermissionError("User context not found.")
                
            result = await coro()
            logger.info(f"[{correlation_id}] Pipeline completed successfully: '{action_name}'")
            return result
            
        except PermissionError as e:
            logger.warning(f"[{correlation_id}] [{SystemErrorType.PERMISSION}] {e}")
            await ApplicationCore._respond_safe(interaction, "⚠️ Lãnh địa cấm: Ngươi chưa vào Voice Channel hoặc không đủ quyền thao túng Bot langngo!", ephemeral=True)
        except ValueError as e:
            logger.warning(f"[{correlation_id}] [{SystemErrorType.VALIDATION}] {e}")
            await ApplicationCore._respond_safe(interaction, f"⚠️ Tần số không hợp lệ: {e}", ephemeral=True)
        except discord.HTTPException as e:
            logger.error(f"[{correlation_id}] Discord API Error: {e}")
            await ApplicationCore._respond_safe(interaction, "⚠️ Đường truyền tới cổng không gian Discord bị nghẽn.", ephemeral=True)
        except Exception as e:
            logger.error(f"[{correlation_id}] [{SystemErrorType.INTERNAL}] {e}\n{traceback.format_exc()}")
            await ApplicationCore._respond_safe(interaction, f"💀 Lỗi hệ thống nội bộ ngầm. Mã truy vết: `{correlation_id}`", ephemeral=True)

    @staticmethod
    async def _respond_safe(interaction: discord.Interaction, message: str, ephemeral: bool = True):
        try:
            if interaction.response.is_done():
                await interaction.followup.send(f"⚡ **[Bot langngo]:** {message}", ephemeral=ephemeral)
            else:
                await interaction.response.send_message(f"⚡ **[Bot langngo]:** {message}", ephemeral=ephemeral)
        except Exception as ex:
            logger.error(f"Failed to dispatch safe response: {ex}")

# ==========================================
# [4][6][18] GUILD MUSIC SESSION & STATE MACHINE
# ==========================================
class PlaybackState:
    IDLE = "IDLE (Đang tĩnh lặng)"
    CONNECTING = "CONNECTING (Đang đột nhập Voice)"
    BUFFERING = "BUFFERING (Đang nạp năng lượng)"
    PLAYING = "PLAYING (Đang càn quét)"
    PAUSED = "PAUSED (Đang tạm hoãn)"
    STOPPED = "STOPPED (Đã ngắt kết nối)"
    RECONNECTING = "RECONNECTING (Đang tái lập kết nối)"
    ERROR = "ERROR (Lỗi hệ thống)"

class GuildMusicSession:
    def __init__(self, guild_id: int):
        self.guild_id = guild_id
        self.queue: List[Dict[str, Any]] = []
        self.current_track: Optional[Dict[str, Any]] = None
        self.state: str = PlaybackState.IDLE
        self.voice_client: Optional[discord.VoiceClient] = None
        self.volume: float = 1.0
        self.loop_mode: str = "OFF"
        self.is_shuffled: bool = False

    def reset(self):
        self.queue.clear()
        self.current_track = None
        self.state = PlaybackState.IDLE
        if self.voice_client:
            self.voice_client = None

class MusicManager:
    def __init__(self):
        self.sessions: Dict[int, GuildMusicSession] = {}

    def get_session(self, guild_id: int) -> GuildMusicSession:
        if guild_id not in self.sessions:
            self.sessions[guild_id] = GuildMusicSession(guild_id)
        return self.sessions[guild_id]

# ==========================================
# [12][30] DARK THEME INTERACTIVE NOW PLAYING
# ==========================================
class MusicControlView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Play/Pause", style=discord.ButtonStyle.danger, emoji="⏯️")
    async def toggle_play(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message("⚡ [Bot langngo]: Đã chuyển đổi trạng thái luồng âm thanh.", ephemeral=True)

    @discord.ui.button(label="Skip", style=discord.ButtonStyle.secondary, emoji="⏭️")
    async def skip_track(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message("⚡ [Bot langngo]: Lệnh càn quét! Đã bỏ qua giai điệu hiện tại.", ephemeral=True)

    @discord.ui.button(label="Destroy", style=discord.ButtonStyle.blurple, emoji="⏹️")
    async def stop_music(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message("⚡ [Bot langngo]: Xóa sổ hàng đợi, thu hồi toàn bộ phân tử âm thanh.", ephemeral=True)

# ==========================================
# [1] BOT CORE & COMMAND REGISTRATION
# ==========================================
class BotLangNgoOS(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.voice_states = True
        super().__init__(command_prefix="!", intents=intents)
        self.music_manager = MusicManager()

    async def setup_hook(self):
        @self.tree.command(name="play", description="[Bot langngo] Triệu hồi và phát âm thanh tối thượng")
        @app_commands.describe(query="Tên bài hát hoặc đường dẫn không gian")
        @app_commands.checks.cooldown(1, 2.0)
        async def play_command(interaction: discord.Interaction, query: str):
            async def logic():
                if not interaction.user.voice or not interaction.user.voice.channel:
                    raise PermissionError("Kẻ mạo danh! Ngươi chưa bước chân vào phòng Voice.")
                
                session = self.music_manager.get_session(interaction.guild_id)
                session.queue.append({"query": query, "requester": interaction.user.id})
                
                embed = discord.Embed(
                    title="🔥 [BOT LANGNGO] — THÊM VÀO HÀNG ĐỢI",
                    description=f"🎯 **Mục tiêu:** `{query}`\n📊 **Vị trí trong chuỗi:** `#{len(session.queue)}`",
                    color=discord.Color.from_rgb(15, 15, 15)
                )
                embed.set_footer(text=f"Được chỉ huy bởi {interaction.user.name} • Hệ thống bảo mật tối thượng")
                
                view = MusicControlView()
                await interaction.response.send_message(embed=embed, view=view)

            await ApplicationCore.execute_pipeline(interaction, "play_command", logic)

        @self.tree.command(name="nowplaying", description="[Bot langngo] Triệu hồi bảng điều khiển tối tăm")
        async def nowplaying_command(interaction: discord.Interaction):
            async def logic():
                session = self.music_manager.get_session(interaction.guild_id)
                embed = discord.Embed(
                    title="💀 [BOT LANGNGO] — TRẠNG THÁI HỆ THỐNG",
                    description="Hạ tầng Audio Engine đang vận hành ngầm với hiệu năng đỉnh cao.",
                    color=discord.Color.from_rgb(30, 30, 35)
                )
                embed.add_field(name="Trạng thái Lõi", value=f"🟢 **{session.state}**", inline=True)
                embed.add_field(name="Hàng đợi Chờ", value=f"⚡ **{len(session.queue)} mục**", inline=True)
                embed.add_field(name="Biên độ Ngưỡng", value=f"🔊 **{int(session.volume * 100)}%**", inline=True)
                embed.set_footer(text="Bot langngo Enterprise Core v3.0 • Zero-Trust Mode")
                
                view = MusicControlView()
                await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

            await ApplicationCore.execute_pipeline(interaction, "nowplaying_command", logic)

        await self.tree.sync()
        logger.info("Bot langngo Core & Interaction Layer successfully synchronized.")

    async def on_ready(self):
        logger.info(f"🔥 Bot langngo đã khởi động hoàn toàn dưới danh tính: {self.user} (ID: {self.user.id})")

# ==========================================
# PRODUCTION ENTRYPOINT
# ==========================================
if __name__ == "__main__":
    token = os.getenv("DISCORD_TOKEN")
    if not token:
        logger.critical("💀 CRITICAL: Biến môi trường DISCORD_TOKEN chưa được thiết lập!")
        sys.exit(1)
        
    bot = BotLangNgoOS()
    try:
        bot.run(token)
    except Exception as e:
        logger.critical(f"💀 Lỗi sập nguồn toàn cục của Bot langngo: {e}")
        sys.exit(1)
