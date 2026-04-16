import json
import asyncio
import discord
import aiohttp
import os
import time
import logging
import random
import datetime
import sys
import yt_dlp
import aiomysql
import wavelink
from discord.ext import commands, tasks
from discord import app_commands
import re

# --- DAVE PROTOCOL MONKEYPATCH (FIXES LAVALINK 4.2.2 E2EE ENCRYPTION) ---
original_request = aiohttp.ClientSession.request
original__request = aiohttp.ClientSession._request
pending_voice_channels = {}

def _resolve_channel_id_for_guild(guild_id: int):
    try:
        global bot
        guild = bot.get_guild(guild_id) if bot else None
    except Exception:
        guild = None

    if guild:
        try:
            voice_client = guild.voice_client
            if voice_client and getattr(voice_client, "channel", None):
                return str(voice_client.channel.id)
        except Exception:
            pass
        try:
            if guild.me and guild.me.voice and guild.me.voice.channel:
                return str(guild.me.voice.channel.id)
        except Exception:
            pass

    fallback = pending_voice_channels.get(guild_id)
    return str(fallback) if fallback else None

def _inject_lavalink_channel_id(method, url, kwargs):
    if str(method).upper() != 'PATCH':
        return
    try:
        payload = kwargs.get("json")
        if not isinstance(payload, dict) or "voice" not in payload: return
        voice_data = payload["voice"]
        if not isinstance(voice_data, dict) or "endpoint" not in voice_data or "channelId" in voice_data: return
        url_str = str(url)
        match = re.search(r'/players/(\d+)', url_str)
        if not match: return
        guild_id = int(match.group(1))
        resolved_channel_id = _resolve_channel_id_for_guild(guild_id)
        if resolved_channel_id:
            payload["voice"]["channelId"] = resolved_channel_id
    except Exception:
        pass

def patched_request(self, method, url, *args, **kwargs):
    _inject_lavalink_channel_id(method, url, kwargs)
    return original_request(self, method, url, *args, **kwargs)

async def patched__request(self, method, url, *args, **kwargs):
    _inject_lavalink_channel_id(method, url, kwargs)
    return await original__request(self, method, url, *args, **kwargs)

aiohttp.ClientSession.request = patched_request
aiohttp.ClientSession._request = patched__request

# --- ENHANCED ROBUST WEBHOOK DISPATCHER ---
WEBHOOK_URL = os.getenv('SYMPHONY_WEBHOOK_URL', '').strip()

async def send_webhook_log(bot_name, title, description, color, retries=3, image_url=None, fields=None):
    if not WEBHOOK_URL or WEBHOOK_URL == 'PASTE_YOUR_NEW_WEBHOOK_URL_HERE': 
        return
        
    for attempt in range(retries):
        try:
            async with HTTPSessionManager() as session:
                webhook = discord.Webhook.from_url(WEBHOOK_URL, session=session)
                embed = discord.Embed(title=title, description=description, color=color, timestamp=discord.utils.utcnow())
                embed.set_footer(text=f"Swarm Network Matrix")
                if image_url: embed.set_thumbnail(url=image_url)
                if fields:
                    for name, value, inline in fields:
                        embed.add_field(name=name, value=value, inline=inline)

                await webhook.send(embed=embed, username=f"Node: {bot_name.capitalize()}")
                return
        except discord.errors.NotFound:
            logger.error("❌ WEBHOOK KILLED: Discord deleted your webhook. Create a new one.")
            return
        except Exception as e:
            if attempt < retries - 1: await asyncio.sleep(2 ** attempt)
            else: logger.error(f"❌ Webhook Dispatch Failed: {e}")

class DBPoolManager:
    _pool = None
    async def __aenter__(self):
        if not DBPoolManager._pool:
            DBPoolManager._pool = await aiomysql.create_pool(**DB_CONFIG)
        return DBPoolManager._pool
    async def __aexit__(self, exc_type, exc_val, exc_tb): pass

class HTTPSessionManager:
    _session = None
    async def __aenter__(self):
        if not HTTPSessionManager._session:
            HTTPSessionManager._session = aiohttp.ClientSession()
        return HTTPSessionManager._session
    async def __aexit__(self, exc_type, exc_val, exc_tb): pass

# --- LOGGING SETUP ---
file_handler = logging.FileHandler(filename="discord_bot.log", encoding="utf-8", mode="a")
discord.utils.setup_logging(handler=file_handler, level=logging.INFO)
logger = logging.getLogger("discord")

# --- CONFIGURATION ---
BOT_ENV_PREFIX = "SYMPHONY"
TOKEN = os.getenv(f"{BOT_ENV_PREFIX}_DISCORD_TOKEN", "").strip()
DB_CONFIG = {
    'host': os.getenv(f"{BOT_ENV_PREFIX}_DB_HOST", "127.0.0.1"),
    'user': os.getenv(f"{BOT_ENV_PREFIX}_DB_USER", "botuser"),
    'password': os.getenv(f"{BOT_ENV_PREFIX}_DB_PASSWORD", ""),
    'db': os.getenv(f"{BOT_ENV_PREFIX}_DB_NAME", "discord_music_symphony"),
    'autocommit': True
}
LAVALINK_URI = os.getenv(f"{BOT_ENV_PREFIX}_LAVALINK_URI", "http://127.0.0.1:2333")
LAVALINK_PASSWORD = os.getenv(f"{BOT_ENV_PREFIX}_LAVALINK_PASSWORD", "")

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="/", intents=intents)
bot.start_time = time.time()
playback_tracking = {}
guild_states = {}
auto_heal_initialized = False
recovering_guilds = set()

ytdl_format_options = {
    'format': 'bestaudio/best', 'outtmpl': '%(extractor)s-%(id)s-%(title)s.%(ext)s',
    'restrictfilenames': True, 'noplaylist': True, 'nocheckcertificate': True,
    'ignoreerrors': True, 'logtostderr': False, 'quiet': True,
    'no_warnings': True, 'default_search': 'auto', 'source_address': '0.0.0.0'
}

# --- DATABASE INITIALIZATION ---
async def init_db():
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("CREATE TABLE IF NOT EXISTS symphony_playback_state (guild_id BIGINT, bot_name VARCHAR(50), channel_id BIGINT, video_url TEXT, position_seconds INT DEFAULT 0, is_playing BOOLEAN DEFAULT FALSE, title TEXT, PRIMARY KEY (guild_id, bot_name))")
                try: await cur.execute("ALTER TABLE symphony_playback_state ADD COLUMN title TEXT")
                except: pass
                await cur.execute("CREATE TABLE IF NOT EXISTS symphony_guild_settings (guild_id BIGINT PRIMARY KEY, home_vc_id BIGINT, volume INT DEFAULT 100, loop_mode VARCHAR(10) DEFAULT 'off', filter_mode VARCHAR(20) DEFAULT 'none', dj_role_id BIGINT DEFAULT NULL, feedback_channel_id BIGINT DEFAULT NULL, transition_mode VARCHAR(10) DEFAULT 'off', custom_speed FLOAT DEFAULT 1.0, custom_pitch FLOAT DEFAULT 1.0, custom_modifiers_left INT DEFAULT 0, dj_only_mode BOOLEAN DEFAULT FALSE, stay_in_vc BOOLEAN DEFAULT FALSE)")
                await cur.execute("CREATE TABLE IF NOT EXISTS symphony_queue (id INT AUTO_INCREMENT PRIMARY KEY, guild_id BIGINT, bot_name VARCHAR(50), video_url TEXT, title TEXT, requester_id BIGINT DEFAULT NULL)")
                await cur.execute("CREATE TABLE IF NOT EXISTS symphony_history (id INT AUTO_INCREMENT PRIMARY KEY, guild_id BIGINT, video_url TEXT, title TEXT, played_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, requester_id BIGINT DEFAULT NULL)")
                await cur.execute("CREATE TABLE IF NOT EXISTS symphony_user_playlists (id INT AUTO_INCREMENT PRIMARY KEY, user_id BIGINT, playlist_name VARCHAR(255), video_url TEXT, title TEXT)")
                logger.info("Database tables verified/created for SYMPHONY.")

# --- CORE LOGIC & HELPERS ---
async def save_state(guild_id):
    state = guild_states.get(guild_id)
    if not state: return
    try:
        with open(f"state_{guild_id}.json", "w") as f: json.dump(state, f)
    except: pass

async def load_states():
    states = {}
    for file in os.listdir():
        if file.startswith("state_") and file.endswith(".json"):
            try:
                gid = file.replace("state_", "").replace(".json", "")
                with open(file) as f: states[gid] = json.load(f)
            except: pass
    return states

async def delete_state(guild_id):
    try:
        os.remove(f"state_{guild_id}.json")
    except FileNotFoundError:
        pass
    except Exception:
        pass

async def ensure_guild_settings(guild_id):
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("INSERT IGNORE INTO symphony_guild_settings (guild_id) VALUES (%s)", (guild_id,))

def _scalar_from_row(row, default=0):
    if row is None:
        return default
    if isinstance(row, dict):
        return next(iter(row.values()), default)
    if isinstance(row, (tuple, list)):
        return row[0] if row else default
    return row

async def insert_queue_front(cur, table_name, guild_id, bot_name, video_url, title, requester_id, max_attempts=5):
    if not re.fullmatch(r"[A-Za-z0-9_]+", table_name):
        raise ValueError(f"Unsafe table name: {table_name}")

    for attempt in range(max_attempts):
        await cur.execute(f"SELECT COALESCE(MIN(id), 0) AS min_id FROM {table_name}")
        min_row = await cur.fetchone()
        new_id = (_scalar_from_row(min_row, 0) or 0) - 1
        try:
            await cur.execute(
                f"INSERT INTO {table_name} (id, guild_id, bot_name, video_url, title, requester_id) VALUES (%s, %s, %s, %s, %s, %s)",
                (new_id, guild_id, bot_name, video_url, title, requester_id)
            )
            return new_id
        except aiomysql.IntegrityError as e:
            if e.args and e.args[0] == 1062 and attempt < max_attempts - 1:
                await asyncio.sleep(0.05 * (attempt + 1))
                continue
            raise

async def get_home_channel(guild):
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("CREATE TABLE IF NOT EXISTS symphony_bot_home_channels (guild_id BIGINT, bot_name VARCHAR(50), home_vc_id BIGINT, PRIMARY KEY (guild_id, bot_name))")
                await cur.execute("SELECT home_vc_id FROM symphony_bot_home_channels WHERE guild_id = %s AND bot_name = 'symphony'", (guild.id,))
                res = await cur.fetchone()
    if res and res[0]:
        return guild.get_channel(res[0])
    return None

async def _fade_volume(voice_client, start_volume, end_volume, duration=5.0, steps=10):
    if not voice_client:
        return
    step_delay = duration / steps if steps > 0 else duration
    for step in range(steps + 1):
        volume = int(round(start_volume + (end_volume - start_volume) * (step / steps)))
        try:
            await voice_client.set_volume(max(0, min(200, volume)))
        except Exception:
            return
        if step < steps:
            await asyncio.sleep(step_delay)

async def update_stage_topic(guild, title, requester_id):
    try:
        vc = guild.voice_client
        if not vc or not getattr(vc, "channel", None): return
        channel = vc.channel
        
        # FIX: Explicit instance check for Stage channels
        if not isinstance(channel, discord.StageChannel): return

        requester_name = f"<@{requester_id}>" if requester_id else "Unknown User"
        topic = f"🎵 {title[:60]} | 👤 Req: {requester_name}"

        # FIX: Must use StageInstance to control topics
        if channel.instance is None:
            await channel.create_instance(topic=topic)
        else:
            await channel.instance.edit(topic=topic)
    except Exception as e:
        logger.error(f"[STAGE ERROR] {e}")

async def send_feedback(guild, embed):
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT feedback_channel_id FROM symphony_guild_settings WHERE guild_id = %s", (guild.id,))
                res = await cur.fetchone()
                if res and res[0]:
                    channel = guild.get_channel(res[0])
                    if channel:
                        try: await channel.send(embed=embed)
                        except discord.Forbidden: pass

async def ensure_voice_connection(guild, channel_id):
    channel = guild.get_channel(channel_id)
    if not channel: return None
    voice_client = guild.voice_client
    pending_voice_channels[guild.id] = channel_id
    try:
        if not voice_client: 
            voice_client = await channel.connect(cls=wavelink.Player, timeout=60.0)
        elif voice_client.channel.id != channel_id: 
            await voice_client.move_to(channel)
        if getattr(voice_client, "channel", None):
            pending_voice_channels[guild.id] = voice_client.channel.id
        
        if isinstance(channel, discord.StageChannel):
            if guild.me.voice and guild.me.voice.suppress:
                try: await guild.me.edit(suppress=False)
                except Exception: pass
                
        return voice_client
    except Exception as e:
        logger.error(f"[{guild.id}] Voice connect error: {e}")
        return None

async def is_dj(interaction: discord.Interaction, silent=False):
    if interaction.user.guild_permissions.administrator: return True
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT dj_role_id, dj_only_mode FROM symphony_guild_settings WHERE guild_id = %s", (interaction.guild.id,))
                res = await cur.fetchone()
                if res and res[1]: 
                    if res[0] and discord.utils.get(interaction.user.roles, id=res[0]): return True
                    if not silent:
                        await interaction.response.send_message(embed=discord.Embed(description="❌ **Strict DJ Mode is Active.** You need the DJ Role.", color=discord.Color.red()), ephemeral=True)
                    return False
    return True

def make_progress_bar(current, total, length=15):
    if total <= 0: return f"[{'▬'*length}] {current//60}:{current%60:02d} / Live"
    progress = max(0, min(length, int((current / total) * length)))
    bar = "▬" * progress + "🔘" + "▬" * (length - progress - 1)
    return f"[{bar}] {current//60}:{current%60:02d} / {total//60}:{total%60:02d}"

def _has_human_listeners(voice_client):
    if not voice_client or not getattr(voice_client, "channel", None): return False
    return any(not member.bot for member in voice_client.channel.members)

def _should_auto_disconnect(guild, stay_in_vc=False):
    if stay_in_vc: return False
    return not _has_human_listeners(guild.voice_client)

@bot.event
async def on_wavelink_track_end(payload: wavelink.TrackEndEventPayload):
    # FIX: Properly cast reason to upper string to handle API changes safely
    if payload.player and str(getattr(payload, 'reason', '')).upper() != "REPLACED":
        try:
            reason = str(payload.reason).upper()
            if payload.track:
                async with DBPoolManager() as pool:
                    async with pool.acquire() as conn:
                        async with conn.cursor() as cur:
                            await cur.execute("SELECT loop_mode FROM symphony_guild_settings WHERE guild_id = %s", (payload.player.guild.id,))
                            mode_row = await cur.fetchone()
                            loop_mode = mode_row[0] if mode_row else 'off'
                            track_data = playback_tracking.get(payload.player.guild.id, {})
                            original_requester = track_data.get('requester_id', bot.user.id if bot.user else None)

                            if reason == "FINISHED":
                                if loop_mode == 'queue':
                                    await cur.execute("INSERT INTO symphony_queue (guild_id, bot_name, video_url, title, requester_id) VALUES (%s, 'symphony', %s, %s, %s)", (payload.player.guild.id, payload.track.uri, payload.track.title, original_requester))
                                elif loop_mode == 'song':
                                    await insert_queue_front(cur, "symphony_queue", payload.player.guild.id, "symphony", payload.track.uri, payload.track.title, original_requester)
        except Exception as e:
            logger.error(f"[{payload.player.guild.id}] Looping logic DB error: {e}")
        coro = process_queue(payload.player.guild, payload.player.channel.id)
        bot.loop.create_task(coro)

async def process_queue(guild, channel_id, start_position=0):
    recovering_guilds.discard(guild.id)
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT volume, loop_mode, filter_mode, transition_mode, custom_speed, custom_pitch, custom_modifiers_left, stay_in_vc FROM symphony_guild_settings WHERE guild_id = %s", (guild.id,))
                res = await cur.fetchone()
                vol, loop_mode, filter_mode, trans_mode, c_speed, c_pitch, c_mod_left, stay_in_vc = res if res else (100, 'off', 'none', 'off', 1.0, 1.0, 0, False)
                
                await cur.execute("SELECT id, video_url, title, requester_id FROM symphony_queue WHERE guild_id = %s AND bot_name = 'symphony' ORDER BY id ASC LIMIT 1", (guild.id,))
                next_song = await cur.fetchone()

                if not next_song:
                    await cur.execute("UPDATE symphony_playback_state SET is_playing = FALSE WHERE guild_id = %s AND bot_name = 'symphony'", (guild.id,))
                    playback_tracking.pop(guild.id, None)
                    guild_states.pop(guild.id, None)
                    await delete_state(guild.id)
                    await bot.change_presence(status=discord.Status.online)
                    
                    try:
                        async with DBPoolManager() as dj_pool:
                            async with dj_pool.acquire() as dj_conn:
                                async with dj_conn.cursor(aiomysql.DictCursor) as dj_cur:
                                    await dj_cur.execute("CREATE TABLE IF NOT EXISTS discord_music.swarm_toggles (guild_id BIGINT PRIMARY KEY, auto_dj BOOLEAN DEFAULT FALSE, audio_filter VARCHAR(20) DEFAULT 'normal')")
                                    await dj_cur.execute("SELECT auto_dj FROM discord_music.swarm_toggles WHERE guild_id = %s", (guild.id,))
                                    dj_res = await dj_cur.fetchone()
                                    if dj_res and dj_res.get('auto_dj'):
                                        await dj_cur.execute("SELECT genre FROM discord_music.user_music_tastes ORDER BY RAND() LIMIT 1")
                                        g_res = await dj_cur.fetchone()
                                        genre = g_res['genre'] if g_res else "lofi hip hop"
                                        await dj_cur.execute("INSERT INTO symphony_queue (guild_id, bot_name, video_url, title, requester_id) VALUES (%s, 'symphony', %s, %s, %s)", (guild.id, f"ytsearch:{genre} track", f"📻 Auto-DJ: {genre}", bot.user.id))
                                        bot.loop.create_task(process_queue(guild, channel_id))
                                        return
                    except Exception: pass

                    if _should_auto_disconnect(guild, stay_in_vc) and guild.voice_client:
                        await guild.voice_client.disconnect()
                    return

                song_id, url, title, requester_id = next_song
                await cur.execute("DELETE FROM symphony_queue WHERE id = %s", (song_id,))
                await cur.execute("INSERT INTO symphony_history (guild_id, video_url, title, requester_id) VALUES (%s, %s, %s, %s)", (guild.id, url, title, requester_id))

                try:
                    tracks = await wavelink.Playable.search(url)
                    if not tracks: raise ValueError("No stream found.")
                    track = tracks[0] if isinstance(tracks, list) else tracks
                    if isinstance(tracks, wavelink.Playlist): track = tracks.tracks[0]
                    duration = track.length / 1000
                    uploader = track.author
                except Exception as e:
                    logger.error(f"[{guild.id}] Lavalink search failed for '{title}': {e}")
                    await insert_queue_front(cur, "symphony_queue", guild.id, "symphony", url, title, requester_id)
                    await asyncio.sleep(5)
                    bot.loop.create_task(process_queue(guild, channel_id))
                    return

                voice_client = await ensure_voice_connection(guild, channel_id)
                if not voice_client: return

                wav_filters = wavelink.Filters()
                await voice_client.set_volume(vol)
                
                if c_mod_left > 0:
                    wav_filters.timescale.set(speed=c_speed, pitch=c_pitch)
                    c_mod_left -= 1
                    await cur.execute("UPDATE symphony_guild_settings SET custom_modifiers_left = %s WHERE guild_id = %s", (c_mod_left, guild.id))
                    if c_mod_left == 0: await cur.execute("UPDATE symphony_guild_settings SET custom_speed = 1.0, custom_pitch = 1.0 WHERE guild_id = %s", (guild.id,))

                if filter_mode == 'nightcore':
                    wav_filters.timescale.set(speed=1.25, pitch=1.3)
                    c_speed = 1.25
                elif filter_mode == 'vaporwave':
                    wav_filters.timescale.set(speed=0.8, pitch=0.8)
                    c_speed = 0.8
                elif filter_mode == 'bassboost':
                    wav_filters.equalizer.set(bands=[(0, 0.3), (1, 0.2), (2, 0.1)])

                await voice_client.set_filters(wav_filters)
                
                if trans_mode == 'fade' and start_position <= 0:
                    await voice_client.set_volume(0)

                await voice_client.play(track)
                if start_position > 0:
                    await voice_client.seek(int(start_position * 1000))
                elif trans_mode == 'fade':
                    bot.loop.create_task(_fade_volume(voice_client, 0, vol))

                # FIX: Execute auto-stage updater
                bot.loop.create_task(update_stage_topic(guild, title, requester_id))

                await bot.change_presence(activity=discord.Activity(type=discord.ActivityType.listening, name=title))
                playback_tracking[guild.id] = {'start_time': time.time(), 'offset': start_position, 'url': url, 'channel_id': channel_id, 'title': title, 'duration': duration, 'speed': c_speed, 'current_filter': filter_mode, 'requester_id': requester_id, 'transition_mode': trans_mode, 'volume': vol}
                
                # Update persistent state
                guild_states[guild.id] = {"voice_channel_id": channel_id, "position": start_position}
                await save_state(guild.id)

                embed = discord.Embed(title="🎵 Now Playing", description=f"**[{title}]({url})**\n*By: {uploader}*", color=discord.Color.from_rgb(88, 101, 242))
                if requester_id: embed.add_field(name="Requested by", value=f"<@{requester_id}>", inline=True)
                await send_feedback(guild, embed)

async def stop_playback(guild):
    if guild.voice_client: await guild.voice_client.disconnect()
    playback_tracking.pop(guild.id, None)
    guild_states.pop(guild.id, None)
    recovering_guilds.discard(guild.id)
    await delete_state(guild.id)
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("UPDATE symphony_playback_state SET is_playing = FALSE WHERE guild_id = %s AND bot_name = 'symphony'", (guild.id,))
                await cur.execute("DELETE FROM symphony_queue WHERE guild_id = %s AND bot_name = 'symphony'", (guild.id,))
    await bot.change_presence(status=discord.Status.online)

async def restore_guild_state(guild_id, state):
    target_guild_id = int(guild_id)
    if target_guild_id in recovering_guilds or target_guild_id in playback_tracking:
        return
    recovering_guilds.add(target_guild_id)
    handoff = False
    # FIX: Rewritten to use native systems safely rather than broken logic
    try:
        guild = bot.get_guild(target_guild_id)
        if not guild: return
        vc_id = state.get("voice_channel_id")
        if not vc_id: return
        channel = guild.get_channel(vc_id)
        if not channel: return
        
        vc = await ensure_voice_connection(guild, vc_id)
        if not vc: return
        
        bot.loop.create_task(process_queue(guild, vc_id, start_position=state.get("position", 0)))
        handoff = True
    except Exception as e:
        logger.error(f"[RESTORE ERROR] {guild_id}: {e}")
    finally:
        if not handoff:
            recovering_guilds.discard(target_guild_id)

@tasks.loop(minutes=2.0)
async def auto_heal_loop():
    global auto_heal_initialized

    if not auto_heal_initialized:
        states = await load_states()
        for gid, state in states.items():
            asyncio.create_task(restore_guild_state(gid, state))
        auto_heal_initialized = True

    for gid, state in list(guild_states.items()):
        try:
            guild = bot.get_guild(int(gid))
            if guild:
                vc = guild.voice_client
                if not vc or not vc.is_connected():
                    logger.info(f"[HEAL] Rejoining {gid}")
                    asyncio.create_task(restore_guild_state(gid, state))
        except Exception:
            pass

# --- BOT EVENTS & LAVALINK CONNECTION ---
@bot.event
async def setup_hook():
    await init_db()

@bot.event
async def on_wavelink_node_ready(payload: wavelink.NodeReadyEventPayload):
    logger.info(f"🔥 Lavalink Bridge Officially Connected and Locked! (Node: {payload.node.identifier})")
    logger.info("Checking for orphaned playback states to auto-resume...")
    try:
        async with DBPoolManager() as pool:
            async with pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    await cur.execute("SELECT guild_id, channel_id, position_seconds, video_url, title FROM symphony_playback_state WHERE is_playing = TRUE AND bot_name = 'symphony'")
                    orphans = await cur.fetchall()
                    for orphan in orphans:
                        guild = bot.get_guild(orphan['guild_id'])
                        if guild:
                            if guild.id in recovering_guilds or guild.id in playback_tracking or guild.voice_client:
                                continue
                            recovering_guilds.add(guild.id)
                            await insert_queue_front(cur, "symphony_queue", guild.id, "symphony", orphan['video_url'], orphan.get('title', 'Resumed Track'), bot.user.id)
                            bot.loop.create_task(process_queue(guild, orphan['channel_id'], start_position=orphan['position_seconds']))
    except Exception as e:
        logger.error(f"Auto-resume error: {e}")

@bot.event
async def on_wavelink_node_closed(node: wavelink.Node, disconnected):
    logger.warning(f"⚠️ Lavalink Connection Lost! Native self-healing activated...")

@bot.event
async def on_ready():
    logger.info(f'Logged in as {bot.user}')
    await bot.tree.sync()
    if not position_updater.is_running(): position_updater.start()
    
    async def connect_lavalink():
        await bot.wait_until_ready()
        nodes = [wavelink.Node(uri=LAVALINK_URI, password=LAVALINK_PASSWORD)]
        while True:
            try:
                await wavelink.Pool.connect(nodes=nodes, client=bot, cache_capacity=100)
                break
            except Exception:
                logger.warning(f"Waiting for Lavalink to boot... Retrying in 5s")
                await asyncio.sleep(5)
                
    bot.loop.create_task(connect_lavalink())

@bot.event
async def on_voice_state_update(member, before, after):
    if member == bot.user and before.channel is not None and after.channel is None:
        playback_tracking.pop(member.guild.id, None)
        guild_states.pop(member.guild.id, None)
        recovering_guilds.discard(member.guild.id)
        await delete_state(member.guild.id)
        pending_voice_channels.pop(member.guild.id, None)

@tasks.loop(seconds=5.0)
async def position_updater():
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                for guild_id, data in list(playback_tracking.items()):
                    guild = bot.get_guild(guild_id)
                    if guild and guild.voice_client and getattr(guild.voice_client, 'playing', False):
                        pos = int((time.time() - data['start_time']) * data.get('speed', 1.0) + data['offset'])
                        await cur.execute("REPLACE INTO symphony_playback_state (guild_id, bot_name, channel_id, video_url, position_seconds, is_playing, title) VALUES (%s, 'symphony', %s, %s, %s, TRUE, %s)", (guild_id, data['channel_id'], data['url'], pos, data['title']))

# --- SETTINGS COMMANDS ---
@bot.tree.command(name="symphony_main_sethome", description="Set bot's default voice/stage channel")
@commands.has_permissions(administrator=True)
async def sethome(interaction: discord.Interaction, channel: discord.VoiceChannel | discord.StageChannel):
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("CREATE TABLE IF NOT EXISTS symphony_bot_home_channels (guild_id BIGINT, bot_name VARCHAR(50), home_vc_id BIGINT, PRIMARY KEY (guild_id, bot_name))")
                await cur.execute("REPLACE INTO symphony_bot_home_channels (guild_id, bot_name, home_vc_id) VALUES (%s, %s, %s)", (interaction.guild.id, 'symphony', channel.id))
    await interaction.response.send_message(embed=discord.Embed(title="🏠 Home Set", description=f"Home channel set to {channel.mention}.", color=discord.Color.green()), ephemeral=True)

@bot.tree.command(name="symphony_main_setfeedback", description="Set the text channel for bot announcements")
@commands.has_permissions(administrator=True)
async def setfeedback(interaction: discord.Interaction, channel: discord.TextChannel):
    await ensure_guild_settings(interaction.guild.id)
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("UPDATE symphony_guild_settings SET feedback_channel_id = %s WHERE guild_id = %s", (channel.id, interaction.guild.id))
    await interaction.response.send_message(embed=discord.Embed(title="✅ Feedback Channel Set", description=f"Updates will be sent to {channel.mention}.", color=discord.Color.green()), ephemeral=True)

@bot.tree.command(name="symphony_main_djrole", description="Set DJ Role (Admins)")
@commands.has_permissions(administrator=True)
async def djrole(interaction: discord.Interaction, role: discord.Role):
    await ensure_guild_settings(interaction.guild.id)
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("UPDATE symphony_guild_settings SET dj_role_id = %s WHERE guild_id = %s", (role.id, interaction.guild.id))
    await interaction.response.send_message(embed=discord.Embed(description=f"🎧 DJ role set to {role.mention}", color=discord.Color.green()), ephemeral=True)

@bot.tree.command(name="symphony_main_removedj", description="Remove DJ Role")
@commands.has_permissions(administrator=True)
async def removedj(interaction: discord.Interaction):
    await ensure_guild_settings(interaction.guild.id)
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("UPDATE symphony_guild_settings SET dj_role_id = NULL WHERE guild_id = %s", (interaction.guild.id,))
    await interaction.response.send_message(embed=discord.Embed(description="DJ role requirements removed.", color=discord.Color.green()), ephemeral=True)

@bot.tree.command(name="symphony_main_djmode", description="Toggle Strict DJ Mode")
@commands.has_permissions(administrator=True)
async def toggle_djmode(interaction: discord.Interaction):
    await ensure_guild_settings(interaction.guild.id)
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT dj_only_mode FROM symphony_guild_settings WHERE guild_id = %s", (interaction.guild.id,))
                res = await cur.fetchone()
                new_val = not res[0] if res else True
                await cur.execute("UPDATE symphony_guild_settings SET dj_only_mode = %s WHERE guild_id = %s", (new_val, interaction.guild.id))
    state = "ENABLED" if new_val else "DISABLED"
    await interaction.response.send_message(embed=discord.Embed(description=f"🎧 Strict DJ Mode is now **{state}**.", color=discord.Color.green()), ephemeral=True)

@bot.tree.command(name="symphony_main_247", description="Toggle 24/7 Mode")
@commands.has_permissions(administrator=True)
async def toggle_247(interaction: discord.Interaction):
    await ensure_guild_settings(interaction.guild.id)
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT stay_in_vc FROM symphony_guild_settings WHERE guild_id = %s", (interaction.guild.id,))
                res = await cur.fetchone()
                new_val = not res[0] if res else True
                await cur.execute("UPDATE symphony_guild_settings SET stay_in_vc = %s WHERE guild_id = %s", (new_val, interaction.guild.id))
    state = "ENABLED" if new_val else "DISABLED"
    await interaction.response.send_message(embed=discord.Embed(description=f"🕰️ 24/7 Mode is now **{state}**.", color=discord.Color.green()), ephemeral=True)

@bot.tree.command(name="symphony_main_restart", description="Forcefully restart the bot instance (Admins only)")
@commands.has_permissions(administrator=True)
async def restart_bot(interaction: discord.Interaction):
    await interaction.response.send_message("Restarting...", ephemeral=True)
    await bot.close()
    sys.exit(0)

# --- PLAYBACK COMMANDS ---
@bot.tree.command(name="symphony_main_play", description="Play a song, link, livestream, or YouTube playlist")
async def play(interaction: discord.Interaction, search: str):
    interaction_token_valid = True
    try:
        await interaction.response.defer()
    except discord.NotFound: interaction_token_valid = False
    except discord.InteractionResponded: interaction_token_valid = True
    except Exception: interaction_token_valid = False

    async def send_play_feedback(embed: discord.Embed):
        if interaction_token_valid:
            try: return await interaction.followup.send(embed=embed)
            except Exception: pass
        if interaction.channel:
            try: return await interaction.channel.send(embed=embed)
            except Exception: pass
        return None

    # FIX: Priority #1 is the home channel, not just a fallback.
    channel = None
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("CREATE TABLE IF NOT EXISTS symphony_bot_home_channels (guild_id BIGINT, bot_name VARCHAR(50), home_vc_id BIGINT, PRIMARY KEY (guild_id, bot_name))")
                await cur.execute("SELECT home_vc_id FROM symphony_bot_home_channels WHERE guild_id = %s AND bot_name = 'symphony'", (interaction.guild.id,))
                res = await cur.fetchone()
                if res and res[0]: channel = interaction.guild.get_channel(res[0])
                    
    if not channel: 
        channel = interaction.user.voice.channel if interaction.user.voice else None

    if not channel: 
        await send_play_feedback(discord.Embed(title="❌ Error", description="Join a channel first or set a home channel.", color=discord.Color.red()))
        return

    try:
        tracks = await wavelink.Playable.search(search)
        if not tracks: raise Exception("Private or unavailable.")
    except Exception as e:
        await send_play_feedback(discord.Embed(title="❌ Error", description=str(e), color=discord.Color.red()))
        return
    
    entries_to_add = tracks.tracks if isinstance(tracks, wavelink.Playlist) else [tracks[0] if isinstance(tracks, list) else tracks]
    is_playlist_request = isinstance(tracks, wavelink.Playlist) or ('list=' in search and len(entries_to_add) > 1)
    playlist_url = resolve_playlist_source(search, tracks if isinstance(tracks, wavelink.Playlist) else None) if is_playlist_request else None
    added_count = 0

    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                for track in entries_to_add:
                    await cur.execute("INSERT INTO symphony_queue (guild_id, bot_name, video_url, title, requester_id) VALUES (%s, 'symphony', %s, %s, %s)", (interaction.guild.id, track.uri, track.title, interaction.user.id))
                    added_count += 1
                await cur.execute("SELECT COUNT(*) FROM symphony_queue WHERE guild_id = %s AND bot_name = 'symphony'", (interaction.guild.id,))
                q_len = (await cur.fetchone())[0]
    if playlist_url:
        await set_active_playlist(interaction.guild.id, playlist_url, len(entries_to_add), interaction.user.id, channel.id)

    vc = interaction.guild.voice_client
    if not vc or (not getattr(vc, 'playing', False) and not getattr(vc, 'paused', False)):
        await send_play_feedback(discord.Embed(title="🎶 Queued & Starting", description=f"Added **{added_count}** tracks. Starting Lavalink Engine!", color=discord.Color.green()))
        await process_queue(interaction.guild, channel.id)
    else:
        await send_play_feedback(discord.Embed(title="📥 Added to Queue", description=f"Added **{added_count}** tracks. (Queue size: {q_len})", color=discord.Color.blue()))

@bot.tree.command(name="symphony_main_playnext", description="Put song at top of queue")
async def playnext(interaction: discord.Interaction, search: str):
    if not await is_dj(interaction): return
    await interaction.response.defer()
    
    try:
        tracks = await wavelink.Playable.search(search)
        if not tracks: raise Exception("Track could not be found.")
        track = tracks.tracks[0] if isinstance(tracks, wavelink.Playlist) else (tracks[0] if isinstance(tracks, list) else tracks)
    except Exception as e:
        return await interaction.followup.send(embed=discord.Embed(description=f"Error resolving track: {e}", color=discord.Color.red()))
        
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT id, video_url, title, requester_id FROM symphony_queue WHERE guild_id = %s AND bot_name = 'symphony' ORDER BY id ASC", (interaction.guild.id,))
                q = await cur.fetchall()
                await cur.execute("DELETE FROM symphony_queue WHERE guild_id = %s AND bot_name = 'symphony'", (interaction.guild.id,))
                await cur.execute("INSERT INTO symphony_queue (guild_id, bot_name, video_url, title, requester_id) VALUES (%s, 'symphony', %s, %s, %s)", (interaction.guild.id, track.uri, track.title, interaction.user.id))
                for r in q: await cur.execute("INSERT INTO symphony_queue (guild_id, bot_name, video_url, title, requester_id) VALUES (%s, 'symphony', %s, %s, %s)", (interaction.guild.id, r[1], r[2], r[3]))
    vc = interaction.guild.voice_client
    if not vc or (not getattr(vc, 'playing', False) and not getattr(vc, 'paused', False)):
        channel = vc.channel if vc and getattr(vc, 'channel', None) else await get_home_channel(interaction.guild)
        if not channel:
            channel = interaction.user.voice.channel if interaction.user.voice else None
        if channel:
            await process_queue(interaction.guild, channel.id)
    await interaction.followup.send(embed=discord.Embed(description=f"**Playing next:** {track.title}", color=discord.Color.green()))

@bot.tree.command(name="symphony_main_skip", description="Skip current song")
async def skip(interaction: discord.Interaction):
    if not await is_dj(interaction): return
    # FIX: Account for silent failures when nothing is playing
    if interaction.guild.voice_client and (getattr(interaction.guild.voice_client, 'playing', False) or getattr(interaction.guild.voice_client, 'paused', False)):
        await interaction.guild.voice_client.stop()
        await interaction.response.send_message(embed=discord.Embed(description="⏭️ Skipped", color=discord.Color.blurple()), ephemeral=True)
    else:
        await interaction.response.send_message(embed=discord.Embed(description="❌ Nothing is playing.", color=discord.Color.red()), ephemeral=True)

@bot.tree.command(name="symphony_main_stop", description="Stop music and clear state")
async def stop(interaction: discord.Interaction):
    if not await is_dj(interaction): return
    await stop_playback(interaction.guild)
    await clear_active_playlist(interaction.guild.id)
    await interaction.response.send_message(embed=discord.Embed(title="⏹️ Stopped", description="Music stopped and cleared.", color=discord.Color.red()), ephemeral=True)

@bot.tree.command(name="symphony_main_pause", description="Pause playback")
async def pause(interaction: discord.Interaction):
    if not await is_dj(interaction): return
    if interaction.guild.voice_client and getattr(interaction.guild.voice_client, 'playing', False):
        await interaction.guild.voice_client.pause(True)
        await interaction.response.send_message(embed=discord.Embed(description="⏸️ Paused", color=discord.Color.blue()), ephemeral=True)
    else:
        await interaction.response.send_message(embed=discord.Embed(description="❌ Nothing is currently playing.", color=discord.Color.red()), ephemeral=True)

@bot.tree.command(name="symphony_main_resume", description="Resume playback")
async def resume(interaction: discord.Interaction):
    if not await is_dj(interaction): return
    if interaction.guild.voice_client and getattr(interaction.guild.voice_client, 'paused', False):
        await interaction.guild.voice_client.pause(False)
        await interaction.response.send_message(embed=discord.Embed(description="▶️ Resumed", color=discord.Color.green()), ephemeral=True)
    else:
        await interaction.response.send_message(embed=discord.Embed(description="❌ Nothing is currently paused.", color=discord.Color.red()), ephemeral=True)

@bot.tree.command(name="symphony_main_clear", description="Clear the queue")
async def clear(interaction: discord.Interaction):
    if not await is_dj(interaction): return
    vc = interaction.guild.voice_client
    if vc and (getattr(vc, "playing", False) or getattr(vc, "paused", False)):
        try: await vc.stop()
        except Exception: pass
    playback_tracking.pop(interaction.guild.id, None)
    guild_states.pop(interaction.guild.id, None)
    await delete_state(interaction.guild.id)
    await clear_active_playlist(interaction.guild.id)
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("DELETE FROM symphony_queue WHERE guild_id = %s AND bot_name = 'symphony'", (interaction.guild.id,))
                await cur.execute("UPDATE symphony_playback_state SET is_playing = FALSE, position_seconds = 0 WHERE guild_id = %s AND bot_name = 'symphony'", (interaction.guild.id,))
    await bot.change_presence(status=discord.Status.online)
    await interaction.response.send_message(embed=discord.Embed(description="🗑️ Playback stopped and queue cleared.", color=discord.Color.red()), ephemeral=True)

@bot.tree.command(name="symphony_main_join", description="Force bot to join your channel")
async def join(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    # FIX: Prioritize home channel over user channel globally
    channel = None
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("CREATE TABLE IF NOT EXISTS symphony_bot_home_channels (guild_id BIGINT, bot_name VARCHAR(50), home_vc_id BIGINT, PRIMARY KEY (guild_id, bot_name))")
                await cur.execute("SELECT home_vc_id FROM symphony_bot_home_channels WHERE guild_id = %s AND bot_name = 'symphony'", (interaction.guild.id,))
                res = await cur.fetchone()
                if res and res[0]: channel = interaction.guild.get_channel(res[0])
                    
    if not channel: 
        channel = interaction.user.voice.channel if interaction.user.voice else None

    if channel:
        await ensure_voice_connection(interaction.guild, channel.id)
        await interaction.followup.send(embed=discord.Embed(description=f"Joined {channel.mention}.", color=discord.Color.green()))
    else:
        await interaction.followup.send("Join a channel first, or set a home channel.", ephemeral=True)

@bot.tree.command(name="symphony_main_leave", description="Force bot to leave")
async def leave(interaction: discord.Interaction):
    if not await is_dj(interaction): return
    if interaction.guild.voice_client:
        await clear_active_playlist(interaction.guild.id)
        await interaction.guild.voice_client.disconnect()
        await interaction.response.send_message(embed=discord.Embed(description="Left the channel.", color=discord.Color.orange()), ephemeral=True)

@bot.tree.command(name="symphony_main_queue", description="View queue")
async def queue_cmd(interaction: discord.Interaction):
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT title FROM symphony_queue WHERE guild_id = %s AND bot_name = 'symphony' ORDER BY id ASC LIMIT 10", (interaction.guild.id,))
                songs = await cur.fetchall()
    if songs: 
        desc = "\n".join(f"{i+1}. {s[0]}" for i, s in enumerate(songs))
        await interaction.response.send_message(embed=discord.Embed(title="📜 Queue", description=desc, color=discord.Color.blurple()), ephemeral=True)
    else: 
        await interaction.response.send_message(embed=discord.Embed(description="Queue empty.", color=discord.Color.red()), ephemeral=True)

@bot.tree.command(name="symphony_main_shuffle", description="Shuffle the queue")
async def shuffle(interaction: discord.Interaction):
    if not await is_dj(interaction): return
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT id, video_url, title, requester_id FROM symphony_queue WHERE guild_id = %s AND bot_name = 'symphony'", (interaction.guild.id,))
                q = await cur.fetchall()
                if not q: return await interaction.response.send_message("Queue empty.", ephemeral=True)
                l = list(q); random.shuffle(l)
                await cur.execute("DELETE FROM symphony_queue WHERE guild_id = %s AND bot_name = 'symphony'", (interaction.guild.id,))
                for row in l: await cur.execute("INSERT INTO symphony_queue (guild_id, bot_name, video_url, title, requester_id) VALUES (%s, 'symphony', %s, %s, %s)", (interaction.guild.id, row[1], row[2], row[3]))
    await interaction.response.send_message(embed=discord.Embed(description="🔀 Queue shuffled.", color=discord.Color.green()), ephemeral=True)

@bot.tree.command(name="symphony_main_remove", description="Remove song by queue number")
async def remove(interaction: discord.Interaction, index: int):
    if not await is_dj(interaction): return
    if index < 1:
        return await interaction.response.send_message("Invalid index.", ephemeral=True)
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT id FROM symphony_queue WHERE guild_id = %s AND bot_name = 'symphony' ORDER BY id ASC LIMIT 1 OFFSET %s", (interaction.guild.id, index-1))
                row = await cur.fetchone()
                if row: 
                    await cur.execute("DELETE FROM symphony_queue WHERE id = %s", (row[0],))
                    await interaction.response.send_message(embed=discord.Embed(description=f"Removed item #{index}", color=discord.Color.green()), ephemeral=True)
                else: await interaction.response.send_message("Invalid index.", ephemeral=True)

@bot.tree.command(name="symphony_main_skipto", description="Skip to a queue number")
async def skipto(interaction: discord.Interaction, index: int):
    if not await is_dj(interaction): return
    if index < 1:
        return await interaction.response.send_message("Invalid index.", ephemeral=True)
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT id FROM symphony_queue WHERE guild_id = %s AND bot_name = 'symphony' ORDER BY id ASC LIMIT %s", (interaction.guild.id, index-1))
                rows = await cur.fetchall()
                for r in rows: await cur.execute("DELETE FROM symphony_queue WHERE id = %s", (r[0],))
    if interaction.guild.voice_client: await interaction.guild.voice_client.stop()
    await interaction.response.send_message(embed=discord.Embed(description=f"Skipped to #{index}", color=discord.Color.green()), ephemeral=True)

@bot.tree.command(name="symphony_main_move", description="Move song to new position")
async def move(interaction: discord.Interaction, frm: int, to: int):
    if not await is_dj(interaction): return
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT id, video_url, title, requester_id FROM symphony_queue WHERE guild_id = %s AND bot_name = 'symphony' ORDER BY id ASC", (interaction.guild.id,))
                q = list(await cur.fetchall())
                if frm > len(q) or to > len(q) or frm < 1 or to < 1: return await interaction.response.send_message("Invalid index", ephemeral=True)
                item = q.pop(frm-1)
                q.insert(to-1, item)
                await cur.execute("DELETE FROM symphony_queue WHERE guild_id = %s AND bot_name = 'symphony'", (interaction.guild.id,))
                for r in q: await cur.execute("INSERT INTO symphony_queue (guild_id, bot_name, video_url, title, requester_id) VALUES (%s, 'symphony', %s, %s, %s)", (interaction.guild.id, r[1], r[2], r[3]))
    await interaction.response.send_message(embed=discord.Embed(description=f"Moved item from {frm} to {to}", color=discord.Color.green()), ephemeral=True)

# --- PLAYLISTS & HISTORY ---
@bot.tree.command(name="symphony_main_savequeue", description="Save the current queue as a custom personal playlist")
async def savequeue(interaction: discord.Interaction, name: str):
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT video_url, title FROM symphony_queue WHERE guild_id = %s AND bot_name = 'symphony' ORDER BY id ASC", (interaction.guild.id,))
                q = await cur.fetchall()
                if not q: return await interaction.response.send_message("Queue is empty!", ephemeral=True)
                for url, title in q:
                    await cur.execute("INSERT INTO symphony_user_playlists (user_id, playlist_name, video_url, title) VALUES (%s, %s, %s, %s)", (interaction.user.id, name, url, title))
    await interaction.response.send_message(embed=discord.Embed(description=f"💾 Saved **{len(q)}** tracks to your personal playlist: **{name}**", color=discord.Color.green()), ephemeral=True)

@bot.tree.command(name="symphony_main_loadqueue", description="Load a custom personal playlist into the current queue")
async def loadqueue(interaction: discord.Interaction, name: str):
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT video_url, title FROM symphony_user_playlists WHERE user_id = %s AND playlist_name = %s", (interaction.user.id, name))
                q = await cur.fetchall()
                if not q: return await interaction.response.send_message("Playlist not found or empty.", ephemeral=True)
                for url, title in q:
                    await cur.execute("INSERT INTO symphony_queue (guild_id, bot_name, video_url, title, requester_id) VALUES (%s, 'symphony', %s, %s, %s)", (interaction.guild.id, url, title, interaction.user.id))
    await interaction.response.send_message(embed=discord.Embed(description=f"📂 Loaded **{len(q)}** tracks from **{name}** into the queue!", color=discord.Color.green()))
    vc = interaction.guild.voice_client
    if not vc or (not getattr(vc, 'playing', False) and not getattr(vc, 'paused', False)):
        channel = interaction.user.voice.channel if interaction.user.voice else None
        if channel: await process_queue(interaction.guild, channel.id)

@bot.tree.command(name="symphony_main_leaderboard", description="Show the top 10 most played tracks in this server")
async def leaderboard(interaction: discord.Interaction):
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT title, COUNT(*) as plays FROM symphony_history WHERE guild_id = %s GROUP BY title ORDER BY plays DESC LIMIT 10", (interaction.guild.id,))
                songs = await cur.fetchall()
    if not songs: return await interaction.response.send_message("No play history yet.", ephemeral=True)
    desc = "\n".join(f"**{i+1}.** {s[0]} *(Played {s[1]} times)*" for i, s in enumerate(songs))
    await interaction.response.send_message(embed=discord.Embed(title="🏆 Server Top Tracks", description=desc, color=discord.Color.gold()))

@bot.tree.command(name="symphony_main_history", description="Show last 5 songs played in the server")
async def history(interaction: discord.Interaction):
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT title FROM symphony_history WHERE guild_id = %s ORDER BY played_at DESC LIMIT 5", (interaction.guild.id,))
                songs = await cur.fetchall()
    if songs: await interaction.response.send_message(embed=discord.Embed(title="📜 History", description="\n".join(f"- {s[0]}" for s in songs), color=discord.Color.blurple()), ephemeral=True)
    else: await interaction.response.send_message("No history.", ephemeral=True)

@bot.tree.command(name="symphony_main_userhistory", description="See the last 10 tracks requested by a specific user")
async def userhistory(interaction: discord.Interaction, member: discord.Member):
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT id, title, video_url FROM symphony_history WHERE guild_id = %s AND requester_id = %s ORDER BY played_at DESC LIMIT 10", (interaction.guild.id, member.id))
                songs = await cur.fetchall()
    if not songs: return await interaction.response.send_message(embed=discord.Embed(description=f"📭 {member.display_name} hasn't queued any songs yet.", color=discord.Color.red()), ephemeral=True)
    desc = "\n".join([f"**{idx + 1}.** [{song[1]}]({song[2]})" for idx, song in enumerate(songs)])
    embed = discord.Embed(title=f"🎧 {member.display_name}'s Play History", description=desc, color=discord.Color.blue())
    embed.set_footer(text="Use /symphony_main_steal <user> <number> to add one to the queue!")
    await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="symphony_main_steal", description="Steal a song from a user's history and add it to the queue")
async def steal(interaction: discord.Interaction, member: discord.Member, track_number: int):
    if track_number < 1:
        return await interaction.response.send_message(embed=discord.Embed(description="❌ Track number must be 1 or greater.", color=discord.Color.red()), ephemeral=True)
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT video_url, title FROM symphony_history WHERE guild_id = %s AND requester_id = %s ORDER BY played_at DESC LIMIT 1 OFFSET %s", (interaction.guild.id, member.id, track_number - 1))
                song = await cur.fetchone()
                if not song: return await interaction.response.send_message(embed=discord.Embed(description=f"❌ Could not find track #{track_number} in their history.", color=discord.Color.red()), ephemeral=True)
                url, title = song
                await cur.execute("INSERT INTO symphony_queue (guild_id, bot_name, video_url, title, requester_id) VALUES (%s, 'symphony', %s, %s, %s)", (interaction.guild.id, url, title, interaction.user.id))
    await interaction.response.send_message(embed=discord.Embed(title="🥷 Song Stolen!", description=f"Added **{title}** to the queue from {member.display_name}'s history.", color=discord.Color.green()))
    vc = interaction.guild.voice_client
    if not vc or (not getattr(vc, 'playing', False) and not getattr(vc, 'paused', False)):
        channel = interaction.user.voice.channel if interaction.user.voice else None
        if channel: await process_queue(interaction.guild, channel.id)

@bot.tree.command(name="symphony_main_grab", description="DM yourself the current song")
async def grab(interaction: discord.Interaction):
    if interaction.guild.id in playback_tracking:
        data = playback_tracking[interaction.guild.id]
        dm_embed = discord.Embed(
            title="🎵 Track Saved!",
            description=f"Hey **{interaction.user.display_name}**!\nHere is the track you wanted to save:\n\n**[{data.get('title', 'Unknown Title')}]({data['url']})**",
            color=discord.Color.from_rgb(88, 101, 242)
        )
        try:
            await interaction.user.send(embed=dm_embed)
            await interaction.response.send_message(embed=discord.Embed(description="📬 Check your DMs!", color=discord.Color.green()), ephemeral=True)
        except discord.Forbidden:
            await interaction.response.send_message(embed=discord.Embed(description="❌ I can't DM you! Please check your privacy settings.", color=discord.Color.red()), ephemeral=True)
    else:
        await interaction.response.send_message(embed=discord.Embed(description="Nothing is currently playing.", color=discord.Color.red()), ephemeral=True)

# --- MODIFIERS & FILTERS ---
@bot.tree.command(name="symphony_main_volume", description="Set volume (1-200)")
async def volume(interaction: discord.Interaction, vol: int):
    if not await is_dj(interaction): return
    vol = max(1, min(200, vol))
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("INSERT INTO symphony_guild_settings (guild_id, volume) VALUES (%s, %s) ON DUPLICATE KEY UPDATE volume = %s", (interaction.guild.id, vol, vol))
    if interaction.guild.voice_client:
        try: await interaction.guild.voice_client.set_volume(vol)
        except: pass
    await interaction.response.send_message(embed=discord.Embed(description=f"🔊 Volume set to {vol}%", color=discord.Color.green()), ephemeral=True)

@bot.tree.command(name="symphony_main_loop", description="Toggle loop: off, song, queue")
async def loop_cmd(interaction: discord.Interaction, mode: str):
    if not await is_dj(interaction): return
    if mode not in ['off', 'song', 'queue']: return await interaction.response.send_message("Invalid mode.", ephemeral=True)
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("INSERT INTO symphony_guild_settings (guild_id, loop_mode) VALUES (%s, %s) ON DUPLICATE KEY UPDATE loop_mode = %s", (interaction.guild.id, mode, mode))
    await interaction.response.send_message(embed=discord.Embed(description=f"🔁 Looping set to: {mode}", color=discord.Color.green()), ephemeral=True)

@bot.tree.command(name="symphony_main_filter", description="Apply an audio filter to the music")
@app_commands.describe(mode="Choose an audio filter to apply")
@app_commands.choices(mode=[
    app_commands.Choice(name="None (Standard high quality audio)", value="none"),
    app_commands.Choice(name="Bassboost (Enhances low-end frequencies)", value="bassboost"),
    app_commands.Choice(name="Nightcore (Speeds up and raises pitch)", value="nightcore"),
    app_commands.Choice(name="Vaporwave (Slows down and adds reverb/low pitch)", value="vaporwave")
])
async def filter_cmd(interaction: discord.Interaction, mode: str):
    if not await is_dj(interaction): return
    await ensure_guild_settings(interaction.guild.id)
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                if mode != 'none': await cur.execute("UPDATE symphony_guild_settings SET filter_mode = %s, custom_modifiers_left = 0 WHERE guild_id = %s", (mode, interaction.guild.id))
                else: await cur.execute("UPDATE symphony_guild_settings SET filter_mode = %s WHERE guild_id = %s", (mode, interaction.guild.id))
    if interaction.guild.voice_client:
        wav_filters = wavelink.Filters()
        if mode == 'nightcore': wav_filters.timescale.set(speed=1.25, pitch=1.3)
        elif mode == 'vaporwave': wav_filters.timescale.set(speed=0.8, pitch=0.8)
        elif mode == 'bassboost': wav_filters.equalizer.set(bands=[(0, 0.3), (1, 0.2), (2, 0.1)])
        try: await interaction.guild.voice_client.set_filters(wav_filters)
        except: pass
    await interaction.response.send_message(embed=discord.Embed(description=f"🎛️ Filter set to: **{mode}**.", color=discord.Color.blurple()), ephemeral=True)

@bot.tree.command(name="symphony_main_fade", description="Toggle 5-second smooth fade in/out transitions")
@app_commands.describe(mode="Enable or disable smooth fades")
@app_commands.choices(mode=[
    app_commands.Choice(name="Enable 5s Fades", value="fade"),
    app_commands.Choice(name="Disable Fades (Standard)", value="off")
])
async def toggle_fade(interaction: discord.Interaction, mode: str):
    if not await is_dj(interaction): return
    await ensure_guild_settings(interaction.guild.id)
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("UPDATE symphony_guild_settings SET transition_mode = %s WHERE guild_id = %s", (mode, interaction.guild.id))
    if mode == "fade": await interaction.response.send_message(embed=discord.Embed(description="🌊 Smooth **5-second Fades** have been enabled.", color=discord.Color.green()), ephemeral=True)
    else: await interaction.response.send_message(embed=discord.Embed(description="⏹️ Smooth Fades have been disabled.", color=discord.Color.red()), ephemeral=True)

@bot.tree.command(name="symphony_main_modify", description="Change speed and pitch for upcoming tracks")
@app_commands.describe(speed="Speed multiplier (0.5 to 2.0)", pitch="Pitch multiplier (0.5 to 2.0)", duration="How many tracks this lasts (default 1)")
async def modify_audio(interaction: discord.Interaction, speed: float = 1.0, pitch: float = 1.0, duration: int = 1):
    if not await is_dj(interaction): return
    await ensure_guild_settings(interaction.guild.id)
    speed = max(0.5, min(2.0, speed))
    pitch = max(0.5, min(2.0, pitch))
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT filter_mode FROM symphony_guild_settings WHERE guild_id = %s", (interaction.guild.id,))
                res = await cur.fetchone()
                if res and res[0] != 'none': return await interaction.response.send_message(embed=discord.Embed(description="❌ **Conflict:** Disable standard Filters via `/symphony_main_filter none` first.", color=discord.Color.red()), ephemeral=True)
                await cur.execute("UPDATE symphony_guild_settings SET custom_speed = %s, custom_pitch = %s, custom_modifiers_left = %s WHERE guild_id = %s", (speed, pitch, duration, interaction.guild.id))
    if interaction.guild.voice_client:
        wav_filters = wavelink.Filters()
        wav_filters.timescale.set(speed=speed, pitch=pitch)
        try: await interaction.guild.voice_client.set_filters(wav_filters)
        except: pass
    await interaction.response.send_message(embed=discord.Embed(title="🎛️ Audio Modifiers Set", description=f"**Speed:** {speed}x\n**Pitch:** {pitch}x\n*Active for the next {duration} track(s).* ", color=discord.Color.gold()), ephemeral=True)

# --- SCRUBBING ---
@bot.tree.command(name="symphony_main_seek", description="Seek to seconds")
async def seek(interaction: discord.Interaction, seconds: int):
    if not await is_dj(interaction): return
    if interaction.guild.id not in playback_tracking: return await interaction.response.send_message("Nothing playing.", ephemeral=True)
    if interaction.guild.voice_client:
        await interaction.guild.voice_client.seek(seconds * 1000)
        playback_tracking[interaction.guild.id]['offset'] = seconds
        playback_tracking[interaction.guild.id]['start_time'] = time.time()
    await interaction.response.send_message(embed=discord.Embed(description=f"Seeked to {seconds}s", color=discord.Color.green()), ephemeral=True)

@bot.tree.command(name="symphony_main_forward", description="Skip forward X seconds")
async def forward(interaction: discord.Interaction, seconds: int):
    if not await is_dj(interaction): return
    if interaction.guild.id not in playback_tracking: return await interaction.response.send_message("Nothing playing.", ephemeral=True)
    data = playback_tracking[interaction.guild.id]
    current = int((time.time() - data['start_time']) * data.get('speed', 1.0) + data['offset'])
    new_pos = current + seconds
    if interaction.guild.voice_client:
        await interaction.guild.voice_client.seek(new_pos * 1000)
        playback_tracking[interaction.guild.id]['offset'] = new_pos
        playback_tracking[interaction.guild.id]['start_time'] = time.time()
    await interaction.response.send_message(embed=discord.Embed(description=f"Skipped forward {seconds}s", color=discord.Color.green()), ephemeral=True)

@bot.tree.command(name="symphony_main_rewind", description="Rewind X seconds")
async def rewind(interaction: discord.Interaction, seconds: int):
    if not await is_dj(interaction): return
    if interaction.guild.id not in playback_tracking: return await interaction.response.send_message("Nothing playing.", ephemeral=True)
    data = playback_tracking[interaction.guild.id]
    current = int((time.time() - data['start_time']) * data.get('speed', 1.0) + data['offset'])
    new_pos = max(0, current - seconds)
    if interaction.guild.voice_client:
        await interaction.guild.voice_client.seek(new_pos * 1000)
        playback_tracking[interaction.guild.id]['offset'] = new_pos
        playback_tracking[interaction.guild.id]['start_time'] = time.time()
    await interaction.response.send_message(embed=discord.Embed(description=f"Rewound {seconds}s", color=discord.Color.green()), ephemeral=True)

@bot.tree.command(name="symphony_main_replay", description="Restart current song")
async def replay(interaction: discord.Interaction):
    if not await is_dj(interaction): return
    if interaction.guild.id not in playback_tracking: return await interaction.response.send_message("Nothing playing.", ephemeral=True)
    if interaction.guild.voice_client:
        await interaction.guild.voice_client.seek(0)
        playback_tracking[interaction.guild.id]['offset'] = 0
        playback_tracking[interaction.guild.id]['start_time'] = time.time()
    await interaction.response.send_message(embed=discord.Embed(description="Replaying song.", color=discord.Color.green()), ephemeral=True)

# --- UTILITY & INFO ---
@bot.tree.command(name="symphony_main_panel", description="Spawn the advanced music control panel")
async def panel(interaction: discord.Interaction):
    class AdvancedPanel(discord.ui.View):
        def __init__(self): super().__init__(timeout=None)
        @discord.ui.button(label="⏯️ Play/Pause", style=discord.ButtonStyle.primary, row=0)
        async def pr(self, i: discord.Interaction, b: discord.ui.Button):
            if not await is_dj(i): return
            vc = i.guild.voice_client
            if vc:
                if getattr(vc, 'playing', False): 
                    await vc.pause(True)
                    await i.response.send_message("⏸️ Playback Paused", ephemeral=True)
                else: 
                    await vc.pause(False)
                    await i.response.send_message("▶️ Playback Resumed", ephemeral=True)
            else: await i.response.send_message("Nothing is playing.", ephemeral=True)
        @discord.ui.button(label="⏹️ Stop", style=discord.ButtonStyle.danger, row=0)
        async def st(self, i: discord.Interaction, b: discord.ui.Button):
            if not await is_dj(i): return
            await stop_playback(i.guild)
            await i.response.send_message("⏹️ Stopped and cleared state", ephemeral=True)
        @discord.ui.button(label="⏭️ Skip", style=discord.ButtonStyle.secondary, row=0)
        async def sk(self, i: discord.Interaction, b: discord.ui.Button):
            if not await is_dj(i): return
            if i.guild.voice_client: 
                await i.guild.voice_client.stop()
                await i.response.send_message("⏭️ Skipped to next track", ephemeral=True)
            else: await i.response.send_message("Nothing to skip.", ephemeral=True)
        @discord.ui.button(label="⏪ -10s", style=discord.ButtonStyle.secondary, row=1)
        async def rw(self, i: discord.Interaction, b: discord.ui.Button):
            if not await is_dj(i): return
            if i.guild.id not in playback_tracking: return await i.response.send_message("Nothing playing.", ephemeral=True)
            data = playback_tracking[i.guild.id]
            current = int((time.time() - data['start_time']) * data.get('speed', 1.0) + data['offset'])
            new_pos = max(0, current - 10)
            if i.guild.voice_client:
                await i.guild.voice_client.seek(new_pos * 1000)
                playback_tracking[i.guild.id]['offset'] = new_pos
                playback_tracking[i.guild.id]['start_time'] = time.time()
            await i.response.send_message("Rewound 10 seconds.", ephemeral=True)
        @discord.ui.button(label="⏩ +10s", style=discord.ButtonStyle.secondary, row=1)
        async def fw(self, i: discord.Interaction, b: discord.ui.Button):
            if not await is_dj(i): return
            if i.guild.id not in playback_tracking: return await i.response.send_message("Nothing playing.", ephemeral=True)
            data = playback_tracking[i.guild.id]
            current = int((time.time() - data['start_time']) * data.get('speed', 1.0) + data['offset'])
            new_pos = current + 10
            if i.guild.voice_client:
                await i.guild.voice_client.seek(new_pos * 1000)
                playback_tracking[i.guild.id]['offset'] = new_pos
                playback_tracking[i.guild.id]['start_time'] = time.time()
            await i.response.send_message("Skipped forward 10 seconds.", ephemeral=True)
        @discord.ui.button(label="🔀 Shuffle", style=discord.ButtonStyle.success, row=2)
        async def shuf(self, i: discord.Interaction, b: discord.ui.Button):
            if not await is_dj(i): return
            await i.response.defer(ephemeral=True)
            async with DBPoolManager() as pool:
                async with pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute("SELECT id, video_url, title, requester_id FROM symphony_queue WHERE guild_id = %s AND bot_name = 'symphony'", (i.guild.id,))
                        q = await cur.fetchall()
                        if not q: return await i.followup.send("Queue empty.")
                        l = list(q); random.shuffle(l)
                        await cur.execute("DELETE FROM symphony_queue WHERE guild_id = %s AND bot_name = 'symphony'", (i.guild.id,))
                        for row in l: await cur.execute("INSERT INTO symphony_queue (guild_id, bot_name, video_url, title, requester_id) VALUES (%s, 'symphony', %s, %s, %s)", (i.guild.id, row[1], row[2], row[3]))
            await i.followup.send("🔀 Queue successfully shuffled!")
        @discord.ui.button(label="📜 View Queue", style=discord.ButtonStyle.secondary, row=2)
        async def vq(self, i: discord.Interaction, b: discord.ui.Button):
            await i.response.defer(ephemeral=True)
            async with DBPoolManager() as pool:
                async with pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute("SELECT title FROM symphony_queue WHERE guild_id = %s AND bot_name = 'symphony' ORDER BY id ASC LIMIT 10", (i.guild.id,))
                        songs = await cur.fetchall()
            if songs: await i.followup.send("**Current Queue:**\n" + "\n".join(f"{idx+1}. {s[0]}" for idx, s in enumerate(songs)))
            else: await i.followup.send("Queue is empty.")

    embed = discord.Embed(title="🎛️ SYMPHONY Music Control Panel", description="Manage your audio playback directly from these buttons.", color=discord.Color.from_rgb(43, 45, 49))
    embed.set_footer(text="SYMPHONY Main Music System")
    await interaction.response.send_message(embed=embed, view=AdvancedPanel())

@bot.tree.command(name="symphony_main_nowplaying", description="Show song status")
async def nowplaying(interaction: discord.Interaction):
    if interaction.guild.id in playback_tracking:
        data = playback_tracking[interaction.guild.id]
        cur_t = int((time.time() - data['start_time']) * data.get('speed', 1.0) + data['offset'])
        dur = data.get('duration', 0)
        p_bar = make_progress_bar(cur_t, dur)
        embed = discord.Embed(title="🎵 Now Playing", description=f"**[{data.get('title', 'Playing')}]({data['url']})**\n\n`{p_bar}`", color=discord.Color.blue())
        await interaction.response.send_message(embed=embed, ephemeral=True)
    else: 
        await interaction.response.send_message(embed=discord.Embed(description="Nothing playing.", color=discord.Color.red()), ephemeral=True)

@bot.tree.command(name="symphony_main_ping", description="Bot latency")
async def ping(interaction: discord.Interaction):
    latency = bot.latency
    latency_ms = 0 if not isinstance(latency, (int, float)) or latency != latency else round(latency * 1000)
    await interaction.response.send_message(embed=discord.Embed(description=f"🏓 Pong! {latency_ms}ms", color=discord.Color.green()), ephemeral=True)

@bot.tree.command(name="symphony_main_uptime", description="Bot uptime")
async def uptime(interaction: discord.Interaction):
    up = str(datetime.timedelta(seconds=int(time.time() - bot.start_time)))
    await interaction.response.send_message(embed=discord.Embed(description=f"⏱️ Uptime: {up}", color=discord.Color.green()), ephemeral=True)

@bot.tree.command(name="symphony_main_stats", description="Bot statistics")
async def stats(interaction: discord.Interaction):
    await interaction.response.send_message(embed=discord.Embed(description=f"📊 Servers: {len(bot.guilds)}\n🎧 Active Players: {len(playback_tracking)}", color=discord.Color.green()), ephemeral=True)

@bot.tree.command(name="symphony_main_help", description="List all SYMPHONY commands")
async def help_cmd(interaction: discord.Interaction):
    cmds = [c.name for c in bot.tree.get_commands() if c.name.startswith("symphony_main_")]
    await interaction.response.send_message(embed=discord.Embed(title="📚 Command List", description=", ".join(cmds), color=discord.Color.blue()), ephemeral=True)

@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: discord.app_commands.AppCommandError):
    if isinstance(error, app_commands.CheckFailure):
        error_msg = str(error) if str(error) else "You don't have permission to use this command."
        if not interaction.response.is_done(): await interaction.response.send_message(error_msg, ephemeral=True)
        else: await interaction.followup.send(error_msg, ephemeral=True)
        return
    logger.error(f"Command {interaction.command.name} failed: {error}", exc_info=True)
    error_msg = f"An error occurred: `{error}`"
    if not interaction.response.is_done(): await interaction.response.send_message(error_msg, ephemeral=True)
    else: await interaction.followup.send(error_msg, ephemeral=True)

# --- LIVE PLAYLIST SYNC FEATURE ---
async def init_playlist_db():
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute('''CREATE TABLE IF NOT EXISTS symphony_active_playlists (guild_id BIGINT, bot_name VARCHAR(50), playlist_url TEXT, known_track_count INT DEFAULT 0, requester_id BIGINT, channel_id BIGINT DEFAULT NULL, PRIMARY KEY (guild_id, bot_name))''')
                try: await cur.execute("ALTER TABLE symphony_active_playlists ADD COLUMN channel_id BIGINT DEFAULT NULL")
                except: pass

def resolve_playlist_source(search, playlist=None):
    candidates = [getattr(playlist, 'url', None), getattr(playlist, 'uri', None), search]
    for candidate in candidates:
        if isinstance(candidate, str):
            cleaned = candidate.strip()
            if cleaned.startswith("http://") or cleaned.startswith("https://"):
                return cleaned
    return None

async def set_active_playlist(guild_id, playlist_url, known_track_count, requester_id, channel_id):
    if not playlist_url:
        return
    await init_playlist_db()
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("REPLACE INTO symphony_active_playlists (guild_id, bot_name, playlist_url, known_track_count, requester_id, channel_id) VALUES (%s, 'symphony', %s, %s, %s, %s)", (guild_id, playlist_url, known_track_count, requester_id, channel_id))

async def clear_active_playlist(guild_id):
    await init_playlist_db()
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("DELETE FROM symphony_active_playlists WHERE guild_id = %s AND bot_name = 'symphony'", (guild_id,))

@tasks.loop(seconds=30.0)
async def playlist_sync_loop():
    async with DBPoolManager() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await init_playlist_db()
                await cur.execute("SELECT guild_id, playlist_url, known_track_count, requester_id, channel_id FROM symphony_active_playlists WHERE bot_name = 'symphony'")
                playlists = await cur.fetchall()

    if not playlists: return

    opts = ytdl_format_options.copy()
    opts['extract_flat'] = True
    ydl = yt_dlp.YoutubeDL(opts)
    loop = asyncio.get_event_loop()

    for guild_id, url, known_count, req_id, channel_id in playlists:
        try:
            data = await loop.run_in_executor(None, lambda: ydl.extract_info(url, download=False))
            if not data or 'entries' not in data: continue
        
            entries = [e for e in data['entries'] if e is not None]
            current_count = len(entries)

            if current_count > known_count:
                new_tracks = entries[known_count:]
                added_count = 0
                async with DBPoolManager() as pool:
                    async with pool.acquire() as conn:
                        async with conn.cursor() as cur:
                            for entry in new_tracks:
                                t_title = entry.get('title', 'Unknown Track')
                                t_url = entry.get('url') or entry.get('webpage_url')
                                if t_url and not t_url.startswith('http'): t_url = f"https://www.youtube.com/watch?v={entry.get('id')}"
                                if t_url:
                                    await cur.execute("INSERT INTO symphony_queue (guild_id, bot_name, video_url, title, requester_id) VALUES (%s, 'symphony', %s, %s, %s)", (guild_id, t_url, t_title, req_id))
                                    added_count += 1
                            await cur.execute("UPDATE symphony_active_playlists SET known_track_count = %s WHERE guild_id = %s", (current_count, guild_id))

                guild = bot.get_guild(guild_id)
                if guild:
                    vc = guild.voice_client
                    if added_count > 0 and (not vc or (not getattr(vc, 'playing', False) and not getattr(vc, 'paused', False))):
                        target_channel = vc.channel if vc and getattr(vc, 'channel', None) else guild.get_channel(channel_id) if channel_id else await get_home_channel(guild)
                        if target_channel:
                            bot.loop.create_task(process_queue(guild, target_channel.id))
                    embed = discord.Embed(title="📡 Playlist Updated", description=f"Detected **{current_count - known_count}** new tracks added to the monitored playlist! Auto-queued them.", color=discord.Color.green())
                    await send_feedback(guild, embed)
                    await send_webhook_log(bot.user.name if bot.user else "Unknown Node", "📡 Playlist Sync", f"Detected **{current_count - known_count}** new tracks for guild {guild.name} and queued them automatically.", discord.Color.green())
        except Exception as e:
            logger.error(f"Sync Loop Error: {e}")

@bot.event
async def on_ready_sync():
    await init_playlist_db()
    if not playlist_sync_loop.is_running(): playlist_sync_loop.start()
bot.add_listener(on_ready_sync, 'on_ready')

# --- ARIA SWARM OVERRIDE LISTENER ---
@tasks.loop(seconds=2.0)
async def aria_command_listener():
    try:
        async with DBPoolManager() as pool:
            async with pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    await cur.execute("CREATE TABLE IF NOT EXISTS symphony_swarm_overrides (guild_id BIGINT, bot_name VARCHAR(50), command VARCHAR(20), PRIMARY KEY(guild_id, bot_name))")
                    await cur.execute("SELECT guild_id, command FROM symphony_swarm_overrides WHERE bot_name = %s", ('symphony',))
                    commands = await cur.fetchall()

        if not commands: return

        for row in commands:
            guild_id = row['guild_id']
            cmd = row['command']
            guild = bot.get_guild(guild_id)

            if guild and guild.voice_client:
                vc = guild.voice_client
                executed = False
                
                if cmd == 'PAUSE' and getattr(vc, 'playing', False): 
                    await vc.pause(True); executed = True
                elif cmd == 'RESUME' and getattr(vc, 'paused', False): 
                    await vc.pause(False); executed = True
                elif cmd == 'SKIP' or cmd == 'STOP': 
                    await vc.stop(); executed = True
                elif cmd == 'UPDATE_FILTER':
                    async with DBPoolManager() as _pool:
                        async with _pool.acquire() as _conn:
                            async with _conn.cursor() as _cur:
                                await _cur.execute("SELECT filter_mode FROM symphony_guild_settings WHERE guild_id = %s", (guild_id,))
                                res = await _cur.fetchone()
                                if res:
                                    f_mode = res[0]
                                    wav_filters = wavelink.Filters()
                                    if f_mode == 'nightcore': wav_filters.timescale.set(speed=1.25, pitch=1.3)
                                    elif f_mode == 'vaporwave': wav_filters.timescale.set(speed=0.8, pitch=0.8)
                                    elif f_mode == 'bassboost': wav_filters.equalizer.set(bands=[(0, 0.3), (1, 0.2), (2, 0.1)])
                                    try: await vc.set_filters(wav_filters)
                                    except: pass
                    executed = True

                if cmd == 'STOP':
                    async with DBPoolManager() as pool:
                        async with pool.acquire() as conn:
                            async with conn.cursor() as cur:
                                await cur.execute("DELETE FROM symphony_queue WHERE guild_id = %s AND bot_name = %s", (guild_id, 'symphony'))
                    await clear_active_playlist(guild_id)

                if executed:
                    try: await send_webhook_log(bot.user.name if bot.user else "Unknown Node", "🤖 Aria Override", f"Aria forcefully executed a **{cmd}** command in `{guild.name}`.", discord.Color.purple())
                    except: pass

            async with DBPoolManager() as pool:
                async with pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute("DELETE FROM symphony_swarm_overrides WHERE guild_id = %s AND bot_name = %s", (guild_id, 'symphony'))
    except Exception as e: pass

@bot.event
async def on_ready_aria_listener():
    if not aria_command_listener.is_running(): aria_command_listener.start()
bot.add_listener(on_ready_aria_listener, 'on_ready')

# --- AUTOMATED BACKGROUND MAINTENANCE ---
@tasks.loop(minutes=5.0)
async def zombie_reaper_loop():
    for guild in bot.guilds:
        vc = guild.voice_client
        if vc and not getattr(vc, 'playing', False) and not getattr(vc, 'paused', False):
            try:
                async with DBPoolManager() as pool:
                    async with pool.acquire() as conn:
                        async with conn.cursor() as cur:
                            await cur.execute("SELECT COUNT(*) FROM symphony_queue WHERE guild_id = %s AND bot_name = 'symphony'", (guild.id,))
                            res = await cur.fetchone()
                            if res and res[0] == 0:
                                await cur.execute("SELECT stay_in_vc FROM symphony_guild_settings WHERE guild_id = %s", (guild.id,))
                                cfg = await cur.fetchone()
                                stay_in_vc = bool(cfg[0]) if cfg else False
                                if _should_auto_disconnect(guild, stay_in_vc):
                                    await stop_playback(guild)
                                else:
                                    playback_tracking.pop(guild.id, None)
                                    await cur.execute("UPDATE symphony_playback_state SET is_playing = FALSE WHERE guild_id = %s AND bot_name = 'symphony'", (guild.id,))
            except: pass

@tasks.loop(hours=24.0)
async def database_janitor_loop():
    try:
        async with DBPoolManager() as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("DELETE FROM symphony_history WHERE played_at < NOW() - INTERVAL 30 DAY")
                    deleted_rows = cur.rowcount
                    if deleted_rows > 0:
                        logger.info(f"🧹 Janitor cleared {deleted_rows} old history records.")
                        await send_webhook_log(bot.user.name if bot.user else "Unknown Node", "🧹 Database Janitor", f"Successfully cleared **{deleted_rows}** old song history records to optimize database speed.", discord.Color.blurple())
    except Exception as e:
        logger.error(f"Janitor Error: {e}")

@bot.event
async def on_ready_maintenance():
    if not zombie_reaper_loop.is_running(): zombie_reaper_loop.start()
    if not database_janitor_loop.is_running(): database_janitor_loop.start()
bot.add_listener(on_ready_maintenance, 'on_ready')

# --- ARIA DIRECT DRONE CONTROL ---
@tasks.loop(seconds=2.0)
async def direct_order_listener():
    try:
        async with DBPoolManager() as pool:
            async with pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    await cur.execute("CREATE TABLE IF NOT EXISTS symphony_swarm_direct_orders (id INT AUTO_INCREMENT PRIMARY KEY, bot_name VARCHAR(50), guild_id BIGINT, vc_id BIGINT, text_channel_id BIGINT, command VARCHAR(50), data TEXT)")
                    await cur.execute("SELECT * FROM symphony_swarm_direct_orders WHERE bot_name = %s", ('symphony',))
                    orders = await cur.fetchall()

        if not orders: return

        for order in orders:
            oid = order['id']
            guild = bot.get_guild(order['guild_id'])
            cmd = order['command']
            data = order['data']
            
            if guild:
                text_channel = guild.get_channel(order['text_channel_id'])
                vc_target = guild.get_channel(order['vc_id'])
                
                if cmd == 'PLAY' and vc_target:
                    await ensure_voice_connection(guild, vc_target.id)
                    try:
                        tracks = await wavelink.Playable.search(data)
                        if tracks:
                            entries = tracks.tracks if isinstance(tracks, wavelink.Playlist) else [tracks[0] if isinstance(tracks, list) else tracks]
                            is_playlist_request = isinstance(tracks, wavelink.Playlist) or (isinstance(data, str) and 'list=' in data and len(entries) > 1)
                            playlist_url = resolve_playlist_source(data, tracks if isinstance(tracks, wavelink.Playlist) else None) if is_playlist_request else None
                            added_count = 0
                            async with DBPoolManager() as pool:
                                async with pool.acquire() as conn:
                                    async with conn.cursor() as cur:
                                        for track in entries:
                                            await cur.execute("INSERT INTO symphony_queue (guild_id, bot_name, video_url, title, requester_id) VALUES (%s, %s, %s, %s, %s)", (guild.id, 'symphony', track.uri, track.title, bot.user.id))
                                            added_count += 1
                            if playlist_url:
                                await set_active_playlist(guild.id, playlist_url, len(entries), bot.user.id if bot.user else None, vc_target.id)
                                            
                            if added_count > 0:
                                try:
                                    await send_feedback(guild, discord.Embed(title="🎶 Direct Order Received", description=f"Aria successfully deposited **{added_count}** tracks into my matrix. Booting audio engine...", color=discord.Color.green()))
                                    await send_webhook_log(bot.user.name if bot.user else "Unknown Node", "📥 Matrix Loaded", f"Aria routed a payload of **{added_count}** tracks directly into `{guild.name}`.", discord.Color.blue())
                                except: pass
                    except Exception as e:
                        logger.error(f"Direct Play Extractor Error: {e}")
                                
                    if guild.voice_client and not getattr(guild.voice_client, 'playing', False) and not getattr(guild.voice_client, 'paused', False):
                        bot.loop.create_task(process_queue(guild, vc_target.id))
                        
                elif cmd == 'LEAVE':
                    if guild.voice_client:
                        force_leave = isinstance(data, str) and data.strip().lower() in {'force', 'override', 'admin'}
                        if _has_human_listeners(guild.voice_client) and not force_leave:
                            logger.info(f"[{guild.id}] Ignoring non-forced LEAVE order while human listeners are present.")
                        else:
                            await clear_active_playlist(guild.id)
                            await guild.voice_client.disconnect(force=True)
                            async with DBPoolManager() as pool:
                                async with pool.acquire() as conn:
                                    async with conn.cursor() as cur:
                                        await cur.execute("DELETE FROM symphony_queue WHERE guild_id = %s AND bot_name = 'symphony'", (guild.id,))
                                        await cur.execute("REPLACE INTO symphony_playback_state (guild_id, bot_name, is_playing) VALUES (%s, 'symphony', FALSE)", (guild.id,))

                await send_webhook_log(bot.user.name if bot.user else "Unknown Node", "🤖 Direct Drone Execution", f"Received and executed direct `{cmd}` order from Aria in `{guild.name}`.", discord.Color.purple())

            async with DBPoolManager() as pool:
                async with pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute("DELETE FROM symphony_swarm_direct_orders WHERE id = %s", (oid,))
                        
    except Exception as e: pass

@bot.event
async def on_ready_direct_order():
    if not direct_order_listener.is_running(): direct_order_listener.start()
bot.add_listener(on_ready_direct_order, 'on_ready')

# --- SWARM INTELLIGENCE MODULE ---
class SwarmIntelligence(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.bot_name = 'symphony'
        self.status_updater.start()
        self.heartbeat.start()
        self.watchdog.start()

    def cog_unload(self):
        self.status_updater.cancel()
        self.heartbeat.cancel()
        self.watchdog.cancel()

    @tasks.loop(seconds=15)
    async def status_updater(self):
        try:
            for guild in self.bot.guilds:
                if guild.voice_client and getattr(guild.voice_client, 'playing', False):
                    track_title = playback_tracking.get(guild.id, {}).get("title")
                    if not track_title:
                        async with DBPoolManager() as pool:
                            async with pool.acquire() as conn:
                                async with conn.cursor(aiomysql.DictCursor) as cur:
                                    await cur.execute("SELECT title FROM symphony_playback_state WHERE guild_id = %s AND bot_name = 'symphony' AND is_playing = TRUE LIMIT 1", (guild.id,))
                                    res = await cur.fetchone()
                                    track_title = res["title"] if res else None
                    if track_title:
                        await self.bot.change_presence(activity=discord.Activity(type=discord.ActivityType.listening, name=f"{track_title[:20]} in {guild.name}"))
                        return
            await self.bot.change_presence(activity=discord.Activity(type=discord.ActivityType.watching, name="the Swarm | Idle"))
        except: pass

    @tasks.loop(seconds=30)
    async def heartbeat(self):
        try:
            async with DBPoolManager() as pool:
                async with pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute("CREATE TABLE IF NOT EXISTS swarm_health (bot_name VARCHAR(50) PRIMARY KEY, last_pulse TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, status VARCHAR(20))")
                        await cur.execute("REPLACE INTO swarm_health (bot_name, status) VALUES (%s, 'HEALTHY')", (self.bot_name,))
        except: pass

    @tasks.loop(seconds=15)
    async def watchdog(self):
        try:
            for guild in self.bot.guilds:
                vc = guild.voice_client
                if vc and not getattr(vc, 'playing', False) and not getattr(vc, 'paused', False):
                    if guild.id in playback_tracking:
                        track_info = playback_tracking[guild.id]
                        now = time.time()
                        if now - track_info.get('start_time', 0) > 10:
                            if now - track_info.get('last_watchdog_revival', 0) < 45: continue
                            async with DBPoolManager() as pool:
                                async with pool.acquire() as conn:
                                    async with conn.cursor() as cur:
                                        revival_attempts = track_info.get('watchdog_revival_attempts', 0)
                                        if revival_attempts >= 3:
                                            playback_tracking.pop(guild.id, None)
                                            await cur.execute(f"UPDATE {self.bot_name}_playback_state SET is_playing = FALSE WHERE guild_id = %s AND bot_name = %s", (guild.id, self.bot_name))
                                            await send_webhook_log(self.bot.user.name if self.bot.user else "Unknown Node", "⚙️ Watchdog Cooldown", f"Stall persisted in `{guild.name}`; watchdog parked to prevent revival loop.", discord.Color.orange())
                                            continue
                                        current_pos = int((now - track_info.get('start_time', now)) * track_info.get('speed', 1.0) + track_info.get('offset', 0))
                                        await insert_queue_front(cur, f"{self.bot_name}_queue", guild.id, self.bot_name, track_info.get('url', ''), track_info.get('title', 'Recovered Track'), track_info.get('requester_id', self.bot.user.id if self.bot.user else None))
                                        track_info['watchdog_revival_attempts'] = revival_attempts + 1
                                        track_info['last_watchdog_revival'] = now
                                        track_info['start_time'] = now
                                        track_info['offset'] = current_pos
                                        await send_webhook_log(self.bot.user.name if self.bot.user else "Unknown Node", "⚙️ Watchdog Revival", f"Detected playback stall in `{guild.name}`. Recovering track safely at {current_pos}s.", discord.Color.orange())
                                        self.bot.loop.create_task(process_queue(guild, track_info.get('channel_id'), start_position=current_pos))
        except: pass

    @status_updater.before_loop
    @heartbeat.before_loop
    @watchdog.before_loop
    async def before_loops(self):
        await self.bot.wait_until_ready()

async def setup_intelligence(bot):
    await bot.add_cog(SwarmIntelligence(bot))

@bot.event
async def on_ready_intelligence():
    if not bot.get_cog("SwarmIntelligence"): await setup_intelligence(bot)
bot.add_listener(on_ready_intelligence, 'on_ready')

@bot.event
async def on_ready_auto_heal():
    if not auto_heal_loop.is_running():
        auto_heal_loop.start()
bot.add_listener(on_ready_auto_heal, 'on_ready')

@bot.tree.interaction_check
async def global_proximity_shield(interaction: discord.Interaction):
    admin_only = ['sethome', 'setfeedback', 'djrole', 'removedj', 'djmode', '247', 'restart']
    protected = ['play', 'stop', 'pause', 'resume', 'skip', 'join', 'leave', 'playnext', 'shuffle', 'clear', 'skipto', 'move', 'remove', 'seek', 'forward', 'rewind', 'replay']
    if not interaction.command: return True
    if any(interaction.command.name.endswith(s) for s in admin_only) and not interaction.user.guild_permissions.administrator:
        raise discord.app_commands.CheckFailure("You need administrator permission to use this command.")
    if not any(interaction.command.name.endswith(s) for s in protected): return True
    vc = interaction.guild.voice_client if interaction.guild else None
    if not vc: return True
    if getattr(vc, 'channel', None) and (not interaction.user.voice or interaction.user.voice.channel != vc.channel):
        raise discord.app_commands.AppCommandError("You must be in the active voice channel to issue commands.")
    return True

# --- BOT RUN TRIGGER ---
def validate_runtime_config():
    required = {
        f"{BOT_ENV_PREFIX}_DISCORD_TOKEN": TOKEN,
        f"{BOT_ENV_PREFIX}_DB_PASSWORD": DB_CONFIG.get('password'),
        f"{BOT_ENV_PREFIX}_LAVALINK_PASSWORD": LAVALINK_PASSWORD,
    }
    missing = [name for name, value in required.items() if not value]
    if missing: raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

def main():
    validate_runtime_config()
    bot.run(TOKEN)

if __name__ == "__main__":
    main()
