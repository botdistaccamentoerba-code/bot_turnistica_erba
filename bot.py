import logging
import sqlite3
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, ContextTypes, filters
from datetime import datetime, timedelta
import asyncio
import os
from flask import Flask
import threading
import requests
import time
import base64
import json
import csv
from io import StringIO, BytesIO
from telegram.error import BadRequest
import re

# === CONFIGURAZIONE ===
DATABASE_NAME = 'turni_vvf.db'
BOT_TOKEN = os.environ.get('BOT_TOKEN')
SUPER_USER_IDS = [1816045269]  # Tu come super user
ADMIN_IDS = [1816045269, 653425963]  # Admin (includi te stesso)
USER_IDS = []  # Verrà popolato dal database

# Configurazione backup GitHub
GITHUB_TOKEN = os.environ.get('GITHUB_TOKEN')
GIST_ID = os.environ.get('GIST_ID')

# Configurazione squadre
SQUADRE_NOTTURNE = ["An", "Bn", "Cn", "S1n", "S2n"]
SQUADRE_SERALI = ["S1", "S2", "S3", "S4", "S5", "S6", "S7"]
SQUADRE_FESTIVE = ["A", "B", "C", "D"]

# Sequenze di turni
SEQUENZA_SERALE = ["S1", "S2", "S3", "S4", "S5", "S6", "S7"]
SEQUENZA_NOTTURNA_FERIALE = ["An", "Bn", "Cn"]  # Lun-Ven
SEQUENZA_NOTTURNA_WEEKEND = ["S1n", "S2n"]  # Ven-Sab e Sab-Dom alternati
SEQUENZA_FESTIVA = ["A", "B", "C", "D"]

# Tipi di turno
TIPI_TURNO = ["notte", "sera", "festivo", "festa_nazionale", "ore_singole"]

# Data di inizio calendario (1 Novembre 2025)
DATA_INIZIO_CALENDARIO = datetime(2025, 11, 1).date()

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

# === DATABASE ===
def init_db():
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()

    # Tabella utenti
    c.execute('''CREATE TABLE IF NOT EXISTS utenti
                 (user_id INTEGER PRIMARY KEY,
                  username TEXT,
                  nome TEXT,
                  cognome TEXT,
                  qualifica TEXT,
                  grado_patente_terrestre TEXT,
                  patente_nautica BOOLEAN DEFAULT 0,
                  saf BOOLEAN DEFAULT 0,
                  tpss BOOLEAN DEFAULT 0,
                  atp BOOLEAN DEFAULT 0,
                  squadra_notte TEXT,
                  squadra_sera TEXT,
                  squadra_festiva TEXT,
                  ruolo TEXT DEFAULT 'in_attesa',
                  data_richiesta TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                  data_approvazione TIMESTAMP)''')

    # Tabella turni
    c.execute('''CREATE TABLE IF NOT EXISTS turni
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  data DATE,
                  tipo_turno TEXT,
                  squadra TEXT,
                  descrizione TEXT,
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')

    # Tabella cambi
    c.execute('''CREATE TABLE IF NOT EXISTS cambi
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  user_id_da INTEGER,
                  user_id_a INTEGER,
                  turno_id INTEGER,
                  tipo_scambio TEXT, -- 'dare', 'ricevere', 'scambiare'
                  stato TEXT DEFAULT 'pending', -- 'pending', 'confermato', 'completato'
                  data_creazione TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                  FOREIGN KEY (user_id_da) REFERENCES utenti (user_id),
                  FOREIGN KEY (user_id_a) REFERENCES utenti (user_id),
                  FOREIGN KEY (turno_id) REFERENCES turni (id))''')

    # Tabella feste_nazionali
    c.execute('''CREATE TABLE IF NOT EXISTS feste_nazionali
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  data DATE,
                  nome_festa TEXT,
                  squadra TEXT,
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')

    # Inserisci super user e admin
    for admin_id in ADMIN_IDS:
        ruolo = 'super_user' if admin_id in SUPER_USER_IDS else 'admin'
        c.execute('''INSERT OR IGNORE INTO utenti 
                     (user_id, nome, cognome, qualifica, grado_patente_terrestre, 
                      patente_nautica, saf, tpss, atp, squadra_notte, squadra_sera, squadra_festiva, ruolo, data_approvazione) 
                     VALUES (?, 'Admin', 'Admin', 'VV', 'IIIE', 0, 0, 0, 0, 'Bn', 'S7', 'D', ?, CURRENT_TIMESTAMP)''', 
                     (admin_id, ruolo))

    conn.commit()
    conn.close()
    
    # Genera il calendario automatico
    genera_calendario_automatico()

init_db()

# === GENERAZIONE CALENDARIO AUTOMATICO ===
def genera_calendario_automatico():
    """Genera automaticamente il calendario dei turni per i prossimi 5 anni"""
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    
    # Verifica se il calendario è già stato generato
    c.execute("SELECT COUNT(*) FROM turni")
    count = c.fetchone()[0]
    
    if count > 0:
        conn.close()
        return  # Calendario già generato
    
    print("🔄 Generazione calendario automatico per 5 anni...")
    
    data_corrente = DATA_INIZIO_CALENDARIO
    data_fine = data_corrente + timedelta(days=5*365)  # 5 anni
    
    # Indici per le sequenze cicliche
    idx_serale = 2  # Inizia con S3 (dato di partenza)
    idx_notturno_feriale = 0  # Inizia con An
    idx_notturno_weekend = 1  # Inizia con S2n (dato di partenza: ven-sab = S2n)
    idx_festivo = 2  # Inizia con C (dato di partenza)
    
    while data_corrente <= data_fine:
        giorno_settimana = data_corrente.weekday()  # 0=lun, 1=mar, ..., 6=dom
        
        # TURNI SERALI (tutti i giorni)
        squadra_sera = SEQUENZA_SERALE[idx_serale % len(SEQUENZA_SERALE)]
        c.execute('''INSERT OR IGNORE INTO turni (data, tipo_turno, squadra, descrizione)
                     VALUES (?, 'sera', ?, ?)''',
                 (data_corrente.isoformat(), squadra_sera, f"Turno serale {squadra_sera}"))
        
        # TURNI NOTTURNI
        if giorno_settimana in [4]:  # Venerdì (notte ven-sab)
            squadra_notte = SEQUENZA_NOTTURNA_WEEKEND[idx_notturno_weekend % len(SEQUENZA_NOTTURNA_WEEKEND)]
            c.execute('''INSERT OR IGNORE INTO turni (data, tipo_turno, squadra, descrizione)
                         VALUES (?, 'notte', ?, ?)''',
                     (data_corrente.isoformat(), squadra_notte, f"Turno notte {squadra_notte}"))
            idx_notturno_weekend += 1
            
        elif giorno_settimana in [5]:  # Sabato (notte sab-dom)
            # Alterna tra S1n e S2n per le notti weekend
            squadra_notte = SEQUENZA_NOTTURNA_WEEKEND[(idx_notturno_weekend - 1) % len(SEQUENZA_NOTTURNA_WEEKEND)]
            c.execute('''INSERT OR IGNORE INTO turni (data, tipo_turno, squadra, descrizione)
                         VALUES (?, 'notte', ?, ?)''',
                     (data_corrente.isoformat(), squadra_notte, f"Turno notte {squadra_notte}"))
            
        elif giorno_settimana in [0, 1, 2, 3]:  # Lun-Gio (notti feriali)
            squadra_notte = SEQUENZA_NOTTURNA_FERIALE[idx_notturno_feriale % len(SEQUENZA_NOTTURNA_FERIALE)]
            c.execute('''INSERT OR IGNORE INTO turni (data, tipo_turno, squadra, descrizione)
                         VALUES (?, 'notte', ?, ?)''',
                     (data_corrente.isoformat(), squadra_notte, f"Turno notte {squadra_notte}"))
            idx_notturno_feriale += 1
        
        # TURNI FESTIVI (sabato e domenica)
        if giorno_settimana in [5, 6]:  # Sabato e Domenica
            squadra_festiva = SEQUENZA_FESTIVA[idx_festivo % len(SEQUENZA_FESTIVA)]
            c.execute('''INSERT OR IGNORE INTO turni (data, tipo_turno, squadra, descrizione)
                         VALUES (?, 'festivo', ?, ?)''',
                     (data_corrente.isoformat(), squadra_festiva, f"Turno festivo {squadra_festiva}"))
        
        # Avanza gli indici
        idx_serale += 1
        
        # Il festivo avanza solo il sabato (per coprire tutto il weekend)
        if giorno_settimana == 5:  # Sabato
            idx_festivo += 1
        
        data_corrente += timedelta(days=1)
    
    # Inserisci feste nazionali di esempio (da personalizzare)
    feste_esempio = [
        ('2025-01-01', 'Capodanno', 'A'),
        ('2025-01-06', 'Epifania', 'B'),
        ('2025-04-25', 'Liberazione', 'C'),
        ('2025-05-01', 'Festa dei Lavoratori', 'D'),
        ('2025-06-02', 'Festa della Repubblica', 'A'),
        ('2025-08-15', 'Ferragosto', 'B'),
        ('2025-11-01', 'Ognissanti', 'C'),
        ('2025-12-08', 'Immacolata', 'D'),
        ('2025-12-25', 'Natale', 'A'),
        ('2025-12-26', 'Santo Stefano', 'B'),
    ]
    
    for data_festa, nome, squadra in feste_esempio:
        c.execute('''INSERT OR IGNORE INTO feste_nazionali (data, nome_festa, squadra)
                     VALUES (?, ?, ?)''', (data_festa, nome, squadra))
        
        # Aggiungi anche come turno
        c.execute('''INSERT OR IGNORE INTO turni (data, tipo_turno, squadra, descrizione)
                     VALUES (?, 'festa_nazionale', ?, ?)''',
                 (data_festa, squadra, f"Festa: {nome}"))
    
    conn.commit()
    conn.close()
    print("✅ Calendario generato automaticamente per 5 anni!")

# === FUNZIONI UTILITY ===
def is_super_user(user_id):
    return user_id in SUPER_USER_IDS

def is_admin(user_id):
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    c.execute("SELECT ruolo FROM utenti WHERE user_id = ?", (user_id,))
    result = c.fetchone()
    conn.close()
    return result and result[0] in ['super_user', 'admin']

def is_user_approved(user_id):
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    c.execute("SELECT ruolo FROM utenti WHERE user_id = ? AND ruolo IN ('super_user', 'admin', 'user')", (user_id,))
    result = c.fetchone()
    conn.close()
    return result is not None

def get_user_squadre(user_id):
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    c.execute("SELECT squadra_notte, squadra_sera, squadra_festiva FROM utenti WHERE user_id = ?", (user_id,))
    result = c.fetchone()
    conn.close()
    return result if result else (None, None, None)

def get_user_nome(user_id):
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    c.execute("SELECT nome, cognome FROM utenti WHERE user_id = ?", (user_id,))
    result = c.fetchone()
    conn.close()
    if result:
        return f"{result[0]} {result[1]}"
    return f"User_{user_id}"

def get_richieste_in_attesa():
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    c.execute('''SELECT user_id, username, nome, cognome, telefono, data_richiesta 
                 FROM utenti WHERE ruolo = 'in_attesa' ORDER BY data_richiesta''')
    result = c.fetchall()
    conn.close()
    return result

def get_utenti_approvati():
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    c.execute('''SELECT user_id, username, nome, cognome, telefono, ruolo, data_approvazione,
                 squadra_notte, squadra_sera, squadra_festiva
                 FROM utenti WHERE ruolo IN ('super_user', 'admin', 'user') ORDER BY cognome, nome''')
    result = c.fetchall()
    conn.close()
    return result

def get_vigili_completo():
    """Restituisce tutti i vigili con tutti i dati per CSV"""
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    c.execute('''SELECT nome, cognome, qualifica, grado_patente_terrestre, 
                 patente_nautica, saf, tpss, atp, squadra_notte, squadra_sera, squadra_festiva
                 FROM utenti WHERE ruolo IN ('super_user', 'admin', 'user') 
                 ORDER BY cognome, nome''')
    result = c.fetchall()
    conn.close()
    return result

def approva_utente(user_id):
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    c.execute('''UPDATE utenti SET ruolo = 'user', data_approvazione = CURRENT_TIMESTAMP 
                 WHERE user_id = ?''', (user_id,))
    conn.commit()
    conn.close()

def rimuovi_utente(user_id):
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    c.execute("DELETE FROM utenti WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()

def aggiorna_squadre_utente(user_id, squadra_notte, squadra_sera, squadra_festiva):
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    c.execute('''UPDATE utenti SET squadra_notte = ?, squadra_sera = ?, squadra_festiva = ?
                 WHERE user_id = ?''', (squadra_notte, squadra_sera, squadra_festiva, user_id))
    conn.commit()
    conn.close()

# === FUNZIONI TURNI E CALENDARIO ===
def get_turni_per_data(data):
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    c.execute("SELECT * FROM turni WHERE data = ? ORDER BY tipo_turno", (data,))
    result = c.fetchall()
    conn.close()
    return result

def get_turni_per_squadra(squadra):
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    c.execute("SELECT * FROM turni WHERE squadra = ? ORDER BY data", (squadra,))
    result = c.fetchall()
    conn.close()
    return result

def get_turni_futuri_per_utente(user_id):
    squadra_notte, squadra_sera, squadra_festiva = get_user_squadre(user_id)
    oggi = datetime.now().date()
    
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    
    # Cerca turni per le squadre dell'utente
    query = '''SELECT * FROM turni 
               WHERE squadra IN (?, ?, ?) AND data >= ?
               ORDER BY data LIMIT 20'''
    c.execute(query, (squadra_notte, squadra_sera, squadra_festiva, oggi))
    turni_diretti = c.fetchall()
    
    # Cerca cambi pendenti per l'utente
    query = '''SELECT t.*, c.tipo_scambio, c.user_id_da, c.user_id_a
               FROM cambi c
               JOIN turni t ON c.turno_id = t.id
               WHERE (c.user_id_da = ? OR c.user_id_a = ?) AND c.stato = 'pending'
               ORDER BY t.data'''
    c.execute(query, (user_id, user_id))
    cambi_pendenti = c.fetchall()
    
    conn.close()
    
    return turni_diretti, cambi_pendenti

def get_prossimi_turni_utente(user_id):
    squadra_notte, squadra_sera, squadra_festiva = get_user_squadre(user_id)
    oggi = datetime.now().date()
    
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    
    # Prossime 2 sere
    c.execute('''SELECT * FROM turni 
                 WHERE squadra = ? AND tipo_turno = 'sera' AND data >= ?
                 ORDER BY data LIMIT 2''', (squadra_sera, oggi))
    prossime_sere = c.fetchall()
    
    # Prossime 2 notti
    c.execute('''SELECT * FROM turni 
                 WHERE squadra = ? AND tipo_turno = 'notte' AND data >= ?
                 ORDER BY data LIMIT 2''', (squadra_notte, oggi))
    prossime_notti = c.fetchall()
    
    # Prossimo turno festivo
    c.execute('''SELECT * FROM turni 
                 WHERE squadra = ? AND tipo_turno = 'festivo' AND data >= ?
                 ORDER BY data LIMIT 1''', (squadra_festiva, oggi))
    prossimo_festivo = c.fetchone()
    
    # Prossima festa nazionale
    c.execute('''SELECT * FROM feste_nazionali 
                 WHERE data >= ? ORDER BY data LIMIT 1''', (oggi,))
    prossima_festa = c.fetchone()
    
    conn.close()
    
    return {
        'sere': prossime_sere,
        'notti': prossime_notti,
        'festivo': prossimo_festivo,
        'festa_nazionale': prossima_festa
    }

def get_cambi_pendenti_utente(user_id):
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    
    # Cambi che devo cedere
    c.execute('''SELECT c.*, t.data, t.tipo_turno, u.nome as nome_a
                 FROM cambi c
                 JOIN turni t ON c.turno_id = t.id
                 JOIN utenti u ON c.user_id_a = u.user_id
                 WHERE c.user_id_da = ? AND c.stato = 'pending'
                 ORDER BY t.data''', (user_id,))
    cambi_da_cedere = c.fetchall()
    
    # Cambi che devo ricevere
    c.execute('''SELECT c.*, t.data, t.tipo_turno, u.nome as nome_da
                 FROM cambi c
                 JOIN turni t ON c.turno_id = t.id
                 JOIN utenti u ON c.user_id_da = u.user_id
                 WHERE c.user_id_a = ? AND c.stato = 'pending'
                 ORDER BY t.data''', (user_id,))
    cambi_da_ricevere = c.fetchall()
    
    conn.close()
    
    return cambi_da_cedere, cambi_da_ricevere

def formatta_data_per_visualizzazione(data_str):
    """Converte la data dal formato DB a quello di visualizzazione"""
    try:
        data = datetime.strptime(data_str, '%Y-%m-%d')
        return data.strftime('%d/%m/%Y')
    except:
        return data_str

def formatta_turno_notte_per_visualizzazione(data_str, squadra):
    """Formatta i turni notte come richiesto: 'S1n - venerdì 31 ottobre su sabato 01 novembre'"""
    try:
        data = datetime.strptime(data_str, '%Y-%m-%d')
        giorno_precedente = data - timedelta(days=1)
        
        # Nomi dei giorni in italiano
        giorni_settimana = ['lunedì', 'martedì', 'mercoledì', 'giovedì', 'venerdì', 'sabato', 'domenica']
        mesi_italiano = ['gennaio', 'febbraio', 'marzo', 'aprile', 'maggio', 'giugno',
                        'luglio', 'agosto', 'settembre', 'ottobre', 'novembre', 'dicembre']
        
        giorno_prec_nome = giorni_settimana[giorno_precedente.weekday()]
        giorno_corrente_nome = giorni_settimana[data.weekday()]
        
        return f"{squadra} - {giorno_prec_nome} {giorno_precedente.day} {mesi_italiano[giorno_precedente.month-1]} su {giorno_corrente_nome} {data.day} {mesi_italiano[data.month-1]}"
    except:
        return f"{squadra} - {data_str}"

# === GESTIONE CAMBI ===
def crea_cambio(user_id_da, user_id_a, turno_id, tipo_scambio):
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    c.execute('''INSERT INTO cambi (user_id_da, user_id_a, turno_id, tipo_scambio)
                 VALUES (?, ?, ?, ?)''', (user_id_da, user_id_a, turno_id, tipo_scambio))
    cambio_id = c.lastrowid
    conn.commit()
    conn.close()
    return cambio_id

def get_turni_utente_per_tipo(user_id, tipo_turno):
    squadra_notte, squadra_sera, squadra_festiva = get_user_squadre(user_id)
    oggi = datetime.now().date()
    
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    
    squadra = None
    if tipo_turno == 'notte':
        squadra = squadra_notte
    elif tipo_turno == 'sera':
        squadra = squadra_sera
    elif tipo_turno == 'festivo':
        squadra = squadra_festiva
    
    if squadra:
        c.execute('''SELECT * FROM turni 
                     WHERE squadra = ? AND tipo_turno = ? AND data >= ?
                     ORDER BY data LIMIT 25''', (squadra, tipo_turno, oggi))
        result = c.fetchall()
    else:
        result = []
    
    conn.close()
    return result

# === TASTIERA FISICA CON EMOJI ===
def crea_tastiera_fisica(user_id):
    if not is_user_approved(user_id):
        return ReplyKeyboardMarkup([[KeyboardButton("🚀 Richiedi Accesso")]], resize_keyboard=True)

    tastiera = [
        [KeyboardButton("👥 Chi tocca"), KeyboardButton("📅 Prossimi turni")],
        [KeyboardButton("🔄 Aggiungi cambio"), KeyboardButton("📊 Statistiche")],
        [KeyboardButton("👥 Le mie squadre"), KeyboardButton("📤 Estrazione")],
        [KeyboardButton("/start 🔄"), KeyboardButton("🆘 Help")]
    ]

    if is_admin(user_id):
        tastiera.append([KeyboardButton("👮 gestisci richieste"), KeyboardButton("✏️ modifica cambio")])

    return ReplyKeyboardMarkup(tastiera, resize_keyboard=True, is_persistent=True)

# === HANDLER START ===
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_name = update.effective_user.first_name
    
    # Pulisci context user_data
    for key in list(context.user_data.keys()):
        del context.user_data[key]
    
    # Registra utente se non esiste
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    c.execute('''INSERT OR IGNORE INTO utenti (user_id, username, nome, ruolo) 
                 VALUES (?, ?, ?, 'in_attesa')''', 
                 (user_id, update.effective_user.username, user_name))
    conn.commit()
    conn.close()

    if not is_user_approved(user_id):
        # Notifica admin della nuova richiesta
        richieste = get_richieste_in_attesa()
        for admin_id in ADMIN_IDS:
            try:
                await context.bot.send_message(
                    admin_id,
                    f"🆕 NUOVA RICHIESTA ACCESSO BOT TURNI\n\nUser: {user_name}\nID: {user_id}\nUsername: @{update.effective_user.username}\nRichieste in attesa: {len(richieste)}"
                )
            except Exception as e:
                print(f"Errore notifica admin: {e}")

        await update.message.reply_text(
            "✅ Richiesta di accesso inviata agli amministratori.\nAttendi l'approvazione!",
            reply_markup=crea_tastiera_fisica(user_id)
        )
        return

    welcome_text = ""
    if is_super_user(user_id):
        welcome_text = f"👑 BENVENUTO SUPER USER {user_name}!"
    elif is_admin(user_id):
        welcome_text = f"👨‍💻 BENVENUTO ADMIN {user_name}!"
    else:
        welcome_text = f"👤 BENVENUTO {user_name}!"
    
    await update.message.reply_text(
        welcome_text + "\n\nUsa la tastiera in basso per navigare tra le funzioni.",
        reply_markup=crea_tastiera_fisica(user_id)
    )

# === CHI TOCCA ===
async def chi_tocca(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_user_approved(user_id):
        return
    
    oggi = datetime.now().date()
    
    # Trova il sabato della settimana corrente
    giorno_settimana = oggi.weekday()  # 0=lunedì, 6=domenica
    sabato_corrente = oggi + timedelta(days=(5 - giorno_settimana))
    
    messaggio = "👥 **CHI TOCCA OGGI E NEI PROSSIMI GIORNI**\n\n"
    
    # Turno della sera odierna
    turni_oggi = get_turni_per_data(oggi.isoformat())
    turno_sera_oggi = next((t for t in turni_oggi if t[2] == 'sera'), None)
    if turno_sera_oggi:
        messaggio += f"🌙 **Sera di oggi ({oggi.strftime('%d/%m')}):** {turno_sera_oggi[3]}\n"
    
    # Turno della notte che viene
    domani = oggi + timedelta(days=1)
    turni_domani = get_turni_per_data(domani.isoformat())
    turno_notte_domani = next((t for t in turni_domani if t[2] == 'notte'), None)
    if turno_notte_domani:
        descrizione = formatta_turno_notte_per_visualizzazione(domani.isoformat(), turno_notte_domani[3])
        messaggio += f"🌃 **Notte di stasera:** {descrizione}\n"
    
    # Turno festivo del weekend corrente
    turno_festivo_sabato = get_turni_per_data(sabato_corrente.isoformat())
    turno_festivo = next((t for t in turno_festivo_sabato if t[2] == 'festivo'), None)
    if turno_festivo:
        domenica_corrente = sabato_corrente + timedelta(days=1)
        messaggio += f"🎉 **Festivo weekend ({sabato_corrente.strftime('%d/%m')}-{domenica_corrente.strftime('%d/%m')}):** {turno_festivo[3]}\n"
    
    # Prossime 2 festività nazionali
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    c.execute('''SELECT * FROM feste_nazionali 
                 WHERE data >= ? ORDER BY data LIMIT 2''', (oggi,))
    prossime_feste = c.fetchall()
    conn.close()
    
    if prossime_feste:
        messaggio += "\n🎊 **PROSSIME FESTIVITÀ NAZIONALI:**\n"
        for festa in prossime_feste:
            data_festa = datetime.strptime(festa[1], '%Y-%m-%d').strftime('%d/%m/%Y')
            messaggio += f"• {data_festa}: {festa[2]} - Squadra: {festa[3]}\n"
    
    # Verifica se l'utente è coinvolto in qualche turno
    squadra_notte, squadra_sera, squadra_festiva = get_user_squadre(user_id)
    
    coinvolto = False
    if turno_sera_oggi and turno_sera_oggi[3] == squadra_sera:
        coinvolto = True
        messaggio += "\n🚒 **SEI DI TURNO** stasera!\n"
    
    if turno_notte_domani and turno_notte_domani[3] == squadra_notte:
        coinvolto = True
        messaggio += "\n🚒 **SEI DI TURNO** stanotte!\n"
    
    if turno_festivo and turno_festivo[3] == squadra_festiva:
        coinvolto = True
        messaggio += "\n🚒 **SEI DI TURNO** nel weekend!\n"
    
    # Controlla cambi/sostituzioni
    cambi_da_cedere, cambi_da_ricevere = get_cambi_pendenti_utente(user_id)
    if cambi_da_cedere or cambi_da_ricevere:
        messaggio += "\n🔄 **HAI CAMBI IN SOSPESO** - controlla in 'Prossimi turni'\n"
    
    await update.message.reply_text(messaggio)

# === PROSSIMI TURNI ===
async def prossimi_turni(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_user_approved(user_id):
        return
    
    prossimi = get_prossimi_turni_utente(user_id)
    cambi_da_cedere, cambi_da_ricevere = get_cambi_pendenti_utente(user_id)
    
    messaggio = "📅 **I TUOI PROSSIMI TURNI**\n\n"
    
    # Prossime 2 sere
    if prossimi['sere']:
        messaggio += "🌙 **PROSSIME 2 SERE:**\n"
        for turno in prossimi['sere']:
            data_formattata = formatta_data_per_visualizzazione(turno[1])
            messaggio += f"• {data_formattata}: {turno[3]}\n"
        messaggio += "\n"
    
    # Prossime 2 notti
    if prossimi['notti']:
        messaggio += "🌃 **PROSSIME 2 NOTTI:**\n"
        for turno in prossimi['notti']:
            descrizione = formatta_turno_notte_per_visualizzazione(turno[1], turno[3])
            messaggio += f"• {descrizione}\n"
        messaggio += "\n"
    
    # Prossimo turno festivo
    if prossimi['festivo']:
        data_festivo = formatta_data_per_visualizzazione(prossimi['festivo'][1])
        messaggio += f"🎉 **PROSSIMO FESTIVO:** {data_festivo}: {prossimi['festivo'][3]}\n\n"
    
    # Prossima festa nazionale
    if prossimi['festa_nazionale']:
        data_festa = formatta_data_per_visualizzazione(prossimi['festa_nazionale'][1])
        messaggio += f"🎊 **PROSSIMA FESTA NAZIONALE:** {data_festa}: {prossimi['festa_nazionale'][2]} - {prossimi['festa_nazionale'][3]}\n\n"
    
    # Cambi pendenti
    if cambi_da_cedere or cambi_da_ricevere:
        messaggio += "🔄 **CAMBI IN SOSPESO:**\n"
        
        if cambi_da_cedere:
            messaggio += "📤 **Da cedere a:**\n"
            for cambio in cambi_da_cedere:
                data_turno = formatta_data_per_visualizzazione(cambio[7])
                tipo_turno = cambio[8]
                nome_destinatario = cambio[9]
                messaggio += f"• {data_turno} ({tipo_turno}) → {nome_destinatario}\n"
        
        if cambi_da_ricevere:
            messaggio += "📥 **Da ricevere da:**\n"
            for cambio in cambi_da_ricevere:
                data_turno = formatta_data_per_visualizzazione(cambio[7])
                tipo_turno = cambio[8]
                nome_cedente = cambio[9]
                messaggio += f"• {data_turno} ({tipo_turno}) ← {nome_cedente}\n"
    
    if not any(prossimi.values()) and not cambi_da_cedere and not cambi_da_ricevere:
        messaggio += "🎉 Non hai turni in programma per il prossimo futuro!"
    
    await update.message.reply_text(messaggio)

# === AGGIUNGI CAMBIO ===
async def aggiungi_cambio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_user_approved(user_id):
        return
    
    # Ottieni lista utenti approvati (escludendo se stesso)
    utenti = get_utenti_approvati()
    utenti_filtrati = [u for u in utenti if u[0] != user_id]
    
    if not utenti_filtrati:
        await update.message.reply_text("❌ Non ci sono altri utenti nel sistema con cui fare cambi.")
        return
    
    keyboard = []
    for utente in utenti_filtrati[:25]:  # Limite di 25 utenti per callback
        user_id_u, username, nome, cognome, telefono, ruolo, data_approvazione, sq_notte, sq_sera, sq_festiva = utente
        display_name = f"{nome} {cognome} ({sq_notte} {sq_sera} {sq_festiva})"
        keyboard.append([InlineKeyboardButton(display_name, callback_data=f"cambio_sel_{user_id_u}")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    context.user_data['cambio'] = {'fase': 'selezione_utente'}
    
    await update.message.reply_text(
        "🔄 **AGGIUNGI CAMBIO**\n\n"
        "Seleziona la persona con cui hai concordato il cambio:",
        reply_markup=reply_markup
    )

# === STATISTICHE ===
async def statistiche(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_user_approved(user_id):
        return
    
    # Implementazione statistica semplificata
    await update.message.reply_text(
        "📊 **STATISTICHE**\n\n"
        "Funzionalità statistiche in sviluppo.\n"
        "Qui vedrai il bilancio delle sostituzioni con tutti i VVF."
    )

# === LE MIE SQUADRE ===
async def mie_squadre(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_user_approved(user_id):
        return
    
    squadra_notte, squadra_sera, squadra_festiva = get_user_squadre(user_id)
    
    keyboard = [
        [InlineKeyboardButton("👀 Visualizza", callback_data="squadre_visualizza")],
        [InlineKeyboardButton("✏️ Cambia squadra", callback_data="squadre_cambia")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    messaggio = "👥 **LE MIE SQUADRE**\n\n"
    
    if any([squadra_notte, squadra_sera, squadra_festiva]):
        messaggio += f"🌃 **Notturna:** {squadra_notte or 'Non impostata'}\n"
        messaggio += f"🌙 **Serale:** {squadra_sera or 'Non impostata'}\n"
        messaggio += f"🎉 **Festiva:** {squadra_festiva or 'Non impostata'}\n"
    else:
        messaggio += "❌ Non hai ancora impostato le tue squadre.\n"
    
    messaggio += "\nSeleziona un'opzione:"
    
    await update.message.reply_text(messaggio, reply_markup=reply_markup)

# === ESTRAZIONE DATI ===
async def estrazione_dati(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_user_approved(user_id):
        return
    
    keyboard = [
        [InlineKeyboardButton("📅 Calendario turni", callback_data="export_calendario")],
        [InlineKeyboardButton("📊 I miei turni", callback_data="export_miei_turni")],
        [InlineKeyboardButton("👥 Utenti", callback_data="export_utenti")],
        [InlineKeyboardButton("🚒 Vigili", callback_data="export_vigili")]
    ]
    
    if is_admin(user_id):
        keyboard.append([InlineKeyboardButton("🔄 Backup completo", callback_data="export_backup")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        "📤 **ESTRAZIONE DATI**\n\n"
        "Seleziona il tipo di estrazione:",
        reply_markup=reply_markup
    )

# === GESTIONE RICHIESTE ===
async def gestisci_richieste(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        return
    
    keyboard = [
        [InlineKeyboardButton("📋 Richieste in attesa", callback_data="richieste_attesa")],
        [InlineKeyboardButton("👥 Utenti approvati", callback_data="utenti_approvati")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    richieste = get_richieste_in_attesa()
    utenti = get_utenti_approvati()
    
    messaggio = "👥 **GESTIONE RICHIESTE**\n\n"
    messaggio += f"📋 Richieste in attesa: {len(richieste)}\n"
    messaggio += f"👥 Utenti approvati: {len(utenti)}\n\n"
    messaggio += "Seleziona un'operazione:"
    
    await update.message.reply_text(messaggio, reply_markup=reply_markup)

# === MODIFICA CAMBIO ===
async def modifica_cambio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        return
    
    await update.message.reply_text(
        "✏️ **MODIFICA CAMBIO**\n\n"
        "Funzionalità in sviluppo.\n"
        "Qui potrai modificare o rimuovere i cambi esistenti."
    )

# === HELP ===
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    messaggio = """🆘 **HELP - GUIDA ALL'USO**

**FUNZIONALITÀ PRINCIPALI:**

👥 **CHI TOCCA** - Mostra i turni di oggi, stanotte, il weekend e le prossime festività

📅 **PROSSIMI TURNI** - I tuoi prossimi turni e cambi in sospeso

🔄 **AGGIUNGI CAMBIO** - Organizza cambi con altri vigili

📊 **STATISTICHE** - Bilancio ore con gli altri vigili

👥 **LE MIE SQUADRE** - Visualizza o modifica le tue squadre

📤 **ESTRAZIONE** - Scarica dati in formato CSV

**PER AMMINISTRATORI:**
👮 **GESTISCI RICHIESTE** - Approva nuovi utenti
✏️ **MODIFICA CAMBIO** - Gestisci cambi esistenti

**COMANDI:**
/start - Riavvia il bot
Help - Questo messaggio

📌 **SUGGERIMENTI:**
• Usa sempre la tastiera in basso
• Segui i flussi guidati per i cambi
• Controlla sempre i dati prima di confermare
"""
    await update.message.reply_text(messaggio)

# === GESTIONE CALLBACK QUERY ===
async def gestisci_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    callback_data = query.data
    
    try:
        await query.answer()
    except BadRequest:
        return
    
    # Gestione selezione utente per cambio
    if callback_data.startswith("cambio_sel_"):
        user_id_selezionato = int(callback_data.replace("cambio_sel_", ""))
        await gestisci_selezione_utente_cambio(update, context, user_id_selezionato)
    
    # Gestione esportazione dati
    elif callback_data == "export_calendario":
        await esporta_calendario(update, context)
    elif callback_data == "export_vigili":
        await esporta_vigili(update, context)
    elif callback_data == "export_utenti":
        await esporta_utenti(update, context)
    
    # Gestione richieste admin
    elif callback_data == "richieste_attesa":
        await mostra_richieste_attesa(update, context)
    elif callback_data == "utenti_approvati":
        await mostra_utenti_approvati(update, context)
    elif callback_data.startswith("approva_"):
        user_id_approvare = int(callback_data.replace("approva_", ""))
        await approva_utente_handler(update, context, user_id_approvare)
    elif callback_data.startswith("rimuovi_"):
        user_id_rimuovere = int(callback_data.replace("rimuovi_", ""))
        await conferma_rimozione_utente(update, context, user_id_rimuovere)
    
    # Gestione squadre
    elif callback_data == "squadre_visualizza":
        await visualizza_squadre(update, context)
    elif callback_data == "squadre_cambia":
        await cambia_squadra(update, context)

async def gestisci_selezione_utente_cambio(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id_selezionato: int):
    query = update.callback_query
    try:
        await query.answer()
    except BadRequest:
        return
    
    context.user_data['cambio']['user_id_a'] = user_id_selezionato
    context.user_data['cambio']['fase'] = 'tipo_scambio'
    
    nome_utente = get_user_nome(user_id_selezionato)
    
    keyboard = [
        [
            InlineKeyboardButton("📤 Dare turno", callback_data="scambio_dare"),
            InlineKeyboardButton("📥 Ricevere turno", callback_data="scambio_ricevere")
        ],
        [InlineKeyboardButton("🔄 Scambiare turno", callback_data="scambio_scambiare")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        f"🔄 **TIPO DI SCAMBIO**\n\n"
        f"Con: {nome_utente}\n\n"
        f"Seleziona il tipo di scambio:",
        reply_markup=reply_markup
    )

# === ESPORTAZIONE DATI ===
async def esporta_calendario(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    try:
        await query.answer()
    except BadRequest:
        return
    
    # Chiedi l'anno per l'esportazione
    anno_corrente = datetime.now().year
    keyboard = []
    
    for anno in range(anno_corrente, anno_corrente + 6):  # 5 anni + corrente
        keyboard.append([InlineKeyboardButton(str(anno), callback_data=f"export_cal_{anno}")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        "📅 **ESPORTA CALENDARIO**\n\n"
        "Seleziona l'anno da esportare:",
        reply_markup=reply_markup
    )

async def esporta_vigili(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    try:
        await query.answer()
    except BadRequest:
        return
    
    try:
        vigili = get_vigili_completo()
        
        output = StringIO()
        writer = csv.writer(output)
        
        # Header conforme alla richiesta
        writer.writerow(['nome', 'cognome', 'qualifica', 'grado_patente_terrestre', 
                        'patente_nautica', 'saf', 'tpss', 'atp', 'Sq_Notte', 'Sq_Sera', 'Sq_Feste'])
        
        for vigile in vigili:
            nome, cognome, qualifica, grado_patente, patente_nautica, saf, tpss, atp, sq_notte, sq_sera, sq_feste = vigile
            
            # Converti boolean in 0/1
            patente_nautica = 1 if patente_nautica else 0
            saf = 1 if saf else 0
            tpss = 1 if tpss else 0
            atp = 1 if atp else 0
            
            writer.writerow([
                nome, cognome, qualifica, grado_patente,
                patente_nautica, saf, tpss, atp,
                sq_notte, sq_sera, sq_feste
            ])
        
        csv_data = output.getvalue()
        output.close()
        
        csv_bytes = csv_data.encode('utf-8')
        csv_file = BytesIO(csv_bytes)
        csv_file.name = f"vigili_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        
        await query.edit_message_text("📤 Generazione file Vigili in corso...")
        await context.bot.send_document(
            chat_id=query.message.chat_id,
            document=csv_file,
            filename=csv_file.name,
            caption="🚒 **VIGILI**\n\nFile CSV contenente l'elenco completo dei vigili con squadre."
        )
        
    except Exception as e:
        await query.edit_message_text(f"❌ Errore durante l'esportazione: {str(e)}")

async def esporta_utenti(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    try:
        await query.answer()
    except BadRequest:
        return
    
    try:
        utenti = get_utenti_approvati()
        
        output = StringIO()
        writer = csv.writer(output)
        
        writer.writerow(['user_id', 'username', 'nome', 'cognome', 'telefono', 'ruolo', 'data_approvazione'])
        
        for utente in utenti:
            user_id, username, nome, cognome, telefono, ruolo, data_approvazione, sq_notte, sq_sera, sq_festiva = utente
            writer.writerow([
                user_id,
                username or '',
                nome or '',
                cognome or '',
                telefono or '',
                ruolo,
                data_approvazione or ''
            ])
        
        csv_data = output.getvalue()
        output.close()
        
        csv_bytes = csv_data.encode('utf-8')
        csv_file = BytesIO(csv_bytes)
        csv_file.name = f"utenti_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        
        await query.edit_message_text("📤 Generazione file Utenti in corso...")
        await context.bot.send_document(
            chat_id=query.message.chat_id,
            document=csv_file,
            filename=csv_file.name,
            caption="👤 **UTENTI**\n\nFile CSV contenente l'elenco degli utenti approvati."
        )
        
    except Exception as e:
        await query.edit_message_text(f"❌ Errore durante l'esportazione: {str(e)}")

# === GESTIONE RICHIESTE ADMIN ===
async def mostra_richieste_attesa(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    richieste = get_richieste_in_attesa()
    
    if not richieste:
        await query.edit_message_text("✅ Nessuna richiesta di accesso in sospeso.")
        return

    prima_richiesta = richieste[0]
    user_id_rich, username, nome, cognome, telefono, data_richiesta = prima_richiesta
    
    keyboard = [
        [
            InlineKeyboardButton("✅ Approva", callback_data=f"approva_{user_id_rich}"),
            InlineKeyboardButton("❌ Rifiuta", callback_data=f"rimuovi_{user_id_rich}")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        f"👤 **RICHIESTA ACCESSO**\n\n"
        f"🆔 ID: {user_id_rich}\n"
        f"👤 Nome: {nome} {cognome}\n"
        f"📱 Username: @{username}\n"
        f"📞 Telefono: {telefono or 'Non fornito'}\n"
        f"📅 Data: {data_richiesta}",
        reply_markup=reply_markup
    )

async def approva_utente_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
    query = update.callback_query
    approva_utente(user_id)
    
    # Notifica l'utente approvato
    try:
        await context.bot.send_message(
            user_id,
            "✅ **ACCESSO APPROVATO!**\n\n"
            "La tua richiesta di accesso al bot dei turni è stata approvata.\n"
            "Usa /start per iniziare!"
        )
    except:
        pass
    
    await query.edit_message_text(f"✅ Utente {user_id} approvato con successo!")

async def conferma_rimozione_utente(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
    query = update.callback_query
    rimuovi_utente(user_id)
    await query.edit_message_text(f"❌ Utente {user_id} rimosso.")

async def visualizza_squadre(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    
    squadra_notte, squadra_sera, squadra_festiva = get_user_squadre(user_id)
    
    messaggio = "👥 **LE TUE SQUADRE**\n\n"
    messaggio += f"🌃 **Squadra notturna:** {squadra_notte or 'Non impostata'}\n"
    messaggio += f"🌙 **Squadra serale:** {squadra_sera or 'Non impostata'}\n"
    messaggio += f"🎉 **Squadra festiva:** {squadra_festiva or 'Non impostata'}\n\n"
    messaggio += "Usa 'Cambia squadra' per modificare."
    
    await query.edit_message_text(messaggio)

async def cambia_squadra(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    context.user_data['cambia_squadra'] = {'fase': 'notte'}
    
    keyboard = []
    for squadra in SQUADRE_NOTTURNE:
        keyboard.append([InlineKeyboardButton(squadra, callback_data=f"squadra_notte_{squadra}")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        "🌃 **CAMBIASQUADRA NOTTURNA**\n\n"
        "Seleziona la tua squadra notturna:",
        reply_markup=reply_markup
    )

# === GESTIONE MESSAGGI DI TESTO ===
async def gestisci_messaggio_testo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    testo = update.message.text
    
    if not is_user_approved(user_id):
        if testo == "🚀 Richiedi Accesso":
            await start(update, context)
        return
    
    # Gestione comandi dalla tastiera fisica
    if testo == "👥 Chi tocca":
        await chi_tocca(update, context)
    elif testo == "📅 Prossimi turni":
        await prossimi_turni(update, context)
    elif testo == "🔄 Aggiungi cambio":
        await aggiungi_cambio(update, context)
    elif testo == "📊 Statistiche":
        await statistiche(update, context)
    elif testo == "👥 Le mie squadre":
        await mie_squadre(update, context)
    elif testo == "📤 Estrazione":
        await estrazione_dati(update, context)
    elif testo == "👮 gestisci richieste" and is_admin(user_id):
        await gestisci_richieste(update, context)
    elif testo == "✏️ modifica cambio" and is_admin(user_id):
        await modifica_cambio(update, context)
    elif testo == "/start 🔄":
        await start(update, context)
    elif testo == "🆘 Help":
        await help_command(update, context)

# === GESTIONE FILE CSV ===
async def gestisci_file_csv(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("❌ Solo gli amministratori possono importare dati.")
        return
    
    document = update.message.document
    file_name = document.file_name.lower()
    
    if not file_name.endswith('.csv'):
        await update.message.reply_text("❌ Il file deve essere in formato CSV.")
        return
    
    try:
        file = await context.bot.get_file(document.file_id)
        file_content = await file.download_as_bytearray()
        
        encodings = ['utf-8', 'latin-1', 'windows-1252', 'iso-8859-1', 'cp1252']
        csv_content = None
        
        for encoding in encodings:
            try:
                csv_content = file_content.decode(encoding).splitlines()
                print(f"✅ File decodificato con encoding: {encoding}")
                break
            except UnicodeDecodeError:
                continue
        
        if csv_content is None:
            await update.message.reply_text("❌ Impossibile decodificare il file. Usa un encoding UTF-8 valido.")
            return
        
        reader = csv.reader(csv_content)
        headers = next(reader)
        
        # Determina il tipo di CSV in base al nome del file
        if 'vigili' in file_name:
            await gestisci_import_vigili(update, context, reader)
        else:
            await update.message.reply_text(
                "❌ Impossibile determinare il tipo di CSV.\n\n"
                "I nomi dei file devono contenere:\n"
                "• 'vigili' per i vigili\n"
            )
        
    except Exception as e:
        await update.message.reply_text(f"❌ Errore durante l'importazione: {str(e)}")
        print(f"Errore dettagliato: {e}")

async def gestisci_import_vigili(update: Update, context: ContextTypes.DEFAULT_TYPE, reader):
    imported_count = 0
    updated_count = 0
    error_count = 0
    error_details = []
    
    for row_num, row in enumerate(reader, start=2):
        try:
            if len(row) < 11:
                error_count += 1
                error_details.append(f"Riga {row_num}: Numero di colonne insufficiente ({len(row)}/11)")
                continue
            
            nome = row[0]
            cognome = row[1]
            qualifica = row[2]
            grado_patente = row[3]
            patente_nautica = bool(int(row[4])) if row[4] and row[4].isdigit() else False
            saf = bool(int(row[5])) if row[5] and row[5].isdigit() else False
            tpss = bool(int(row[6])) if row[6] and row[6].isdigit() else False
            atp = bool(int(row[7])) if row[7] and row[7].isdigit() else False
            squadra_notte = row[8] if len(row) > 8 else None
            squadra_sera = row[9] if len(row) > 9 else None
            squadra_festiva = row[10] if len(row) > 10 else None
            
            # Cerca se il vigile esiste già (per nome e cognome)
            conn = sqlite3.connect(DATABASE_NAME)
            c = conn.cursor()
            c.execute("SELECT user_id FROM utenti WHERE nome = ? AND cognome = ?", (nome, cognome))
            existing_vigile = c.fetchone()
            
            if existing_vigile:
                # Aggiorna il vigile esistente
                user_id = existing_vigile[0]
                c.execute('''UPDATE utenti SET 
                            qualifica = ?, grado_patente_terrestre = ?, patente_nautica = ?, 
                            saf = ?, tpss = ?, atp = ?, squadra_notte = ?, squadra_sera = ?, squadra_festiva = ?
                            WHERE user_id = ?''',
                         (qualifica, grado_patente, patente_nautica, saf, tpss, atp, 
                          squadra_notte, squadra_sera, squadra_festiva, user_id))
                updated_count += 1
            else:
                # Inserisce nuovo vigile (senza user_id, sarà un record "fantasma" fino a quando non si registra)
                c.execute('''INSERT INTO utenti 
                            (nome, cognome, qualifica, grado_patente_terrestre, patente_nautica, saf, tpss, atp, 
                             squadra_notte, squadra_sera, squadra_festiva, ruolo) 
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'user')''',
                         (nome, cognome, qualifica, grado_patente, patente_nautica, saf, tpss, atp,
                          squadra_notte, squadra_sera, squadra_festiva))
                imported_count += 1
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            error_count += 1
            error_details.append(f"Riga {row_num}: {str(e)}")
            continue
    
    messaggio = f"✅ **IMPORTAZIONE VIGILI COMPLETATA**\n\n"
    messaggio += f"📊 **Risultati:**\n"
    messaggio += f"• ✅ Vigili importati: {imported_count}\n"
    messaggio += f"• 🔄 Vigili aggiornati: {updated_count}\n"
    messaggio += f"• ❌ Errori: {error_count}\n\n"
    
    if error_details:
        messaggio += "📋 **Dettagli errori (prime 5):**\n"
        for detail in error_details[:5]:
            messaggio += f"• {detail}\n"
    
    await update.message.reply_text(messaggio)

# === SISTEMA BACKUP GITHUB ===
def backup_database_to_gist():
    if not GITHUB_TOKEN:
        print("❌ Token GitHub non configurato - backup disabilitato")
        return False
    
    try:
        with open(DATABASE_NAME, 'rb') as f:
            db_content = f.read()
        
        db_base64 = base64.b64encode(db_content).decode('utf-8')
        
        files = {
            'turni_vvf_backup.json': {
                'content': json.dumps({
                    'timestamp': datetime.now().isoformat(),
                    'database_size': len(db_content),
                    'database_base64': db_base64,
                    'backup_type': 'automatic'
                })
            }
        }
        
        headers = {
            'Authorization': f'token {GITHUB_TOKEN}',
            'Accept': 'application/vnd.github.v3+json'
        }
        
        if GIST_ID:
            url = f'https://api.github.com/gists/{GIST_ID}'
            data = {'files': files}
            response = requests.patch(url, headers=headers, json=data)
        else:
            url = 'https://api.github.com/gists'
            data = {
                'description': f'Backup Turni VVF - {datetime.now().strftime("%Y-%m-%d %H:%M")}',
                'public': False,
                'files': files
            }
            response = requests.post(url, headers=headers, json=data)
        
        if response.status_code in [200, 201]:
            print("✅ Backup su Gist completato")
            return True
        else:
            print(f"❌ Errore backup Gist: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Errore durante backup: {str(e)}")
        return False

def restore_database_from_gist():
    if not GITHUB_TOKEN or not GIST_ID:
        print("❌ Token o Gist ID non configurati - restore disabilitato")
        return False
    
    try:
        headers = {
            'Authorization': f'token {GITHUB_TOKEN}',
            'Accept': 'application/vnd.github.v3+json'
        }
        
        url = f'https://api.github.com/gists/{GIST_ID}'
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            gist_data = response.json()
            backup_file = gist_data['files'].get('turni_vvf_backup.json')
            
            if backup_file:
                backup_content = json.loads(backup_file['content'])
                db_base64 = backup_content['database_base64']
                
                db_content = base64.b64decode(db_base64)
                with open(DATABASE_NAME, 'wb') as f:
                    f.write(db_content)
                
                print("✅ Database ripristinato da backup")
                return True
        return False
            
    except Exception as e:
        print(f"❌ Errore durante restore: {str(e)}")
        return False

# === SERVER FLASK PER RENDER ===
app = Flask(__name__)

@app.route('/')
def home():
    return "🤖 Bot Turni VVF - ONLINE 🟢"

@app.route('/health')
def health():
    return "OK"

@app.route('/backup')
def backup_manual():
    if backup_database_to_gist():
        return "✅ Backup effettuato"
    else:
        return "❌ Errore backup"

def run_flask():
    app.run(host='0.0.0.0', port=10000, debug=False)

# === MAIN ===
def main():
    # Ripristino da backup se disponibile
    print("🔄 Verifica backup...")
    restore_database_from_gist()
    
    # Avvia server Flask in thread separato
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    
    # Avvia backup scheduler
    def backup_scheduler():
        while True:
            time.sleep(1800)  # Backup ogni 30 minuti
            backup_database_to_gist()
    
    backup_thread = threading.Thread(target=backup_scheduler, daemon=True)
    backup_thread.start()
    
    # Crea application
    application = Application.builder().token(BOT_TOKEN).build()
    
    # Aggiungi handler
    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, gestisci_messaggio_testo))
    application.add_handler(MessageHandler(filters.Document.ALL, gestisci_file_csv))
    application.add_handler(CallbackQueryHandler(gestisci_callback))
    
    # Avvia bot
    print("🤖 Bot Turni VVF avviato!")
    print("✅ Calendario generato automaticamente per 5 anni")
    print("✅ Tastiera fisica con emoji")
    print("✅ Sistema backup attivo")
    application.run_polling()

if __name__ == '__main__':
    main()
