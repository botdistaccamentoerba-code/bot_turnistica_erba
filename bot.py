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

# Sequenze di turni - CORRETTE basate sul PDF
SEQUENZA_SERALE = ["S1", "S2", "S3", "S4", "S5", "S6", "S7"]
SEQUENZA_NOTTURNA_FERIALE = ["An", "Bn", "Cn"]  # Lun-Gio
SEQUENZA_NOTTURNA_WEEKEND = ["S1n", "S2n"]  # Ven-Sab alternati
SEQUENZA_FESTIVA = ["A", "B", "C", "D"]

# Tipi di turno
TIPI_TURNO = ["notte", "sera", "festivo", "festa_nazionale", "ore_singole"]

# Data di inizio calendario (1 Novembre 2025)
DATA_INIZIO_CALENDARIO = datetime(2025, 11, 1).date()

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

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
    
    # Indici per le sequenze cicliche - CORRETTI basati sul PDF di novembre
    # Partenza basata sul PDF: 1 nov (sab) = festivo C, sera S2, notte S2n
    # 2 nov (dom) = festivo C, sera S3
    # 3 nov (lun) = notte Cn, sera S4
    idx_serale = 2  # Inizia con S3 (domenica 2 nov)
    idx_notturno_feriale = 2  # Inizia con Cn (lunedì 3 nov)
    idx_notturno_weekend = 1  # Inizia con S2n (sabato 1 nov) - poi prossimo sarà S1n
    idx_festivo = 2  # Inizia con C (sabato 1 nov)
    
    while data_corrente <= data_fine:
        giorno_settimana = data_corrente.weekday()  # 0=lun, 1=mar, ..., 6=dom
        
        # TURNI SERALI (tutti i giorni tranne sabato che è festivo)
        if giorno_settimana != 5:  # Non sabato
            squadra_sera = SEQUENZA_SERALE[idx_serale % len(SEQUENZA_SERALE)]
            c.execute('''INSERT OR IGNORE INTO turni (data, tipo_turno, squadra, descrizione)
                         VALUES (?, 'sera', ?, ?)''',
                     (data_corrente.isoformat(), squadra_sera, f"Turno serale {squadra_sera}"))
        
        # TURNI NOTTURNI
        if giorno_settimana == 4:  # Venerdì (notte ven-sab)
            squadra_notte = SEQUENZA_NOTTURNA_WEEKEND[idx_notturno_weekend % len(SEQUENZA_NOTTURNA_WEEKEND)]
            c.execute('''INSERT OR IGNORE INTO turni (data, tipo_turno, squadra, descrizione)
                         VALUES (?, 'notte', ?, ?)''',
                     (data_corrente.isoformat(), squadra_notte, f"Turno notte {squadra_notte}"))
            idx_notturno_weekend += 1  # Alterna S1n/S2n
            
        elif giorno_settimana in [0, 1, 2, 3]:  # Lun-Gio (notti feriali)
            squadra_notte = SEQUENZA_NOTTURNA_FERIALE[idx_notturno_feriale % len(SEQUENZA_NOTTURNA_FERIALE)]
            c.execute('''INSERT OR IGNORE INTO turni (data, tipo_turno, squadra, descrizione)
                         VALUES (?, 'notte', ?, ?)''',
                     (data_corrente.isoformat(), squadra_notte, f"Turno notte {squadra_notte}"))
            idx_notturno_feriale += 1
        
        # NOTTE SAB-DOM: gestita dal turno festivo (nessun turno notte separato)
        
        # TURNI FESTIVI (sabato e domenica)
        if giorno_settimana == 5:  # Sabato (festivo copre tutto il weekend)
            squadra_festiva = SEQUENZA_FESTIVA[idx_festivo % len(SEQUENZA_FESTIVA)]
            c.execute('''INSERT OR IGNORE INTO turni (data, tipo_turno, squadra, descrizione)
                         VALUES (?, 'festivo', ?, ?)''',
                     (data_corrente.isoformat(), squadra_festiva, f"Turno festivo {squadra_festiva}"))
            idx_festivo += 1
        
        # Avanza gli indici SERALI solo nei giorni feriali
        if giorno_settimana != 5:  # Non sabato
            idx_serale += 1
        data_corrente += timedelta(days=1)
    
    # Inserisci feste nazionali REALI (basate sul PDF)
    feste_reali = [
        ('2025-01-01', 'Capodanno', 'A'),
        ('2025-01-06', 'Epifania', 'B'),
        ('2025-04-25', 'Liberazione', 'C'),
        ('2025-05-01', 'Festa dei Lavoratori', 'D'),
        ('2025-06-02', 'Festa della Repubblica', 'A'),
        ('2025-08-15', 'Ferragosto', 'B'),
        ('2025-11-01', 'Ognissanti', 'C'),
        ('2025-12-08', 'Immacolata', 'D'),
        ('2025-12-25', 'Natale', 'C'),
        ('2025-12-26', 'Santo Stefano', 'D'),
        ('2026-01-01', 'Capodanno', 'A'),
    ]
    
    for data_festa, nome, squadra in feste_reali:
        c.execute('''INSERT OR IGNORE INTO feste_nazionali (data, nome_festa, squadra)
                     VALUES (?, ?, ?)''', (data_festa, nome, squadra))
        
        # Aggiungi anche come turno festivo
        c.execute('''INSERT OR IGNORE INTO turni (data, tipo_turno, squadra, descrizione)
                     VALUES (?, 'festa_nazionale', ?, ?)''',
                 (data_festa, squadra, f"Festa: {nome}"))
    
    conn.commit()
    conn.close()
    print("✅ Calendario generato automaticamente per 5 anni!")

# === DATABASE ===
def init_db():
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()

    # Tabella utenti - AGGIORNATA con colonna telefono
    c.execute('''CREATE TABLE IF NOT EXISTS utenti
                 (user_id INTEGER PRIMARY KEY,
                  username TEXT,
                  nome TEXT,
                  cognome TEXT,
                  telefono TEXT,  -- COLONNA AGGIUNTA
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
                  tipo_scambio TEXT, -- 'dare', 'ricevere', 'scambiare', 'ore_singole'
                  stato TEXT DEFAULT 'pending', -- 'pending', 'confermato', 'completato'
                  data_creazione TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                  data_ore_singole DATE,
                  ora_inizio TEXT,
                  ora_fine TEXT,
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

    # Inserisci te stesso come VVF di esempio
    c.execute('''INSERT OR IGNORE INTO utenti 
                 (user_id, nome, cognome, qualifica, grado_patente_terrestre, 
                  patente_nautica, saf, tpss, atp, squadra_notte, squadra_sera, squadra_festiva, ruolo, data_approvazione) 
                 VALUES (?, 'Rudi', 'Caverio', 'VV', 'IIIE', 0, 1, 0, 0, 'Bn', 'S7', 'D', 'super_user', CURRENT_TIMESTAMP)''', 
                 (SUPER_USER_IDS[0],))

    # Inserisci alcuni vigili di esempio per testare la funzione Squadre
    vigili_esempio = [
        ('Marco', 'Rossi', 'VV', 'IIIE', 1, 1, 0, 0, 'An', 'S1', 'A'),
        ('Luca', 'Bianchi', 'AP', 'IIE', 0, 0, 1, 0, 'Bn', 'S2', 'B'),
        ('Giulia', 'Verdi', 'VV', 'IE', 1, 0, 0, 1, 'Cn', 'S3', 'C'),
        ('Anna', 'Neri', 'AP', 'IIIE', 0, 1, 1, 0, 'S1n', 'S4', 'D'),
        ('Paolo', 'Gialli', 'VV', 'IIE', 1, 0, 0, 0, 'S2n', 'S5', 'A'),
        ('Simone', 'Blu', 'AP', 'IE', 0, 1, 0, 1, 'An', 'S6', 'B'),
        ('Elena', 'Rosa', 'VV', 'IIIE', 1, 1, 1, 0, 'Bn', 'S7', 'C'),
    ]
    
    for nome, cognome, qualifica, grado_patente, patente_nautica, saf, tpss, atp, sq_notte, sq_sera, sq_festiva in vigili_esempio:
        c.execute('''INSERT OR IGNORE INTO utenti 
                     (nome, cognome, qualifica, grado_patente_terrestre, 
                      patente_nautica, saf, tpss, atp, squadra_notte, squadra_sera, squadra_festiva, ruolo) 
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'user')''', 
                     (nome, cognome, qualifica, grado_patente, patente_nautica, saf, tpss, atp, sq_notte, sq_sera, sq_festiva))

    # Aggiorna la struttura se necessario
    try:
        c.execute("ALTER TABLE utenti ADD COLUMN telefono TEXT")
        print("✅ Colonna 'telefono' aggiunta alla tabella utenti")
    except sqlite3.OperationalError:
        # La colonna esiste già
        pass
    
    try:
        c.execute("ALTER TABLE cambi ADD COLUMN data_ore_singole DATE")
        c.execute("ALTER TABLE cambi ADD COLUMN ora_inizio TEXT")
        c.execute("ALTER TABLE cambi ADD COLUMN ora_fine TEXT")
        print("✅ Colonne ore singole aggiunte alla tabella cambi")
    except sqlite3.OperationalError:
        # Le colonne esistono già
        pass

    conn.commit()
    conn.close()
    
    # Genera il calendario automatico
    genera_calendario_automatico()

init_db()

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
    c.execute('''SELECT user_id, username, nome, cognome, data_richiesta 
                 FROM utenti WHERE ruolo = 'in_attesa' ORDER BY data_richiesta''')
    result = c.fetchall()
    conn.close()
    return result

def get_utenti_approvati():
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    c.execute('''SELECT user_id, username, nome, cognome, ruolo, data_approvazione,
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

# === NUOVE FUNZIONI PER SQUADRE ===
def get_componenti_squadra(tipo_squadra, nome_squadra):
    """Restituisce i componenti di una squadra specifica"""
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    
    if tipo_squadra == 'notturna':
        c.execute('''SELECT nome, cognome, qualifica, grado_patente_terrestre, 
                     patente_nautica, saf, tpss, atp
                     FROM utenti WHERE squadra_notte = ? AND ruolo IN ('super_user', 'admin', 'user')
                     ORDER BY cognome, nome''', (nome_squadra,))
    elif tipo_squadra == 'serale':
        c.execute('''SELECT nome, cognome, qualifica, grado_patente_terrestre, 
                     patente_nautica, saf, tpss, atp
                     FROM utenti WHERE squadra_sera = ? AND ruolo IN ('super_user', 'admin', 'user')
                     ORDER BY cognome, nome''', (nome_squadra,))
    elif tipo_squadra == 'festiva':
        c.execute('''SELECT nome, cognome, qualifica, grado_patente_terrestre, 
                     patente_nautica, saf, tpss, atp
                     FROM utenti WHERE squadra_festiva = ? AND ruolo IN ('super_user', 'admin', 'user')
                     ORDER BY cognome, nome''', (nome_squadra,))
    
    componenti = c.fetchall()
    conn.close()
    return componenti

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
    
    # PROSSIMI 2 TURNI FESTIVI (modificato da 1 a 2)
    c.execute('''SELECT * FROM turni 
                 WHERE squadra = ? AND tipo_turno = 'festivo' AND data >= ?
                 ORDER BY data LIMIT 2''', (squadra_festiva, oggi))
    prossimi_festivi = c.fetchall()  # Ora è una lista
    
    # Prossime 2 feste nazionali
    c.execute('''SELECT * FROM feste_nazionali 
                 WHERE data >= ? ORDER BY data LIMIT 2''', (oggi,))
    prossime_feste = c.fetchall()
    
    conn.close()
    
    return {
        'sere': prossime_sere,
        'notti': prossime_notti,
        'festivi': prossimi_festivi,  # Nome cambiato da 'festivo' a 'festivi'
        'feste_nazionali': prossime_feste
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

def get_cambi_utente_completo(user_id):
    """Restituisce tutti i cambi di un utente per l'esportazione"""
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    
    # Cambi come cedente
    c.execute('''SELECT c.id, c.tipo_scambio, c.stato, c.data_creazione,
                 t.data as data_turno, t.tipo_turno, t.squadra,
                 u_a.nome as nome_a, u_a.cognome as cognome_a,
                 c.data_ore_singole, c.ora_inizio, c.ora_fine
                 FROM cambi c
                 JOIN turni t ON c.turno_id = t.id
                 JOIN utenti u_a ON c.user_id_a = u_a.user_id
                 WHERE c.user_id_da = ?
                 ORDER BY t.data''', (user_id,))
    
    cambi_ceduti = c.fetchall()
    
    # Cambi come ricevente
    c.execute('''SELECT c.id, c.tipo_scambio, c.stato, c.data_creazione,
                 t.data as data_turno, t.tipo_turno, t.squadra,
                 u_da.nome as nome_da, u_da.cognome as cognome_da,
                 c.data_ore_singole, c.ora_inizio, c.ora_fine
                 FROM cambi c
                 JOIN turni t ON c.turno_id = t.id
                 JOIN utenti u_da ON c.user_id_da = u_da.user_id
                 WHERE c.user_id_a = ?
                 ORDER BY t.data''', (user_id,))
    
    cambi_ricevuti = c.fetchall()
    
    conn.close()
    
    return cambi_ceduti, cambi_ricevuti

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

# === NUOVE FUNZIONI PER CERCA SOSTITUTO ===
def get_prossime_squadre_per_sostituzione(user_id, tipo_turno):
    """Restituisce le prossime squadre per un tipo di turno, escludendo quelle dell'utente"""
    squadra_notte, squadra_sera, squadra_festiva = get_user_squadre(user_id)
    oggi = datetime.now().date()
    
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    
    # Determina la squadra da escludere in base al tipo di turno
    squadra_escludere = None
    if tipo_turno == 'notte':
        squadra_escludere = squadra_notte
        limit = 30
    elif tipo_turno == 'sera':
        squadra_escludere = squadra_sera
        limit = 30
    elif tipo_turno == 'festivo':
        squadra_escludere = squadra_festiva
        limit = 20
    elif tipo_turno == 'festa_nazionale':
        # Per le feste nazionali, restituiamo tutte quelle dei prossimi 2 anni
        anno_corrente = oggi.year
        c.execute('''SELECT * FROM feste_nazionali 
                     WHERE data >= ? AND data <= ?
                     ORDER BY data''', 
                     (oggi.isoformat(), f"{anno_corrente + 2}-12-31"))
        feste = c.fetchall()
        conn.close()
        return feste
    
    if tipo_turno == 'festa_nazionale':
        conn.close()
        return []
    
    # Per turni normali, escludi sabato per i turni serali e notturni
    if tipo_turno in ['sera', 'notte']:
        c.execute(f'''SELECT DISTINCT squadra, COUNT(*) as conteggio 
                     FROM turni 
                     WHERE tipo_turno = ? AND data >= ? AND squadra != ?
                     AND strftime('%w', data) != '6'  -- Escludi sabato (6 = sabato in SQLite)
                     GROUP BY squadra 
                     ORDER BY conteggio DESC, squadra
                     LIMIT ?''', 
                     (tipo_turno, oggi.isoformat(), squadra_escludere, limit))
    else:
        c.execute(f'''SELECT DISTINCT squadra, COUNT(*) as conteggio 
                     FROM turni 
                     WHERE tipo_turno = ? AND data >= ? AND squadra != ?
                     GROUP BY squadra 
                     ORDER BY conteggio DESC, squadra
                     LIMIT ?''', 
                     (tipo_turno, oggi.isoformat(), squadra_escludere, limit))
    
    squadre = c.fetchall()
    conn.close()
    return squadre

def get_dettagli_squadra_per_sostituzione(squadra, tipo_turno):
    """Restituisce i dettagli dei prossimi turni per una squadra specifica"""
    oggi = datetime.now().date()
    
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    
    if tipo_turno == 'festa_nazionale':
        c.execute('''SELECT * FROM feste_nazionali 
                     WHERE squadra = ? AND data >= ?
                     ORDER BY data LIMIT 5''', (squadra, oggi.isoformat()))
    else:
        c.execute('''SELECT * FROM turni 
                     WHERE squadra = ? AND tipo_turno = ? AND data >= ?
                     ORDER BY data LIMIT 5''', (squadra, tipo_turno, oggi.isoformat()))
    
    turni = c.fetchall()
    conn.close()
    return turni

# === GESTIONE CAMBI ===
def crea_cambio(user_id_da, user_id_a, turno_id, tipo_scambio, data_ore_singole=None, ora_inizio=None, ora_fine=None):
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    
    if tipo_scambio == 'ore_singole':
        c.execute('''INSERT INTO cambi (user_id_da, user_id_a, turno_id, tipo_scambio, data_ore_singole, ora_inizio, ora_fine)
                     VALUES (?, ?, ?, ?, ?, ?, ?)''', 
                     (user_id_da, user_id_a, turno_id, tipo_scambio, data_ore_singole, ora_inizio, ora_fine))
    else:
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
        [KeyboardButton("👥 Squadre"), KeyboardButton("📤 Estrazione")],
        [KeyboardButton("/start 🔄"), KeyboardButton("🆘 Help")]
    ]

    if is_admin(user_id):
        tastiera.append([KeyboardButton("👮 Gestisci richieste"), KeyboardButton("✏️ Modifica cambio")])

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
    
    # PROSSIMI 2 TURNI FESTIVI (modificato)
    prossimi_festivi = get_prossimi_turni_utente(user_id)['festivi']
    if prossimi_festivi:
        messaggio += "🎉 **PROSSIMI 2 FESTIVI:**\n"
        for turno in prossimi_festivi[:2]:  # Prendi solo i primi 2
            data_festivo = formatta_data_per_visualizzazione(turno[1])
            sabato = datetime.strptime(turno[1], '%Y-%m-%d')
            domenica = sabato + timedelta(days=1)
            messaggio += f"• {sabato.strftime('%d/%m')}-{domenica.strftime('%d/%m')}: {turno[3]}\n"
        messaggio += "\n"
    
    # Prossime 2 festività nazionali
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    c.execute('''SELECT * FROM feste_nazionali 
                 WHERE data >= ? ORDER BY data LIMIT 2''', (oggi,))
    prossime_feste = c.fetchall()
    conn.close()
    
    if prossime_feste:
        messaggio += "🎊 **PROSSIME FESTIVITÀ NAZIONALI:**\n"
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
    
    if prossimi_festivi and any(t[3] == squadra_festiva for t in prossimi_festivi[:1]):
        coinvolto = True
        messaggio += "\n🚒 **SEI DI TURNO** nel prossimo weekend!\n"
    
    # Controlla cambi/sostituzioni
    cambi_da_cedere, cambi_da_ricevere = get_cambi_pendenti_utente(user_id)
    if cambi_da_cedere or cambi_da_ricevere:
        messaggio += "\n🔄 **HAI CAMBI IN SOSPESO** - controlla in 'Prossimi turni'\n"
    
    # Aggiungi tastiera inline per opzioni aggiuntive
    keyboard = [
        [
            InlineKeyboardButton("📅 Turni settimana corrente", callback_data="turni_settimana"),
            InlineKeyboardButton("📆 Turni prossimi 7 giorni", callback_data="turni_7giorni")
        ],
        [
            InlineKeyboardButton("🔍 Cerca sostituto", callback_data="cerca_sostituto")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(messaggio, reply_markup=reply_markup)

# === SQUADRE ===
async def squadre(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
        messaggio += f"🌃 **Squadra notturna:** {squadra_notte or 'Non impostata'}\n"
        messaggio += f"🌙 **Squadra serale:** {squadra_sera or 'Non impostata'}\n"
        messaggio += f"🎉 **Squadra festiva:** {squadra_festiva or 'Non impostata'}\n"
    else:
        messaggio += "❌ Non hai ancora impostato le tue squadre.\n"
    
    messaggio += "\nSeleziona un'opzione:"
    
    await update.message.reply_text(messaggio, reply_markup=reply_markup)

async def squadre_visualizza(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    keyboard = [
        [
            InlineKeyboardButton("🌃 Notturne", callback_data="visualizza_notturne"),
            InlineKeyboardButton("🌙 Serali", callback_data="visualizza_serali")
        ],
        [
            InlineKeyboardButton("🎉 Festive", callback_data="visualizza_festive")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        "👥 **VISUALIZZA SQUADRE**\n\n"
        "Seleziona il tipo di squadra da visualizzare:",
        reply_markup=reply_markup
    )

async def visualizza_squadre_tipo(update: Update, context: ContextTypes.DEFAULT_TYPE, tipo_squadra: str):
    query = update.callback_query
    await query.answer()
    
    # Mappa i tipi di squadra
    tipo_mappa = {
        'visualizza_notturne': ('notturna', '🌃 NOTTURNE', SQUADRE_NOTTURNE),
        'visualizza_serali': ('serale', '🌙 SERALI', SQUADRE_SERALI),
        'visualizza_festive': ('festiva', '🎉 FESTIVE', SQUADRE_FESTIVE)
    }
    
    tipo_db, tipo_nome, squadre_lista = tipo_mappa[tipo_squadra]
    
    keyboard = []
    for squadra in squadre_lista:
        keyboard.append([InlineKeyboardButton(squadra, callback_data=f"componenti_{tipo_db}_{squadra}")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        f"👥 **SQUADRE {tipo_nome}**\n\n"
        f"Seleziona una squadra per vedere i componenti:",
        reply_markup=reply_markup
    )

async def visualizza_componenti_squadra(update: Update, context: ContextTypes.DEFAULT_TYPE, tipo_squadra: str, nome_squadra: str):
    query = update.callback_query
    await query.answer()
    
    # Mappa i tipi di squadra per i nomi
    tipo_nome_mappa = {
        'notturna': '🌃 NOTTURNA',
        'serale': '🌙 SERALE', 
        'festiva': '🎉 FESTIVA'
    }
    
    tipo_nome = tipo_nome_mappa.get(tipo_squadra, tipo_squadra.upper())
    
    # Ottieni i componenti della squadra
    componenti = get_componenti_squadra(tipo_squadra, nome_squadra)
    
    if not componenti:
        messaggio = f"👥 **SQUADRA {tipo_nome} {nome_squadra}**\n\n"
        messaggio += "❌ Nessun componente trovato per questa squadra.\n\n"
        messaggio += "I vigili devono impostare le loro squadre nel profilo."
    else:
        messaggio = f"👥 **SQUADRA {tipo_nome} {nome_squadra}**\n\n"
        messaggio += f"**Componenti ({len(componenti)}):**\n\n"
        
        for i, (nome, cognome, qualifica, grado_patente, patente_nautica, saf, tpss, atp) in enumerate(componenti, 1):
            messaggio += f"**{i}. {nome} {cognome}**\n"
            messaggio += f"   • Qualifica: {qualifica}\n"
            messaggio += f"   • Grado patente: {grado_patente}\n"
            
            # Aggiungi specializzazioni
            specializzazioni = []
            if patente_nautica:
                specializzazioni.append("⛵ Nautica")
            if saf:
                specializzazioni.append("🚒 SAF")
            if tpss:
                specializzazioni.append("🛡️ TPSS")
            if atp:
                specializzazioni.append("🚁 ATP")
            
            if specializzazioni:
                messaggio += f"   • Specializzazioni: {', '.join(specializzazioni)}\n"
            
            messaggio += "\n"
    
    await query.edit_message_text(messaggio)

# === CERCA SOSTITUTO ===
async def cerca_sostituto(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    keyboard = [
        [
            InlineKeyboardButton("🌃 Notturno", callback_data="sostituto_notte"),
            InlineKeyboardButton("🌙 Serale", callback_data="sostituto_sera")
        ],
        [
            InlineKeyboardButton("🎉 Festivo", callback_data="sostituto_festivo"),
            InlineKeyboardButton("🎊 Festa Nazionale", callback_data="sostituto_festa")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        "🔍 **CERCA SOSTITUTO**\n\n"
        "Seleziona la categoria per cui cerchi un sostituto:",
        reply_markup=reply_markup
    )

async def gestisci_cerca_sostituto(update: Update, context: ContextTypes.DEFAULT_TYPE, tipo_turno: str):
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    user_squadre = get_user_squadre(user_id)
    
    # Mappa i tipi di turno
    tipo_mappa = {
        'sostituto_notte': ('notte', '🌃 NOTTURNO'),
        'sostituto_sera': ('sera', '🌙 SERALE'),
        'sostituto_festivo': ('festivo', '🎉 FESTIVO'),
        'sostituto_festa': ('festa_nazionale', '🎊 FESTA NAZIONALE')
    }
    
    tipo_db, tipo_nome = tipo_mappa[tipo_turno]
    
    # Ottieni le squadre candidate per la sostituzione
    squadre_candidate = get_prossime_squadre_per_sostituzione(user_id, tipo_db)
    
    if not squadre_candidate:
        await query.edit_message_text(
            f"❌ **NESSUN SOSTITUTO TROVATO**\n\n"
            f"Per {tipo_nome.lower()} non sono state trovate squadre disponibili per la sostituzione."
        )
        return
    
    messaggio = f"🔍 **SOSTITUTI PER {tipo_nome}**\n\n"
    
    if tipo_db == 'festa_nazionale':
        # Per feste nazionali
        messaggio += f"🎊 **Feste nazionali nei prossimi 2 anni:**\n\n"
        for festa in squadre_candidate:
            data_festa = formatta_data_per_visualizzazione(festa[1])
            messaggio += f"• **{data_festa}**: {festa[2]} - Squadra: {festa[3]}\n"
        
        messaggio += f"\n📊 **Squadre con più feste:**\n"
        # Conta feste per squadra
        conteggio_squadre = {}
        for festa in squadre_candidate:
            squadra = festa[3]
            conteggio_squadre[squadra] = conteggio_squadre.get(squadra, 0) + 1
        
        for squadra, conteggio in sorted(conteggio_squadre.items(), key=lambda x: x[1], reverse=True):
            messaggio += f"• **{squadra}**: {conteggio} feste\n"
            
    else:
        # Per turni normali
        messaggio += f"📊 **Squadre candidate (esclusa la tua):**\n\n"
        
        for squadra, conteggio in squadre_candidate:
            # Ottieni i prossimi turni per questa squadra
            turni_squadra = get_dettagli_squadra_per_sostituzione(squadra, tipo_db)
            
            messaggio += f"**{squadra}** - {conteggio} turni futuri\n"
            
            # Mostra i prossimi 3 turni
            for i, turno in enumerate(turni_squadra[:3]):
                if tipo_db == 'notte':
                    descrizione = formatta_turno_notte_per_visualizzazione(turno[1], turno[3])
                    messaggio += f"  {i+1}. {descrizione}\n"
                else:
                    data_turno = formatta_data_per_visualizzazione(turno[1])
                    messaggio += f"  {i+1}. {data_turno}\n"
            
            messaggio += "\n"
    
    # Aggiungi informazioni utili
    squadra_escludere = ""
    if tipo_db == 'notte':
        squadra_escludere = user_squadre[0]
    elif tipo_db == 'sera':
        squadra_escludere = user_squadre[1]
    elif tipo_db == 'festivo':
        squadra_escludere = user_squadre[2]
    
    if squadra_escludere:
        messaggio += f"\nℹ️ *La tua squadra ({squadra_escludere}) è stata esclusa dalla ricerca*"
    
    await query.edit_message_text(messaggio)

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
    
    # PROSSIMI 2 TURNI FESTIVI (modificato)
    if prossimi['festivi']:
        messaggio += "🎉 **PROSSIMI 2 FESTIVI:**\n"
        for turno in prossimi['festivi']:
            data_festivo = formatta_data_per_visualizzazione(turno[1])
            sabato = datetime.strptime(turno[1], '%Y-%m-%d')
            domenica = sabato + timedelta(days=1)
            messaggio += f"• {sabato.strftime('%d/%m')}-{domenica.strftime('%d/%m')}: {turno[3]}\n"
        messaggio += "\n"
    
    # Prossime 2 feste nazionali
    if prossimi['feste_nazionali']:
        messaggio += "🎊 **PROSSIME 2 FESTE NAZIONALI:**\n"
        for festa in prossimi['feste_nazionali']:
            data_festa = formatta_data_per_visualizzazione(festa[1])
            messaggio += f"• {data_festa}: {festa[2]} - {festa[3]}\n"
        messaggio += "\n"
    
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
        user_id_u, username, nome, cognome, ruolo, data_approvazione, sq_notte, sq_sera, sq_festiva = utente
        display_name = f"{nome} {cognome} ({sq_notte} {sq_sera} {sq_festiva})"
        keyboard.append([InlineKeyboardButton(display_name, callback_data=f"cambio_sel_{user_id_u}")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    context.user_data['cambio'] = {'fase': 'selezione_utente'}
    
    await update.message.reply_text(
        "🔄 **AGGIUNGI CAMBIO**\n\n"
        "Seleziona la persona con cui hai concordato il cambio:",
        reply_markup=reply_markup
    )

# === NUOVO FLUSSO ORE SINGOLE ===
async def gestisci_ore_singole(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    context.user_data['cambio']['tipo_scambio'] = 'ore_singole'
    context.user_data['cambio']['fase'] = 'data_ore_singole'
    
    await query.edit_message_text(
        "🕐 **ORE SINGOLE**\n\n"
        "Inserisci la data in formato GGMMAA (es: 151125 per 15/11/2025):"
    )

async def gestisci_data_ore_singole(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    data_testo = update.message.text.strip()
    
    # Verifica formato data
    if len(data_testo) != 6 or not data_testo.isdigit():
        await update.message.reply_text(
            "❌ Formato data non valido. Usa GGMMAA (es: 151125 per 15/11/2025):"
        )
        return
    
    try:
        giorno = int(data_testo[0:2])
        mese = int(data_testo[2:4])
        anno = int("20" + data_testo[4:6])  # Assume anni 2000+
        
        data_ore_singole = datetime(anno, mese, giorno).date()
        
        # Verifica che la data non sia nel passato
        oggi = datetime.now().date()
        if data_ore_singole < oggi:
            await update.message.reply_text(
                "❌ La data non può essere nel passato. Inserisci una data futura:"
            )
            return
            
        context.user_data['cambio']['data_ore_singole'] = data_ore_singole.isoformat()
        context.user_data['cambio']['fase'] = 'ora_inizio_ore_singole'
        
        await update.message.reply_text(
            "✅ Data impostata correttamente.\n\n"
            "Ora inserisci l'ora di inizio in formato HHMM (es: 0830 per 8:30):"
        )
        
    except ValueError as e:
        await update.message.reply_text(
            "❌ Data non valida. Controlla il formato e riprova (GGMMAA):"
        )

async def gestisci_ora_inizio_ore_singole(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    ora_testo = update.message.text.strip()
    
    # Verifica formato ora
    if len(ora_testo) != 4 or not ora_testo.isdigit():
        await update.message.reply_text(
            "❌ Formato ora non valido. Usa HHMM (es: 0830 per 8:30):"
        )
        return
    
    try:
        ore = int(ora_testo[0:2])
        minuti = int(ora_testo[2:4])
        
        if ore < 0 or ore > 23 or minuti < 0 or minuti > 59:
            raise ValueError("Ora non valida")
            
        context.user_data['cambio']['ora_inizio'] = f"{ore:02d}:{minuti:02d}"
        context.user_data['cambio']['fase'] = 'ora_fine_ore_singole'
        
        await update.message.reply_text(
            "✅ Ora di inizio impostata correttamente.\n\n"
            "Ora inserisci l'ora di fine in formato HHMM (es: 1730 per 17:30):"
        )
        
    except ValueError:
        await update.message.reply_text(
            "❌ Ora non valida. Usa HHMM con ore 00-23 e minuti 00-59:"
        )

async def gestisci_ora_fine_ore_singole(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    ora_testo = update.message.text.strip()
    
    # Verifica formato ora
    if len(ora_testo) != 4 or not ora_testo.isdigit():
        await update.message.reply_text(
            "❌ Formato ora non valido. Usa HHMM (es: 1730 per 17:30):"
        )
        return
    
    try:
        ore = int(ora_testo[0:2])
        minuti = int(ora_testo[2:4])
        
        if ore < 0 or ore > 23 or minuti < 0 or minuti > 59:
            raise ValueError("Ora non valida")
            
        ora_fine = f"{ore:02d}:{minuti:02d}"
        
        # Verifica che l'ora di fine sia dopo l'ora di inizio
        ora_inizio = context.user_data['cambio']['ora_inizio']
        
        # Converti in datetime per confronto
        data_ore_singole = context.user_data['cambio']['data_ore_singole']
        datetime_inizio = datetime.strptime(f"{data_ore_singole} {ora_inizio}", "%Y-%m-%d %H:%M")
        datetime_fine = datetime.strptime(f"{data_ore_singole} {ora_fine}", "%Y-%m-%d %H:%M")
        
        if datetime_fine <= datetime_inizio:
            await update.message.reply_text(
                "❌ L'ora di fine deve essere dopo l'ora di inizio. Riprova:"
            )
            return
            
        context.user_data['cambio']['ora_fine'] = ora_fine
        
        # Crea il cambio
        user_id_da = user_id
        user_id_a = context.user_data['cambio']['user_id_a']
        data_ore_singole = context.user_data['cambio']['data_ore_singole']
        ora_inizio = context.user_data['cambio']['ora_inizio']
        ora_fine = context.user_data['cambio']['ora_fine']
        
        # Per le ore singole, non abbiamo un turno_id specifico, usiamo un valore fittizio
        cambio_id = crea_cambio(user_id_da, user_id_a, None, 'ore_singole', data_ore_singole, ora_inizio, ora_fine)
        
        # Notifica l'altro utente
        nome_utente = get_user_nome(user_id)
        try:
            await context.bot.send_message(
                user_id_a,
                f"🔄 **NUOVA RICHIESTA ORE SINGOLE**\n\n"
                f"Da: {nome_utente}\n"
                f"Data: {formatta_data_per_visualizzazione(data_ore_singole)}\n"
                f"Ore: {ora_inizio} - {ora_fine}\n\n"
                f"Contatta {nome_utente} per confermare il cambio."
            )
        except Exception as e:
            print(f"Errore notifica ore singole: {e}")
        
        # Conferma all'utente
        await update.message.reply_text(
            f"✅ **RICHIESTA ORE SINGOLE INVIATA**\n\n"
            f"A: {get_user_nome(user_id_a)}\n"
            f"Data: {formatta_data_per_visualizzazione(data_ore_singole)}\n"
            f"Ore: {ora_inizio} - {ora_fine}\n\n"
            f"Attendi la conferma dell'altro vigile."
        )
        
        # Pulisci il context
        del context.user_data['cambio']
        
    except ValueError:
        await update.message.reply_text(
            "❌ Ora non valida. Usa HHMM con ore 00-23 e minuti 00-59:"
        )

# === STATISTICHE ===
async def statistiche(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_user_approved(user_id):
        return
    
    # Calcola statistiche reali
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    
    # Conta cambi per tipo
    c.execute('''SELECT tipo_scambio, COUNT(*) FROM cambi 
                 WHERE stato = 'completato' GROUP BY tipo_scambio''')
    cambi_stats = dict(c.fetchall())
    
    # Conta cambi per utente
    c.execute('''SELECT u.nome, u.cognome, COUNT(*) 
                 FROM cambi c 
                 JOIN utenti u ON c.user_id_da = u.user_id 
                 WHERE c.stato = 'completato' 
                 GROUP BY u.user_id 
                 ORDER BY COUNT(*) DESC LIMIT 10''')
    top_cedenti = c.fetchall()
    
    c.execute('''SELECT u.nome, u.cognome, COUNT(*) 
                 FROM cambi c 
                 JOIN utenti u ON c.user_id_a = u.user_id 
                 WHERE c.stato = 'completato' 
                 GROUP BY u.user_id 
                 ORDER BY COUNT(*) DESC LIMIT 10''')
    top_riceventi = c.fetchall()
    
    conn.close()
    
    messaggio = "📊 **STATISTICHE SOSTITUZIONI**\n\n"
    
    messaggio += "📈 **TIPI DI CAMBIO COMPLETATI:**\n"
    messaggio += f"• 📤 Dare turno: {cambi_stats.get('dare', 0)}\n"
    messaggio += f"• 📥 Ricevere turno: {cambi_stats.get('ricevere', 0)}\n"
    messaggio += f"• 🔄 Scambiare turno: {cambi_stats.get('scambiare', 0)}\n"
    messaggio += f"• 🕐 Ore singole: {cambi_stats.get('ore_singole', 0)}\n"
    messaggio += f"• 📊 Totale: {sum(cambi_stats.values())}\n\n"
    
    if top_cedenti:
        messaggio += "🏆 **TOP 10 CEDENTI:**\n"
        for i, (nome, cognome, count) in enumerate(top_cedenti, 1):
            messaggio += f"{i}. {nome} {cognome}: {count} turni ceduti\n"
        messaggio += "\n"
    
    if top_riceventi:
        messaggio += "🎯 **TOP 10 RICEVENTI:**\n"
        for i, (nome, cognome, count) in enumerate(top_riceventi, 1):
            messaggio += f"{i}. {nome} {cognome}: {count} turni ricevuti\n"
    
    # Aggiungi bottone per esportare i cambi
    keyboard = [
        [InlineKeyboardButton("📤 Esporta i miei cambi", callback_data="export_miei_cambi")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(messaggio, reply_markup=reply_markup)

async def export_miei_cambi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    
    try:
        # Ottieni tutti i cambi dell'utente
        cambi_ceduti, cambi_ricevuti = get_cambi_utente_completo(user_id)
        
        output = StringIO()
        writer = csv.writer(output)
        
        # Header
        writer.writerow(['tipo', 'id_cambio', 'tipo_scambio', 'stato', 'data_creazione', 
                        'data_turno', 'tipo_turno', 'squadra', 'altro_utente', 'data_ore_singole', 'ora_inizio', 'ora_fine'])
        
        # Cambi ceduti
        for cambio in cambi_ceduti:
            id_cambio, tipo_scambio, stato, data_creazione, data_turno, tipo_turno, squadra, nome_a, cognome_a, data_ore_singole, ora_inizio, ora_fine = cambio
            writer.writerow([
                'CEDUTO',
                id_cambio,
                tipo_scambio,
                stato,
                data_creazione,
                data_turno,
                tipo_turno,
                squadra,
                f"{nome_a} {cognome_a}",
                data_ore_singole or '',
                ora_inizio or '',
                ora_fine or ''
            ])
        
        # Cambi ricevuti
        for cambio in cambi_ricevuti:
            id_cambio, tipo_scambio, stato, data_creazione, data_turno, tipo_turno, squadra, nome_da, cognome_da, data_ore_singole, ora_inizio, ora_fine = cambio
            writer.writerow([
                'RICEVUTO',
                id_cambio,
                tipo_scambio,
                stato,
                data_creazione,
                data_turno,
                tipo_turno,
                squadra,
                f"{nome_da} {cognome_da}",
                data_ore_singole or '',
                ora_inizio or '',
                ora_fine or ''
            ])
        
        csv_data = output.getvalue()
        output.close()
        
        csv_bytes = csv_data.encode('utf-8')
        csv_file = BytesIO(csv_bytes)
        csv_file.name = f"cambi_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        
        await query.edit_message_text("📤 Generazione file Cambi in corso...")
        await context.bot.send_document(
            chat_id=query.message.chat_id,
            document=csv_file,
            filename=csv_file.name,
            caption="🔄 **I TUOI CAMBI**\n\nFile CSV contenente tutti i tuoi cambi (ceduti e ricevuti)."
        )
        
    except Exception as e:
        await query.edit_message_text(f"❌ Errore durante l'esportazione: {str(e)}")

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
        await update.message.reply_text("❌ Solo gli amministratori possono gestire le richieste.")
        return
    
    richieste = get_richieste_in_attesa()
    
    if not richieste:
        await update.message.reply_text("✅ Nessuna richiesta di accesso in sospeso.")
        return
    
    # Mostra la prima richiesta in attesa
    prima_richiesta = richieste[0]
    user_id_rich, username, nome, cognome, data_richiesta = prima_richiesta
    
    keyboard = [
        [
            InlineKeyboardButton("✅ Approva", callback_data=f"approva_{user_id_rich}"),
            InlineKeyboardButton("❌ Rifiuta", callback_data=f"rifiuta_{user_id_rich}")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    messaggio = f"👤 **RICHIESTA ACCESSO**\n\n"
    messaggio += f"🆔 ID: {user_id_rich}\n"
    messaggio += f"👤 Nome: {nome} {cognome}\n"
    messaggio += f"📱 Username: @{username}\n"
    messaggio += f"📅 Data: {data_richiesta}\n\n"
    messaggio += f"📋 Richieste rimanenti: {len(richieste) - 1}"
    
    await update.message.reply_text(messaggio, reply_markup=reply_markup)

# === MODIFICA CAMBIO ===
async def modifica_cambio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("❌ Solo gli amministratori possono modificare i cambi.")
        return
    
    # Ottieni tutti i cambi pendenti
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    c.execute('''SELECT c.id, u1.nome, u1.cognome, u2.nome, u2.cognome, t.data, t.tipo_turno, c.tipo_scambio
                 FROM cambi c
                 JOIN utenti u1 ON c.user_id_da = u1.user_id
                 JOIN utenti u2 ON c.user_id_a = u2.user_id
                 JOIN turni t ON c.turno_id = t.id
                 WHERE c.stato = 'pending'
                 ORDER BY t.data''')
    cambi_pendenti = c.fetchall()
    conn.close()
    
    if not cambi_pendenti:
        await update.message.reply_text("✅ Nessun cambio in sospeso da modificare.")
        return
    
    keyboard = []
    for cambio in cambi_pendenti[:20]:  # Limite di 20 cambi
        id_cambio, nome_da, cognome_da, nome_a, cognome_a, data, tipo_turno, tipo_scambio = cambio
        data_formattata = formatta_data_per_visualizzazione(data)
        testo = f"{data_formattata} - {nome_da} → {nome_a} ({tipo_turno})"
        keyboard.append([InlineKeyboardButton(testo, callback_data=f"modifica_cambio_{id_cambio}")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        "✏️ **MODIFICA CAMBIO**\n\n"
        "Seleziona il cambio da modificare:",
        reply_markup=reply_markup
    )

# === HELP ===
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    messaggio = """🆘 **HELP - GUIDA ALL'USO**

**FUNZIONALITÀ PRINCIPALI:**

👥 **CHI TOCCA** - Mostra i turni di oggi, stanotte, il weekend e le prossime festività

📅 **PROSSIMI TURNI** - I tuoi prossimi turni e cambi in sospeso

🔄 **AGGIUNGI CAMBIO** - Organizza cambi con altri vigili

📊 **STATISTICHE** - Bilancio ore con gli altri vigili

👥 **SQUADRE** - Visualizza componenti squadre o modifica le tue

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
    
    # Gestione tipo scambio
    elif callback_data.startswith("scambio_"):
        tipo_scambio = callback_data.replace("scambio_", "")
        await gestisci_tipo_scambio(update, context, tipo_scambio)
    
    # Gestione tipologia turno per cambio
    elif callback_data.startswith("tipo_"):
        await gestisci_tipologia_turno_cambio(update, context, callback_data)
    
    # Gestione esportazione dati
    elif callback_data == "export_calendario":
        await esporta_calendario(update, context)
    elif callback_data == "export_vigili":
        await esporta_vigili(update, context)
    elif callback_data == "export_utenti":
        await esporta_utenti(update, context)
    elif callback_data == "export_miei_cambi":
        await export_miei_cambi(update, context)
    
    # Gestione richieste admin
    elif callback_data == "richieste_attesa":
        await mostra_richieste_attesa(update, context)
    elif callback_data == "utenti_approvati":
        await mostra_utenti_approvati(update, context)
    elif callback_data.startswith("approva_"):
        user_id_approvare = int(callback_data.replace("approva_", ""))
        await approva_utente_handler(update, context, user_id_approvare)
    elif callback_data.startswith("rifiuta_"):
        user_id_rifiutare = int(callback_data.replace("rifiuta_", ""))
        await rifiuta_utente_handler(update, context, user_id_rifiutare)
    
    # Gestione squadre
    elif callback_data == "squadre_visualizza":
        await squadre_visualizza(update, context)
    elif callback_data.startswith("visualizza_"):
        await visualizza_squadre_tipo(update, context, callback_data)
    elif callback_data.startswith("componenti_"):
        # Estrai tipo_squadra e nome_squadra dal callback_data
        parts = callback_data.split('_')
        if len(parts) >= 3:
            tipo_squadra = parts[1]
            nome_squadra = '_'.join(parts[2:])  # Gestisce nomi squadra con underscore
            await visualizza_componenti_squadra(update, context, tipo_squadra, nome_squadra)
    elif callback_data == "squadre_cambia":
        await cambia_squadra(update, context)
    
    # Gestione modifica cambio
    elif callback_data.startswith("modifica_cambio_"):
        cambio_id = int(callback_data.replace("modifica_cambio_", ""))
        await gestisci_modifica_cambio(update, context, cambio_id)
    
    # Nuove gestioni per Chi Tocca
    elif callback_data == "turni_settimana":
        await mostra_turni_settimana(update, context)
    elif callback_data == "turni_7giorni":
        await mostra_turni_7giorni(update, context)
    
    # Gestione cerca sostituto
    elif callback_data == "cerca_sostituto":
        await cerca_sostituto(update, context)
    elif callback_data.startswith("sostituto_"):
        await gestisci_cerca_sostituto(update, context, callback_data)

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
        [
            InlineKeyboardButton("🔄 Scambiare turno", callback_data="scambio_scambiare"),
            InlineKeyboardButton("🕐 Ore singole", callback_data="scambio_ore_singole")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        f"🔄 **TIPO DI SCAMBIO**\n\n"
        f"Con: {nome_utente}\n\n"
        f"Seleziona il tipo di scambio:",
        reply_markup=reply_markup
    )

async def gestisci_tipo_scambio(update: Update, context: ContextTypes.DEFAULT_TYPE, tipo_scambio: str):
    query = update.callback_query
    try:
        await query.answer()
    except BadRequest:
        return
    
    context.user_data['cambio']['tipo_scambio'] = tipo_scambio
    
    nome_utente = get_user_nome(context.user_data['cambio']['user_id_a'])
    
    if tipo_scambio == 'scambio_ore_singole':
        await gestisci_ore_singole(update, context)
        return
    
    keyboard = [
        [
            InlineKeyboardButton("🌃 Notte", callback_data="tipo_notte"),
            InlineKeyboardButton("🌙 Sera", callback_data="tipo_sera"),
            InlineKeyboardButton("🎉 Festivo", callback_data="tipo_festivo")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    tipo_testo = {
        'scambio_dare': "📤 DARE un turno",
        'scambio_ricevere': "📥 RICEVERE un turno", 
        'scambio_scambiare': "🔄 SCAMBIARE turni"
    }.get(tipo_scambio, tipo_scambio)
    
    await query.edit_message_text(
        f"🔄 **SELEZIONA TIPOLOGIA TURNO**\n\n"
        f"{tipo_testo} con: {nome_utente}\n\n"
        f"Seleziona la tipologia di turno:",
        reply_markup=reply_markup
    )

async def gestisci_tipologia_turno_cambio(update: Update, context: ContextTypes.DEFAULT_TYPE, tipologia_turno: str):
    query = update.callback_query
    try:
        await query.answer()
    except BadRequest:
        return
    
    # Mappa le tipologie
    tipo_mappa = {
        'tipo_notte': 'notte',
        'tipo_sera': 'sera',
        'tipo_festivo': 'festivo'
    }
    
    tipo_turno = tipo_mappa.get(tipologia_turno)
    
    if not tipo_turno:
        await query.edit_message_text("❌ Errore: tipologia turno non riconosciuta.")
        return
    
    user_id = query.from_user.id
    user_id_a = context.user_data['cambio']['user_id_a']
    tipo_scambio = context.user_data['cambio']['tipo_scambio'].replace('scambio_', '')
    
    # Ottieni i turni disponibili per l'utente
    turni_disponibili = get_turni_utente_per_tipo(user_id, tipo_turno)
    
    if not turni_disponibili:
        await query.edit_message_text(
            f"❌ Non hai turni {tipo_turno} disponibili per il cambio."
        )
        return
    
    # Mostra i turni disponibili
    keyboard = []
    for turno in turni_disponibili[:15]:  # Limite di 15 turni
        data_formattata = formatta_data_per_visualizzazione(turno[1])
        if tipo_turno == 'notte':
            descrizione = formatta_turno_notte_per_visualizzazione(turno[1], turno[3])
        else:
            descrizione = f"{data_formattata}: {turno[3]}"
        
        keyboard.append([InlineKeyboardButton(descrizione, callback_data=f"turno_sel_{turno[0]}")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    tipo_testo = {
        'notte': '🌃 NOTTURNO',
        'sera': '🌙 SERALE',
        'festivo': '🎉 FESTIVO'
    }.get(tipo_turno, tipo_turno.upper())
    
    await query.edit_message_text(
        f"🔄 **SELEZIONA TURNO {tipo_testo}**\n\n"
        f"Con: {get_user_nome(user_id_a)}\n"
        f"Tipo: {tipo_scambio.upper()}\n\n"
        f"Seleziona il turno:",
        reply_markup=reply_markup
    )

# ... [le restanti funzioni rimangono simili, aggiungendo solo la gestione dei messaggi per le ore singole] ...

# === GESTIONE MESSAGGI DI TESTO ===
async def gestisci_messaggio_testo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    testo = update.message.text
    
    if not is_user_approved(user_id):
        if testo == "🚀 Richiedi Accesso":
            await start(update, context)
        return
    
    # Gestione flusso ore singole
    if 'cambio' in context.user_data:
        fase = context.user_data['cambio'].get('fase')
        
        if fase == 'data_ore_singole':
            await gestisci_data_ore_singole(update, context)
            return
        elif fase == 'ora_inizio_ore_singole':
            await gestisci_ora_inizio_ore_singole(update, context)
            return
        elif fase == 'ora_fine_ore_singole':
            await gestisci_ora_fine_ore_singole(update, context)
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
    elif testo == "👥 Squadre":
        await squadre(update, context)
    elif testo == "📤 Estrazione":
        await estrazione_dati(update, context)
    elif testo == "👮 Gestisci richieste" and is_admin(user_id):
        await gestisci_richieste(update, context)
    elif testo == "✏️ Modifica cambio" and is_admin(user_id):
        await modifica_cambio(update, context)
    elif testo == "/start 🔄":
        await start(update, context)
    elif testo == "🆘 Help":
        await help_command(update, context)

# ... [il resto del codice rimane uguale per esportazione, backup, etc.] ...

# Le funzioni per esportazione, backup, e le altre funzioni rimangono uguali
# ... [inserisci qui tutte le altre funzioni che già esistevano] ...

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
    print("✅ Statistiche funzionanti")
    print("✅ Gestione richieste funzionante")
    print("✅ Funzione CERCA SOSTITUTO attiva")
    print("✅ Funzione SQUADRE migliorata")
    print("✅ Sequenze turni corrette")
    print("✅ Flusso ORE SINGOLE implementato")
    application.run_polling()

if __name__ == '__main__':
    main()
