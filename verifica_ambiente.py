import os

print("🔍 VERIFICA VARIABILI AMBIENTE")
print("BOT_TOKEN:", "✅ PRESENTE" if os.environ.get('BOT_TOKEN') else "❌ MANCANTE")
print("GITHUB_TOKEN:", "✅ PRESENTE" if os.environ.get('GITHUB_TOKEN') else "❌ MANCANTE")
print("GIST_ID:", "✅ PRESENTE" if os.environ.get('GIST_ID') else "⚠️  NON ANCORA CREATO")

# Test database
try:
    import sqlite3
    conn = sqlite3.connect('turni_vvf.db')
    print("✅ Database SQLite funzionante")
    conn.close()
except Exception as e:
    print(f"❌ Errore database: {e}")
