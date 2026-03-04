from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import psycopg2
import psycopg2.extras
import streamlit as st

CONN_STRING = (
    "postgresql://neondb_owner:npg_wc6y8LeWOtkV"
    "@ep-nameless-fire-al8nlq2t-pooler.c-3.eu-central-1.aws.neon.tech"
    "/neondb?sslmode=require&channel_binding=require"
)


def get_conn():
    """Neue Verbindung pro Streamlit-Rerun — vermeidet Timeout-Probleme mit Neon."""
    return psycopg2.connect(CONN_STRING)


def init_db(c) -> None:
    with c.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS companies (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS company_meta (
                company_id INTEGER NOT NULL,
                key TEXT NOT NULL,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (company_id, key),
                FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE
            );
            """
        )
    c.commit()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def add_company(c, name: str) -> int:
    name = (name or "").strip()
    if not name:
        raise ValueError("Unternehmensname ist leer.")
    with c.cursor() as cur:
        cur.execute(
            "INSERT INTO companies (name, created_at) VALUES (%s, %s) RETURNING id;",
            (name, utc_now()),
        )
        new_id = cur.fetchone()[0]
    c.commit()
    return new_id


def delete_company(c, company_id: int) -> None:
    with c.cursor() as cur:
        cur.execute("DELETE FROM companies WHERE id = %s;", (company_id,))
    c.commit()


def list_companies(c) -> pd.DataFrame:
    with c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT id, name, created_at FROM companies ORDER BY id DESC;")
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=["id", "name", "created_at"]) if rows else pd.DataFrame(columns=["id", "name", "created_at"])


def upsert_meta(c, company_id: int, key: str, value: str) -> None:
    key = (key or "").strip()
    value = (value or "").strip()
    if not key:
        raise ValueError("Meta-Key ist leer.")
    with c.cursor() as cur:
        cur.execute(
            """
            INSERT INTO company_meta (company_id, key, value, updated_at)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (company_id, key) DO UPDATE SET
                value = EXCLUDED.value,
                updated_at = EXCLUDED.updated_at;
            """,
            (company_id, key, value, utc_now()),
        )
    c.commit()


def get_meta(c, company_id: int) -> pd.DataFrame:
    with c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT key, value, updated_at FROM company_meta WHERE company_id = %s ORDER BY key;",
            (company_id,),
        )
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=["key", "value", "updated_at"]) if rows else pd.DataFrame(columns=["key", "value", "updated_at"])


# ── UI ──────────────────────────────────────────────────────────────────────

st.set_page_config(page_title="Unternehmen DB", layout="wide")
st.title("Unternehmensdatenbank (Neon PostgreSQL)")

if "page" not in st.session_state:
    st.session_state.page = "Übersicht"

nav1, nav2, nav3 = st.columns(3)
with nav1:
    if st.button("Anlegen", use_container_width=True):
        st.session_state.page = "Anlegen"
with nav2:
    if st.button("Löschen", use_container_width=True):
        st.session_state.page = "Löschen"
with nav3:
    if st.button("Übersicht", use_container_width=True):
        st.session_state.page = "Übersicht"

c = get_conn()
init_db(c)

page = st.session_state.page

if page == "Anlegen":
    st.subheader("Unternehmen anlegen")
    name = st.text_input("Unternehmensname", placeholder="z.B. ACME GmbH")
    if st.button("Eintrag speichern", use_container_width=True):
        try:
            new_id = add_company(c, name)
            st.success(f"Angelegt mit ID: {new_id}")
        except Exception as e:
            st.error(str(e))

    st.divider()
    st.subheader("Meta-Daten hinzufügen")

    df = list_companies(c)
    if df.empty:
        st.info("Erst ein Unternehmen anlegen.")
    else:
        options = {f"{row.id} — {row.name}": int(row.id) for row in df.itertuples(index=False)}
        sel = st.selectbox("Unternehmen", list(options.keys()))
        company_id = options[sel]

        mk = st.text_input("Feld (Key)", placeholder="z.B. website / branche / standort")
        mv = st.text_input("Wert", placeholder="z.B. https://..., Maschinenbau, Berlin")

        if st.button("Meta speichern", use_container_width=True):
            try:
                upsert_meta(c, company_id, mk, mv)
                st.success("Meta gespeichert.")
            except Exception as e:
                st.error(str(e))

        st.write("Aktuelle Meta-Daten:")
        st.dataframe(get_meta(c, company_id), use_container_width=True, hide_index=True)

elif page == "Löschen":
    st.subheader("Unternehmen löschen (Hard delete)")
    df = list_companies(c)
    if df.empty:
        st.info("Keine Einträge vorhanden.")
    else:
        options = {f"{row.id} — {row.name}": int(row.id) for row in df.itertuples(index=False)}
        sel = st.selectbox("Zu löschendes Unternehmen", list(options.keys()))
        company_id = options[sel]

        st.warning("Hard delete: Eintrag wird entfernt. Meta-Daten werden per CASCADE mitgelöscht.")
        confirm = st.checkbox("Ja, wirklich löschen")

        if st.button("Endgültig löschen", use_container_width=True, disabled=not confirm):
            delete_company(c, company_id)
            st.success(f"Gelöscht: ID {company_id}")

else:
    st.subheader("Übersicht")
    df = list_companies(c)
    st.dataframe(df, use_container_width=True, hide_index=True)

    st.divider()
    st.subheader("Meta-Daten ansehen")
    if df.empty:
        st.info("Keine Einträge vorhanden.")
    else:
        options = {f"{row.id} — {row.name}": int(row.id) for row in df.itertuples(index=False)}
        sel = st.selectbox("Unternehmen", list(options.keys()))
        company_id = options[sel]
        st.dataframe(get_meta(c, company_id), use_container_width=True, hide_index=True)

c.close()
