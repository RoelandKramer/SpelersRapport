# app.py
"""
Streamlit app: Player PPTX/PDF generator

Expected repo structure:
.
├─ app.py
├─ powerpoint_template.pptx
├─ bench.csv
├─ speler_foto's/
│   ├─ John Doe.png
│   └─ ...
└─ .streamlit/
   └─ secrets.toml

secrets.toml example:

[auth]
token_url = "https://YOUR_DOMAIN/oauth/token"
client_id = "YOUR_CLIENT_ID"
client_secret = "YOUR_CLIENT_SECRET"
scope = ""  # optional

[api]
base_url = "https://YOUR_DOMAIN"  # e.g. https://api.yourprovider.com

Notes:
- Access token flow implemented as OAuth2 client_credentials.
- If you need a different auth flow, tell me your token endpoint requirements and I'll adjust.
"""

from __future__ import annotations

import io
import math
import os
import re
import shutil
import subprocess
import tempfile
import unicodedata
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
import streamlit as st
from pptx import Presentation


# ----------------------------
# Radar configuration
# ----------------------------
RADAR_METRICS_MAP: Dict[str, str] = {
    "Total distance (m)": "total_distance",
    "HI distance (m)": "high_intensity_distance",
    "Sprint distance (m)": "sprint_distance",
    "HI runs": "hi_runs",
    "Sprint runs": "sprint_runs",
}

CUSTOM_MAXES: Dict[str, float] = {
    "Total distance (m)": 15000,
    "HI distance (m)": 1500,
    "Sprint distance (m)": 500,
    "HI runs": 75,
    "Sprint runs": 30,
}

NAME_R_MULT = {
    "Total distance (m)": 1.22,
    "Sprint distance (m)": 1.22,
    "HI runs": 1.22,
}
DEFAULT_NAME_R = 1.33


FC_DEN_BOSCH_PLAYERS = [
    {"name": "Kevin Monzialo", "player_id": 40665},
    {"name": "Kevin Felida", "player_id": 35836},
]


# ----------------------------
# App configuration
# ----------------------------
TEMPLATE_PPTX_PATH = "powerpoint_template.pptx"
BENCH_CSV_PATH = "df_bench.csv"
PLAYER_PHOTOS_DIR = "Speler_foto's"

TOKEN_HINT = "Generate access token first (required)."


@dataclass(frozen=True)
class AuthConfig:
    token_url: str
    client_id: str
    client_secret: str
    scope: str = ""


@dataclass(frozen=True)
class ApiConfig:
    base_url: str

st.write("Secrets keys:", list(st.secrets.keys()))
st.write("Has auth?", "auth" in st.secrets)

# ----------------------------
# Utilities
# ----------------------------
def _norm_name(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()


def _split_label_unit(label: str) -> Tuple[str, str]:
    if label.endswith("(m)"):
        return label.replace(" (m)", ""), "m"
    return label, ""


def load_bench_csv(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Missing {path}. Put bench.csv in the repo root.")
    df = pd.read_csv(path)
    if "player" not in df.columns:
        raise ValueError("bench.csv must include a 'player' column.")
    return df


def list_players(df_bench: pd.DataFrame) -> List[str]:
    players = sorted({str(p).strip() for p in df_bench["player"].dropna().tolist() if str(p).strip()})
    return players


def pick_player_row_by_name(df_bench: pd.DataFrame, player_name: str, team_name: str | None = None) -> pd.Series:
    target = _norm_name(player_name)
    if not target:
        raise ValueError("Empty player_name passed to matcher.")

    df = df_bench.copy()
    df["_player_norm"] = df["player"].astype(str).map(_norm_name)

    exact = df[df["_player_norm"] == target]
    if len(exact) == 0:
        exact = df[df["_player_norm"].str.contains(target, na=False)]

    if team_name and "team" in df.columns and len(exact) > 1:
        tnorm = _norm_name(team_name)
        exact_team = exact[exact["team"].astype(str).map(_norm_name) == tnorm]
        if len(exact_team) > 0:
            exact = exact_team

    if len(exact) > 0:
        if "total_minutes" in exact.columns:
            exact = exact.sort_values("total_minutes", ascending=False)
        return exact.iloc[0]

    sims = df["_player_norm"].map(lambda n: _similarity(target, n))
    best_idx = sims.idxmax()
    best_score = float(sims.loc[best_idx])

    if best_score < 0.80:
        top = df.assign(_sim=sims).sort_values("_sim", ascending=False).head(5)[["player", "_sim"]]
        raise ValueError(
            f"No reliable name match for '{player_name}'. Best similarity={best_score:.3f}. "
            f"Top candidates:\n{top.to_string(index=False)}"
        )

    best_name = df.loc[best_idx, "_player_norm"]
    near = df[df["_player_norm"] == best_name]
    if team_name and "team" in df.columns and len(near) > 1:
        tnorm = _norm_name(team_name)
        near_team = near[near["team"].astype(str).map(_norm_name) == tnorm]
        if len(near_team) > 0:
            near = near_team

    if "total_minutes" in near.columns:
        near = near.sort_values("total_minutes", ascending=False)
    return near.iloc[0]


def compute_maxes(
    df_bench: pd.DataFrame,
    player_row: pd.Series,
    labels: List[str],
    use_custom: Optional[Dict[str, float]],
) -> Dict[str, float]:
    if use_custom:
        return {lab: float(use_custom[lab]) for lab in labels}

    subset = df_bench
    if "position_group" in df_bench.columns and pd.notna(player_row.get("position_group")):
        subset = df_bench[df_bench["position_group"] == player_row["position_group"]]
        if len(subset) < 30:
            subset = df_bench

    maxes: Dict[str, float] = {}
    for lab in labels:
        col = RADAR_METRICS_MAP[lab]
        s = pd.to_numeric(subset[col], errors="coerce").dropna()
        if len(s) == 0:
            maxes[lab] = 1.0
        else:
            mx = float(s.quantile(0.95))
            maxes[lab] = mx if mx > 0 else 1.0
    return maxes


def generate_radar_chart_for_player(
    df_bench: pd.DataFrame,
    player_name: str,
    out_png_path: str,
    custom_maxes: Optional[Dict[str, float]] = None,
    team_name: Optional[str] = None,
) -> Dict[str, float]:
    row = pick_player_row_by_name(df_bench, player_name=player_name, team_name=team_name)

    labels = list(RADAR_METRICS_MAP.keys())
    raw_vals: List[float] = []
    for lab in labels:
        col = RADAR_METRICS_MAP[lab]
        v = float(pd.to_numeric(row.get(col, np.nan), errors="coerce"))
        if math.isnan(v):
            v = 0.0
        raw_vals.append(v)

    raw_arr = np.array(raw_vals, dtype=float)

    maxes_dict = compute_maxes(df_bench, row, labels, custom_maxes)
    max_vals = np.array([maxes_dict[lab] for lab in labels], dtype=float)
    max_vals = np.where(max_vals <= 0, 1.0, max_vals)

    norm = np.clip(raw_arr / max_vals, 0.0, 1.0)
    norm_closed = np.r_[norm, norm[0]]

    n = len(labels)
    angles = np.linspace(0, 2 * np.pi, n, endpoint=False)
    angles_closed = np.r_[angles, angles[0]]

    fig = plt.figure(figsize=(7.8, 7.8), dpi=150)
    ax = plt.subplot(111, polar=True)

    ax.set_theta_offset(np.pi / 2)
    ax.set_theta_direction(-1)

    ax.plot(angles_closed, norm_closed, linewidth=3, marker="o", markersize=6)
    ax.fill(angles_closed, norm_closed, alpha=0.20)

    rings = [0.25, 0.50, 0.75, 1.00]
    ax.set_ylim(0, 1.0)
    ax.set_yticks(rings)
    ax.set_yticklabels([""] * len(rings))
    ax.yaxis.grid(True, linewidth=1)
    ax.xaxis.grid(True, linewidth=1)

    ax.set_xticks(angles)
    ax.set_xticklabels([""] * n)

    tick_fontsize = 8
    for ang, mx, lab in zip(angles, max_vals, labels):
        _, unit = _split_label_unit(lab)
        for r in rings:
            v = r * mx
            txt = f"{v:.0f}" if unit == "" else f"{v:.0f} {unit}"
            ax.text(ang, r, txt, fontsize=tick_fontsize, ha="center", va="center")

    r_value = 1.15
    for lab, ang, val in zip(labels, angles, raw_arr):
        name, unit = _split_label_unit(lab)

        a = (ang + np.pi / 2) % (2 * np.pi)
        c = np.cos(a)
        if c > 0.25:
            ha = "left"
        elif c < -0.25:
            ha = "right"
        else:
            ha = "center"

        r_name = NAME_R_MULT.get(lab, DEFAULT_NAME_R)

        ax.text(ang, r_name, name, fontsize=13, fontweight="bold", ha=ha, va="center")
        val_line = f"{val:.0f}" if unit == "" else f"{val:.0f} {unit}"
        ax.text(ang, r_value, val_line, fontsize=13, fontweight="bold", ha=ha, va="center")

    ax.spines["polar"].set_visible(False)
    fig.patch.set_alpha(0)
    ax.set_facecolor("none")

    plt.tight_layout()
    fig.savefig(out_png_path, bbox_inches="tight", transparent=True)
    plt.close(fig)

    return {lab: float(v) for lab, v in zip(labels, raw_arr)}


def get_local_player_image_path(player_name: str, photos_dir: str) -> Optional[str]:
    if not player_name:
        return None
    target = os.path.join(photos_dir, f"{player_name}.png")
    if os.path.exists(target):
        return target

    if not os.path.isdir(photos_dir):
        return None

    low = f"{player_name.lower()}.png"
    for f in os.listdir(photos_dir):
        if f.lower() == low:
            return os.path.join(photos_dir, f)
    return None


# ----------------------------
# PPTX token replacement
# ----------------------------
def _iter_shapes(slide):
    for shape in slide.shapes:
        yield shape


def replace_tokens_in_text(text: str, values: Dict[str, str]) -> str:
    out = text
    for k, v in values.items():
        if not isinstance(v, str):
            continue
        out = out.replace(f"{{{{{k}}}}}", v)  # {{TOKEN}}
        out = out.replace(f"{{{k}}}", v)       # {TOKEN} (some templates use single brace)
    return out


def replace_tokens_in_shape(shape, values: Dict[str, str]) -> bool:
    if not getattr(shape, "has_text_frame", False):
        return False

    changed = False
    for p in shape.text_frame.paragraphs:
        for run in p.runs:
            new_text = replace_tokens_in_text(run.text, values)
            if new_text != run.text:
                run.text = new_text
                changed = True
    return changed


def _remove_shape(slide, shape) -> None:
    sp = shape._element
    sp.getparent().remove(sp)


def replace_textbox_exact_with_image(slide, token: str, image_bytes: bytes) -> bool:
    """
    Finds a text shape whose full text equals token, then replaces it with an image at same box.
    """
    for shape in list(_iter_shapes(slide)):
        if not getattr(shape, "has_text_frame", False):
            continue
        full_text = (shape.text or "").strip()
        if full_text != token:
            continue

        left, top, width, height = shape.left, shape.top, shape.width, shape.height
        _remove_shape(slide, shape)
        slide.shapes.add_picture(io.BytesIO(image_bytes), left, top, width=width, height=height)
        return True
    return False


def insert_image_at_token(slide, token: str, image_path: str) -> bool:
    """
    Finds token anywhere in a text shape; removes the shape and inserts picture there.
    """
    for shape in list(_iter_shapes(slide)):
        if not getattr(shape, "has_text_frame", False):
            continue
        if token not in (shape.text or ""):
            continue

        left, top, width, height = shape.left, shape.top, shape.width, shape.height
        _remove_shape(slide, shape)
        slide.shapes.add_picture(image_path, left, top, width=width, height=height)
        return True
    return False


def fill_template(
    template_path: str,
    player_name: str,
    df_bench: pd.DataFrame,
    out_pptx_path: str,
    player_photo_dir: str,
    performance_image_bytes: Optional[bytes],
    token: Optional[str],
    api_base: Optional[str],
    player_id: Optional[int],
) -> Dict[str, Any]:
    """
    Generates PPTX:
    - Text token replacement
    - {IMAGE} player photo insertion (local)
    - {{RADAR_CHART}} radar insertion (from bench)
    - {{PERFORMANCE_PLOT}} and {PRESTATIES_FIGURE} insertion (optional upload)
    """
    if not os.path.exists(template_path):
        raise FileNotFoundError(f"Missing {template_path}. Put powerpoint_template.pptx in repo root.")

    prs = Presentation(template_path)

    values: Dict[str, str] = {"PLAYER_NAME": player_name}

    # Best-effort: fetch API info if player_id + token provided
    if player_id and token and api_base:
        try:
            info = api_get_json(api_base, token, f"/api/v2/players/{int(player_id)}")
            pinfo = info.get("info") or {}
            values.update(
                {
                    "PLAYER_NAME": pinfo.get("footballName") or pinfo.get("name") or player_name,
                    "NATIONALITY": ((pinfo.get("nationalities") or [{}])[0].get("name")) if pinfo.get("nationalities") else "",
                    "BIRTH_DATE": str(pinfo.get("birthDate") or ""),
                    "PREFERRED_FOOT": str(pinfo.get("preferredFoot") or ""),
                }
            )
        except Exception:
            pass

    # Player image
    player_img_bytes: Optional[bytes] = None
    local_img_path = get_local_player_image_path(values["PLAYER_NAME"], player_photo_dir)
    if local_img_path:
        with open(local_img_path, "rb") as f:
            player_img_bytes = f.read()

    # Radar chart to temp file
    radar_png = os.path.join(os.path.dirname(out_pptx_path), "radar_chart.png")
    radar_used = generate_radar_chart_for_player(
        df_bench=df_bench,
        player_name=values["PLAYER_NAME"],
        out_png_path=radar_png,
        custom_maxes=CUSTOM_MAXES,
    )

    # Performance chart to temp file if uploaded
    perf_png: Optional[str] = None
    if performance_image_bytes:
        perf_png = os.path.join(os.path.dirname(out_pptx_path), "performance_chart.png")
        with open(perf_png, "wb") as f:
            f.write(performance_image_bytes)

    # Fill slides
    inserted = {"player_image": 0, "radar": 0, "performance": 0, "text": 0}

    for slide in prs.slides:
        if player_img_bytes:
            if replace_textbox_exact_with_image(slide, "{IMAGE}", player_img_bytes):
                inserted["player_image"] += 1

        if insert_image_at_token(slide, "{{RADAR_CHART}}", radar_png):
            inserted["radar"] += 1

        if perf_png:
            if insert_image_at_token(slide, "{{PERFORMANCE_PLOT}}", perf_png):
                inserted["performance"] += 1
            if replace_textbox_exact_with_image(slide, "{PRESTATIES_FIGURE}", performance_image_bytes):
                inserted["performance"] += 1

        for shape in slide.shapes:
            if replace_tokens_in_shape(shape, values):
                inserted["text"] += 1

    prs.save(out_pptx_path)

    return {"values": values, "radar_used": radar_used, "inserted": inserted}


# ----------------------------
# Auth + API
# ----------------------------
def load_auth_config() -> Tuple[AuthConfig, ApiConfig]:
    try:
        auth = st.secrets["auth"]
        api = st.secrets["api"]
    except Exception as e:
        raise RuntimeError(
            "Missing .streamlit/secrets.toml.\n\n"
            "Add:\n"
            "[auth]\n"
            "token_url=...\n"
            "client_id=...\n"
            "client_secret=...\n"
            "scope=\"\"\n\n"
            "[api]\n"
            "base_url=...\n"
        ) from e

    return (
        AuthConfig(
            token_url=str(auth.get("token_url", "")),
            client_id=str(auth.get("client_id", "")),
            client_secret=str(auth.get("client_secret", "")),
            scope=str(auth.get("scope", "")) if auth.get("scope") is not None else "",
        ),
        ApiConfig(base_url=str(api.get("base_url", ""))),
    )


def generate_access_token_from_secrets() -> str:
    auth = st.secrets["auth"]

    data = {
        "grant_type": auth.get("grant_type", "password"),
        "username": auth["username"],
        "password": auth["password"],
        "client_id": auth["client_id"],
        "client_secret": auth["client_secret"],
    }
    scope = auth.get("scope", "")
    if scope:
        data["scope"] = scope

    resp = requests.post(auth["token_url"], data=data, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    token = payload.get("access_token")
    if not token:
        raise RuntimeError(f"No access_token in response: {payload}")
    return str(token)

def api_get_json(api_base: str, token: str, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = api_base.rstrip("/") + path
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(url, headers=headers, params=params or {}, timeout=30)
    r.raise_for_status()
    return r.json()


# ----------------------------
# PDF conversion (best-effort)
# ----------------------------
def can_convert_to_pdf() -> bool:
    return shutil.which("soffice") is not None


def convert_pptx_to_pdf(pptx_path: str, out_dir: str) -> str:
    """
    Uses LibreOffice (soffice) to convert pptx->pdf.
    """
    if not can_convert_to_pdf():
        raise RuntimeError("LibreOffice (soffice) not found on PATH.")

    cmd = [
        "soffice",
        "--headless",
        "--nologo",
        "--nolockcheck",
        "--nodefault",
        "--nofirststartwizard",
        "--convert-to",
        "pdf",
        "--outdir",
        out_dir,
        pptx_path,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if proc.returncode != 0:
        raise RuntimeError(f"PDF conversion failed.\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}")

    pdf_name = os.path.splitext(os.path.basename(pptx_path))[0] + ".pdf"
    pdf_path = os.path.join(out_dir, pdf_name)
    if not os.path.exists(pdf_path):
        raise RuntimeError("Conversion succeeded but PDF not found.")
    return pdf_path


# ----------------------------
# Streamlit UI
# ----------------------------
def main() -> None:
    st.set_page_config(page_title="Player Report Generator", layout="wide")
    st.title("Player Report Generator (PPTX / PDF)")

    # Load bench.csv once
    try:
        df_bench = load_bench_csv(BENCH_CSV_PATH)
    except Exception as e:
        st.error(str(e))
        st.stop()

    players = list_players(df_bench)
    if not players:
        st.error("No players found in bench.csv 'player' column.")
        st.stop()

    # Sidebar: configuration + health checks
    with st.sidebar:
        st.header("Setup")
        st.markdown(
            f"""
**1) {TOKEN_HINT}**

This app reads API credentials from **`.streamlit/secrets.toml`**.
You do **not** enter secrets in the UI.
"""
        )
        st.markdown("**Template:** " + ("✅" if os.path.exists(TEMPLATE_PPTX_PATH) else "❌ missing"))
        st.markdown("**bench.csv:** " + ("✅" if os.path.exists(BENCH_CSV_PATH) else "❌ missing"))
        st.markdown("**speler_foto's:** " + ("✅" if os.path.isdir(PLAYER_PHOTOS_DIR) else "⚠️ not found (photo optional)"))
        st.markdown("**PDF export:** " + ("✅ LibreOffice found" if can_convert_to_pdf() else "⚠️ LibreOffice not found (PPTX only)"))

    # Session state
    st.session_state.setdefault("access_token", None)
    st.session_state.setdefault("pptx_bytes", None)
    st.session_state.setdefault("pdf_bytes", None)
    st.session_state.setdefault("last_filename_base", None)

    # Step 1: token generation
    st.subheader("Step 1 — Generate access token (required)")
    cols = st.columns([1, 2])
    with cols[0]:
        gen = st.button("Generate access token", type="primary")
    with cols[1]:
        if st.session_state["access_token"]:
            st.success("Access token is set for this session.")
        else:
            st.warning(TOKEN_HINT)

    if gen:
        try:
            token = generate_access_token_from_secrets()
            st.session_state["access_token"] = token
            st.success("Access token generated and stored for this session.")
        except Exception as e:
            st.session_state["access_token"] = None
            st.error(f"Token generation failed: {e}")

    st.divider()

    # Step 2: choose player + optional perf chart
    st.subheader("Step 2 — Select player and generate report")

    left, right = st.columns([1, 1])

    with left:
      # Step 2 — Select player and generate report
      
      player_label = st.selectbox(
          "Player (FC Den Bosch only)",
          options=[p["name"] for p in FC_DEN_BOSCH_PLAYERS],
          index=0,
      )
      
      player_id = next(p["player_id"] for p in FC_DEN_BOSCH_PLAYERS if p["name"] == player_label)
      player_name = player_label

    with right:
        perf_file = st.file_uploader(
            "Optional performance chart (PNG/JPG)",
            type=["png", "jpg", "jpeg"],
            accept_multiple_files=False,
        )
        perf_bytes = perf_file.read() if perf_file else None
        if perf_file:
            st.image(perf_bytes, caption="Uploaded performance chart", use_container_width=True)

    generate = st.button("Generate PPTX (and PDF if available)", type="primary")

    if generate:
        if not st.session_state["access_token"]:
            st.error("No valid access token. Generate the access token first.")
            st.stop()

        token = st.session_state["access_token"]
        api_base = st.session_state.get("api_base", "")

        with st.spinner("Generating report..."):
            try:
                with tempfile.TemporaryDirectory() as td:
                    base_name = re.sub(r"[^a-zA-Z0-9_-]+", "_", player_name).strip("_") or "player_report"
                    out_pptx_path = os.path.join(td, f"{base_name}.pptx")

                    meta = fill_template(
                        template_path=TEMPLATE_PPTX_PATH,
                        player_name=player_name,
                        df_bench=df_bench,
                        out_pptx_path=out_pptx_path,
                        player_photo_dir=PLAYER_PHOTOS_DIR,
                        performance_image_bytes=perf_bytes,
                        token=token,
                        api_base=api_base,
                        player_id=player_id,
                    )

                    with open(out_pptx_path, "rb") as f:
                        pptx_bytes = f.read()

                    st.session_state["pptx_bytes"] = pptx_bytes
                    st.session_state["last_filename_base"] = base_name

                    pdf_bytes = None
                    if can_convert_to_pdf():
                        try:
                            pdf_path = convert_pptx_to_pdf(out_pptx_path, td)
                            with open(pdf_path, "rb") as f:
                                pdf_bytes = f.read()
                        except Exception as e:
                            st.warning(f"PDF conversion failed (PPTX still available): {e}")

                    st.session_state["pdf_bytes"] = pdf_bytes

                st.success("Report generated.")
                with st.expander("Generation details"):
                    st.json(meta)

            except Exception as e:
                st.session_state["pptx_bytes"] = None
                st.session_state["pdf_bytes"] = None
                st.error(f"Generation failed: {e}")

    st.divider()

    # Downloads
    st.subheader("Downloads")
    base = st.session_state.get("last_filename_base") or "player_report"

    if st.session_state["pptx_bytes"]:
        st.download_button(
            "Download PPTX",
            data=st.session_state["pptx_bytes"],
            file_name=f"{base}.pptx",
            mime="application/vnd.openxmlformats-officedocument.presentationml.presentation",
        )
    else:
        st.info("Generate a report to enable PPTX download.")

    if st.session_state["pdf_bytes"]:
        st.download_button(
            "Download PDF",
            data=st.session_state["pdf_bytes"],
            file_name=f"{base}.pdf",
            mime="application/pdf",
        )
    else:
        st.caption("PDF download appears after generation if LibreOffice is available.")


if __name__ == "__main__":
    main()
