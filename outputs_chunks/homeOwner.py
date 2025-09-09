import time
import pandas as pd
from seleniumbase import SB
from bs4 import BeautifulSoup
import os
import re
from multiprocessing import Process
from datetime import datetime
import msvcrt

# -------------------------------
# LEER INPUTS
# -------------------------------
with open("inputs.txt") as f:
    base_dir = f.readline().strip()  # Carpeta base
    threads = int(f.readline().strip())  # Número de hilos

INPUT_CSV = os.path.join(base_dir, "owners_thurston.csv")
OUTPUT_DIR = os.path.join(base_dir, "outputs_chunks")
LOGS_CSV = os.path.join(base_dir, "logs.csv")

# Crear carpeta de outputs
os.makedirs(OUTPUT_DIR, exist_ok=True)


# -------------------------------
# FUNCION DE PARSEO
# -------------------------------
def test_extract_details(page_source):
    results = []
    soup = BeautifulSoup(page_source, "html.parser")
    persons = soup.select("div.person")

    for p in persons:
        name = p.select_one("#container-name h2")
        name_text = name.get_text(strip=True) if name else None
        age = p.get("data-age")

        # Teléfono
        phone_number = None
        last_known_section = p.select_one(
            "div.section-box:has(h3:contains('Last Known Phone Numbers'))"
        )
        if last_known_section:
            phone_h4 = last_known_section.select_one("h4")
            if phone_h4:
                match = re.search(r"\(\d{3}\)\s?\d{3}-\d{4}", phone_h4.get_text())
                if match:
                    phone_number = match.group(0)

        # Dirección
        address, city, state, zip_code = None, None, None, None
        last_address = p.select_one(
            "div.section-box:has(h3:contains('Last Known Address')) p"
        )
        if last_address:
            address_text = last_address.get_text(" ", strip=True)
            try:
                parts = address_text.split(",")
                if len(parts) == 2:
                    street = parts[0].strip()
                    city_state_zip = parts[1].strip()
                    street_parts = street.split()
                    if len(street_parts) > 2:
                        city = street_parts[-1]
                        address = " ".join(street_parts[:-1])
                    else:
                        address = street
                    state_zip = city_state_zip.split()
                    if len(state_zip) >= 2:
                        state = state_zip[0]
                        zip_code = state_zip[1]
            except Exception:
                pass

        results.append(
            {
                "name": name_text,
                "age": age,
                "primary_phone": phone_number,
                "address": address,
                "city": city,
                "state": state,
                "zip_code": zip_code,
            }
        )
    return results


# -------------------------------
# PROCESAR UNA PERSONA
# -------------------------------
def process_person(row, sb):
    # Limpiar nombres
    first = str(row.get("first_name", "")).lower().replace(" ", "-").replace(",", "")
    last = str(row.get("last_name", "")).lower().replace(" ", "-").replace(",", "")
    state = str(row.get("state", "")).lower().replace(" ", "-").replace(",", "")
    city = str(row.get("city", "")).lower().replace(" ", "-").replace(",", "")

    if not first or not last:
        return []

    url = f"https://www.zabasearch.com/people/{first}-{last}/{state}/{city}/"
    sb.uc_open_with_reconnect(url, 2)
    sb.sleep(1.5)

    # Verificación de página no encontrada
    if sb.is_text_visible("404, NOT FOUND"):
        inverted_url = (
            f"https://www.zabasearch.com/people/{last}-{first}/{state}/{city}/"
        )
        sb.uc_open_with_reconnect(inverted_url, 2)
        sb.sleep(1.5)
        if sb.is_text_visible("404, NOT FOUND"):
            return []

    # Aceptar modal si aparece
    if sb.is_element_visible("#warning-modal"):
        try:
            sb.click('span.inside-copy:contains("I AGREE")')
            sb.sleep(1)
        except Exception:
            pass

    try:
        sb.wait_for_element_visible("body", timeout=5)
        sb.sleep(1)
        page_source = sb.get_page_source()
        return test_extract_details(page_source)
    except Exception as e:
        with open(LOGS_CSV, "a", encoding="utf-8") as f:
            f.write(f"{row.get('owner','UNKNOWN')},{str(e)}\n")
        return []


# -------------------------------
# LOOP DE UN CHUNKk
# -------------------------------
def run_scraper_chunk(df, output_csv, pid):
    with SB(uc=True, headless=False) as sb:
        for _, row in df.iterrows():
            print(
                f"[P{pid}]  {row.get('first_name','')} {row.get('last_name','')} en {row.get('city','')}, {row.get('state','')}"
            )
            data = process_person(row, sb)

            if data:
                df_out = pd.DataFrame(data)
                if not os.path.isfile(output_csv):
                    df_out.to_csv(output_csv, index=False, encoding="utf-8", mode="w")
                else:
                    df_out.to_csv(
                        output_csv,
                        index=False,
                        encoding="utf-8",
                        mode="a",
                        header=False,
                    )
                print(f"[P{pid}]  Guardado {len(df_out)} registro(s) en {output_csv}")
            else:
                print(f"[P{pid}]  No se encontró nada")


# -------------------------------
# MAIN MULTIPROCESS
# -------------------------------
if __name__ == "__main__":
    df = pd.read_csv(INPUT_CSV)
    df = df.dropna(subset=["first_name", "last_name"])  # limpiar NaN

    batch_size = len(df) // threads
    processes = []

    for i in range(threads):
        start = i * batch_size
        end = (i + 1) * batch_size if i < threads - 1 else len(df)
        df_chunk = df.iloc[start:end]
        output_csv = os.path.join(OUTPUT_DIR, f"personas_detalles_part{i+1}.csv")

        p = Process(target=run_scraper_chunk, args=(df_chunk, output_csv, i + 1))
        processes.append(p)

    print(f" Lanzando {threads} procesos en paralelo...")
    start_time = datetime.now().strftime("%H:%M:%S")
    print(" Hora de inicio:", start_time)

    for p in processes:
        p.start()
        time.sleep(2)  # para que no abran todas las ventanas exacto al mismo tiempo

    for p in processes:
        p.join()

    print("\n✅ Proceso terminado. Archivos guardados en", OUTPUT_DIR)
    print("Presiona cualquier tecla para salir...")
    msvcrt.getch()
